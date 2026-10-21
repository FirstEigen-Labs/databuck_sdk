"""Export profile-discovered row rules as Databricks expectations."""

import json
import math
import os
import re
from itertools import combinations
from pathlib import Path


_EXPORTABLE_TYPES = {
    "Null Check": "not_null",
    "Default Pattern Check": "valid_pattern",
    "Length Check": "valid_length",
    "Date Rule": "valid_date",
}
_NAME_PART = re.compile(r"[^a-z0-9_]+")
_ACTIONS = ("warn", "drop", "fail")
_YAML_SUFFIXES = {".yaml", ".yml"}
_YAML_PLAIN_SCALAR = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_PROFILE_METADATA_PARAMETERS = {
    "Null Check": ("nullThreshold",),
    "Default Pattern Check": ("topPattern",),
    "Length Check": ("lengthColumns", "allowedLengths"),
    "Date Rule": ("dateFormat",),
    "Value Anomaly Check": ("columns",),
}


class DiscoveredRules(list):
    """SDK rules that can write row-level Databricks expectations."""

    def prepare_for_lake(self, df, group_by=None):
        """Capture value anomaly baselines for self-contained expectations."""
        specs = _value_anomaly_specs(self)
        date_pairs = _date_pairs(self)
        if not specs and not date_pairs:
            return df

        from pyspark.sql import functions as F

        if specs and group_by:
            raise ValueError("Grouped value anomaly baselines cannot be embedded in one expectation")

        for column, _ in specs:
            if column not in df.columns:
                raise ValueError("Value anomaly column {!r} is missing from the DataFrame".format(column))

        baselines = {}
        if specs:
            aggregates = []
            for column, _ in specs:
                mean_name, std_name = _anomaly_stat_columns(column)
                numeric = F.col(_quoted_column(column)).cast("double")
                aggregates.extend((F.avg(numeric).alias(mean_name),
                                   F.stddev_samp(numeric).alias(std_name)))
            stats = df.agg(*aggregates).first()
            for column, _ in specs:
                mean_name, std_name = _anomaly_stat_columns(column)
                mean = stats[mean_name]
                std = stats[std_name]
                if mean is None or not math.isfinite(float(mean)):
                    raise ValueError("Cannot export a value anomaly baseline for {!r}".format(column))
                std = float(std) if std is not None else 0.5
                if not math.isfinite(std) or std == 0:
                    std = 0.5
                baselines[column] = (float(mean), std)

        self._prepared_value_anomaly = specs
        self._value_anomaly_baselines = baselines
        date_bounds = {}
        for first, first_format, second, second_format in date_pairs:
            first_date = _parsed_date_expression(first, first_format)
            second_date = _parsed_date_expression(second, second_format)
            difference = "DATEDIFF(CAST({} AS DATE), CAST({} AS DATE))".format(
                first_date, second_date)
            differences = df.selectExpr("{} AS __databuck_date_difference".format(difference))
            limits = differences.agg(
                F.expr("PERCENTILE_APPROX(__databuck_date_difference, 0.99)").alias("upper"),
                F.expr("PERCENTILE_APPROX(__databuck_date_difference, 0.05)").alias("lower"),
            ).first()
            if limits["upper"] is None or limits["lower"] is None:
                continue
            upper, lower = float(limits["upper"]), float(limits["lower"])
            spread = upper - lower
            maximum = 0.0 if upper < 0 and lower < 0 else upper + 0.25 * spread
            minimum = 0.0 if upper > 0 and lower > 0 else lower - 0.25 * spread
            date_bounds[(first, second)] = (minimum, maximum)
        self._date_pair_bounds = date_bounds
        return df

    def to_lake(self, path_where_needs_to_export, *, df=None, context=None,
                context_rules=None, table_name=None):
        """Export expectations as action-grouped JSON or table-scoped YAML.

        JSON groups rules by action. Pass ``df`` and ``context`` to have Gemini
        select those actions; without context, every JSON rule uses ``warn``.
        YAML writes flat ``expectations``, optionally with ``table`` when a
        source table name is known, and omits action classification.
        """
        export_format = _export_format(path_where_needs_to_export)

        expectations = {}
        value_specs = _value_anomaly_specs(self)
        if value_specs != getattr(self, "_prepared_value_anomaly", ()):
            raise ValueError("Call rules.prepare_for_lake(df) before exporting value anomaly rules")
        if value_specs and set(getattr(self, "_value_anomaly_baselines", {})) != {
            column for column, _ in value_specs
        }:
            raise ValueError("Value anomaly baselines are missing; call rules.prepare_for_lake(df)")
        date_pairs = _date_pairs(self)
        if date_pairs and not hasattr(self, "_date_pair_bounds"):
            raise ValueError("Call rules.prepare_for_lake(df) before exporting date consistency rules")
        for rule in self:
            rule_type = rule.get("ruleType")
            column = rule.get("columnName")
            if rule_type not in _EXPORTABLE_TYPES or not isinstance(column, str):
                continue
            parameters = rule.get("parameters") or {}

            if rule_type == "Null Check":
                threshold = parameters.get("nullThreshold", rule.get("threshold"))
                try:
                    if float(threshold) != 0.0:
                        continue
                except (TypeError, ValueError):
                    continue
                _add(expectations, "not_null", column,
                     "{} IS NOT NULL".format(_quoted_column(column)))

            elif rule_type == "Default Pattern Check":
                definition = parameters.get("topPattern")
                patterns = _default_patterns(definition)
                if not patterns:
                    continue
                identifier = _quoted_column(column)
                generated = ("REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST({} AS STRING)), "
                             "'[a-zA-Z]', 'A'), '[0-9]', '#')").format(identifier)
                allowed = ", ".join(_sql_regex_literal(pattern) for pattern in patterns)
                _add(expectations, "valid_pattern", column,
                     "{} IS NULL OR TRIM(CAST({} AS STRING)) = '' OR {} IN ({})".format(
                         identifier, identifier, generated, allowed))

            elif rule_type == "Length Check":
                length_columns = parameters.get("lengthColumns")
                if not isinstance(length_columns, dict) or not length_columns:
                    length_columns = {column: parameters.get("allowedLengths")}
                for length_column, definition in length_columns.items():
                    lengths = set()
                    for value in str(definition or "").split(","):
                        try:
                            parsed = float(value.strip())
                            if math.isfinite(parsed) and parsed >= 0 and parsed.is_integer():
                                lengths.add(int(parsed))
                        except ValueError:
                            continue
                    if not lengths:
                        continue
                    identifier = _quoted_column(length_column)
                    formatted = ", ".join(str(length) for length in sorted(lengths))
                    _add(expectations, "valid_length", length_column,
                         "{} IS NULL OR LENGTH(CAST({} AS STRING)) IN ({})".format(
                             identifier, identifier, formatted))

            elif rule_type == "Date Rule":
                identifier = _quoted_column(column)
                parsed = _parsed_date_expression(column, parameters.get("dateFormat"))
                _add(expectations, "valid_date", column,
                     "{} IS NULL OR {} IS NOT NULL".format(identifier, parsed))

        for first, first_format, second, second_format in date_pairs:
            bounds = self._date_pair_bounds.get((first, second))
            if bounds is None:
                continue
            first_date = _parsed_date_expression(first, first_format)
            second_date = _parsed_date_expression(second, second_format)
            difference = "DATEDIFF(CAST({} AS DATE), CAST({} AS DATE))".format(
                first_date, second_date)
            _add(expectations, "consistent_date", "{}_{}".format(first, second),
                 "{} IS NULL OR {} IS NULL OR {} BETWEEN {} AND {}".format(
                     first_date, second_date, difference,
                     format(bounds[0], ".17g"), format(bounds[1], ".17g")))

        for column, threshold in value_specs:
            identifier = _quoted_column(column)
            mean, std = self._value_anomaly_baselines[column]
            _add(expectations, "valid_value", column,
                 "{} IS NULL OR ABS(CAST({} AS DOUBLE) - {}) <= {} * {}".format(
                     identifier, identifier, format(mean, ".17g"),
                     format(threshold, ".15g"), format(std, ".17g")))

        auto_expectation_names = set(expectations)
        auto_expectation_count = len(expectations)
        buckgpt_metadata = {}
        for index, rule in enumerate(context_rules or (), start=1):
            name = "BuckGPT_Rule_{:03d}".format(index)
            expectations[name] = _context_rule_expectation(rule, name)
            buckgpt_metadata[name] = {
                "description": rule.get("description"),
                "source_document": rule.get("source_document"),
                "reference_context": rule.get("reference_context"),
                "invalid_row_sql": rule.get("sql"),
            }
        if isinstance(context, dict) and "domain_name" in context and expectations:
            if df is None:
                raise ValueError("A DataFrame is required to select critical rules")
            print("DataBuck: selecting critical rules (from {} auto-discovered rules, "
                  "{} BuckGPT candidates)".format(len(self), len(buckgpt_metadata)),
                  flush=True)
            selected = _select_critical_expectations(
                expectations, df, context, _profile_rule_metadata(self, expectations),
                buckgpt_metadata,
            )
            expectations = {name: expression for name, expression in expectations.items()
                            if name in selected}
            print("DataBuck: selected {} auto-discovered and {} BuckGPT critical rules".format(
                len(auto_expectation_names.intersection(expectations)),
                sum(name in expectations for name in buckgpt_metadata)), flush=True)
        elif context_rules:
            print("DataBuck: exporting {} auto-discovered and {} BuckGPT rules "
                  "({} total)".format(auto_expectation_count, len(context_rules),
                                      len(expectations)), flush=True)

        if not expectations:
            raise ValueError("No Databricks expectations are available to export")
        if export_format == "yaml":
            self.action_decisions = {}
            return write_expectations(
                expectations, path_where_needs_to_export, table_name=table_name,
            )
        if context is None:
            decisions = {name: {"action": "warn", "reason": "No action classification requested."}
                         for name in expectations}
        else:
            if df is None:
                raise ValueError("A DataFrame is required to classify rule actions")
            print("DataBuck: assigning warn/drop/fail actions", flush=True)
            decisions = _classify_actions_with_llm(expectations, df, context)

        grouped = {action: {} for action in _ACTIONS}
        for name, expression in expectations.items():
            grouped[decisions[name]["action"]][name] = expression
        self.action_decisions = decisions
        return write_expectations(
            grouped, path_where_needs_to_export
        )


def _context_rule_expectation(rule, name):
    """Turn an invalid-row query into a row-level Lakeflow pass condition."""
    if not isinstance(rule, dict) or rule.get("audit_status") != "PASSED":
        raise ValueError("{} must be an audited, passed context rule".format(name))
    sql = str(rule.get("sql") or "").strip().rstrip(";").strip()
    match = re.fullmatch(
        r"SELECT\s+\*\s+FROM\s+\{\{DATAFRAME\}\}\s+WHERE\s+(.+)",
        sql, flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError(
            "{} must use SELECT * FROM {{{{DATAFRAME}}}} WHERE <invalid-row condition>"
            .format(name)
        )
    invalid = match.group(1).strip()
    if not invalid or ";" in invalid or "{{DATAFRAME}}" in invalid:
        raise ValueError("{} has an invalid row condition".format(name))
    # A WHERE clause selects only TRUE. FALSE and NULL both mean the row passes.
    return "NOT COALESCE(({}), FALSE)".format(invalid)


def _profile_rule_metadata(rules, expectations):
    metadata = []
    prefixes = {
        "Null Check": "not_null",
        "Default Pattern Check": "valid_pattern",
        "Length Check": "valid_length",
        "Date Rule": "valid_date",
        "Value Anomaly Check": "valid_value",
    }
    for rule in rules:
        rule_type = rule.get("ruleType")
        if rule_type not in _PROFILE_METADATA_PARAMETERS:
            continue
        parameters = rule.get("parameters") or {}
        if not isinstance(parameters, dict):
            parameters = {}
        columns = [rule.get("columnName")]
        if (rule_type == "Length Check"
                and isinstance(parameters.get("lengthColumns"), dict)
                and parameters["lengthColumns"]):
            columns = list(parameters["lengthColumns"])
        elif rule_type == "Value Anomaly Check":
            columns = parameters.get("columns") or columns
        for column in columns:
            if not isinstance(column, str) or not column.strip():
                continue
            name = _expectation_name(prefixes[rule_type], column)
            if name not in expectations:
                continue
            metadata.append({
                "expectation_name": name,
                "rule_type": rule_type,
                "column_name": column.replace("`", "").strip(),
                "threshold": rule.get("threshold"),
                "parameters": {
                    key: parameters[key]
                    for key in _PROFILE_METADATA_PARAMETERS[rule_type] if key in parameters
                },
            })
    return metadata


def _select_critical_expectations(expectations, df, context, profile_metadata,
                                  buckgpt_metadata=None):
    domain_name = context.get("domain_name")
    if not isinstance(domain_name, str) or not domain_name.strip():
        raise ValueError("context['domain_name'] must be a non-empty string")
    api_key = str(context.get("gemini_api_key") or os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("Set context['gemini_api_key'] or GEMINI_API_KEY to select critical rules")

    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise ImportError("Critical rule selection requires google-genai") from exc

    business_context = {key: value for key, value in context.items()
                        if key not in {"gemini_api_key", "gemini_model", "pdf_paths",
                                       "rule_count", "include_failed_rules", "domain_name"}}
    metadata_by_name = {item["expectation_name"]: item for item in profile_metadata
                        if item.get("expectation_name") in expectations}
    buckgpt_metadata = buckgpt_metadata or {}
    candidates = [
        {"id": index, "name": name, "expression": expression,
         "origin": "BuckGPT" if name in buckgpt_metadata else "auto-discovered",
         "metadata": buckgpt_metadata[name] if name in buckgpt_metadata
         else metadata_by_name.get(name)}
        for index, (name, expression) in enumerate(expectations.items(), start=1)
    ]
    prompt = """Act as a {domain_name} subject matter expert and a database expert.
Review the DataBuck auto-discovered and BuckGPT candidates below. Select only
the rules whose failure would materially affect business decisions, regulatory
or policy compliance, financial correctness, customer outcomes, or the ability
to use this dataset for its stated business purpose.

Evaluate each rule in the supplied business context. A generic completeness
check on a technical ID is often low value; do not select it merely because
the column is named ID or the rule exists. An account, customer, transaction,
or other key can still be critical when the context shows that missing values
would break identity, reconciliation, joins, or a required business process.
Likewise, do not assume every pattern, length, date, or outlier rule is
critical. Use the schema, discovery metadata, rule expression, and business
context to judge impact. Do not infer a business requirement from a column
name alone.

Review each distinct SQL check from either origin. Retain it only when you can
identify a concrete business consequence if that exact check fails, supported
by the supplied business context, schema, and candidate metadata. A profiling
pattern or length learned from observed data is not automatically a business
requirement. A BuckGPT rule passing its policy audit is not automatically
business critical; use its quoted reference and source to judge impact. Do not
retain every rule merely because the domain is regulated. Select as many or as
few rules as the evidence supports; there is no target count.
Return only a JSON array of candidate integer IDs to retain. Use the IDs shown
below; do not create names, SQL, or explanations. Return [] if none qualify.

Business context:
{context}

DataFrame schema:
{schema}

Exportable auto-discovered and audited BuckGPT candidates (no sample records):
{candidates}
""".format(
        domain_name=domain_name.strip(),
        context=json.dumps(business_context, default=str, ensure_ascii=False),
        schema=df.schema.json(),
        candidates=json.dumps(candidates, default=str, ensure_ascii=False),
    )
    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        temperature=0.0,
        response_mime_type="application/json",
    )
    retry_reason = None
    for attempt in range(2):
        response = client.models.generate_content(
            model=context.get("gemini_model", "gemini-2.5-flash"),
            contents=prompt if attempt == 0 else (
                prompt + ("\nYour previous response was invalid: {}. "
                          "Return only valid integer IDs from the candidate list "
                          "as a JSON array.").format(retry_reason)
            ),
            config=config,
        )
        try:
            return _parse_critical_profile_selection(response.text or "", expectations)
        except ValueError as exc:
            retry_reason = str(exc)
            if attempt == 1:
                raise ValueError(
                    "Gemini returned an invalid critical rule selection "
                    "after one retry: {}".format(exc)
                ) from exc


def _parse_critical_profile_selection(response_text, expectations):
    try:
        result = json.loads(response_text.strip())
    except json.JSONDecodeError as exc:
        raise ValueError("response is not valid JSON") from exc
    if isinstance(result, dict):
        for key in ("selected_ids", "selected_rule_ids", "candidate_ids",
                    "selected_rule_names", "selected_rules", "critical_rules",
                    "selected_expectations", "rules"):
            if key in result:
                result = result[key]
                break
    if not isinstance(result, list):
        raise ValueError("response must contain a list of expectation names")

    candidate_names = list(expectations)
    names = []
    for item in result:
        if isinstance(item, dict):
            item = next((item[key] for key in ("id", "candidate_id", "name",
                        "expectation_name", "rule_name", "expectation") if key in item), None)
        if isinstance(item, int) and not isinstance(item, bool):
            if item < 1 or item > len(candidate_names):
                raise ValueError("candidate ID is outside the available range")
            names.append(candidate_names[item - 1])
        elif isinstance(item, str) and item.strip():
            names.append(item.strip())
        else:
            raise ValueError("each selected rule must have a candidate ID or name")
    unknown = sorted(set(names) - set(expectations))
    if unknown:
        raise ValueError("unknown expectation names: {}".format(", ".join(unknown[:3])))
    return set(names)


def _classify_actions_with_llm(expectations, df, context):
    if isinstance(context, str):
        context = {"business_context": context}
    if not isinstance(context, dict):
        raise TypeError("context must be a string or dictionary")
    api_key = context.get("gemini_api_key") or os.environ.get("GEMINI_API_KEY")
    api_key = str(api_key or "").strip()
    if not api_key:
        raise ValueError("Set context['gemini_api_key'] or GEMINI_API_KEY to classify rule actions")

    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise ImportError("Action classification requires google-genai") from exc

    sample = [json.loads(row) for row in df.limit(5).toJSON().collect()]
    business_context = {key: value for key, value in context.items()
                        if key not in {"gemini_api_key", "gemini_model", "pdf_paths",
                                       "rule_count", "include_failed_rules"}}
    prompt = """Classify each DataBuck row expectation for a Databricks pipeline.
Return only a JSON array with one object for every expectation, containing
exactly: name, action, reason. Use the exact expectation names provided.

Allowed actions:
- warn: keep invalid rows and record quality metrics.
- drop: discard invalid individual rows while the pipeline can continue.
- fail: stop the pipeline update when a violation makes the entire update
  unacceptable.

Decide from the expectation meaning, column names and types, the DataFrame
sample, and any optional business context. Use your judgment to choose the
appropriate action for each rule. A small sample does not establish business
criticality by itself. Choose warn if the evidence does not justify drop or
fail; reserve fail for violations that should stop the entire update.
Do not change, omit, or invent rules. Explain each choice in reason.

Business context:
{context}

DataFrame schema:
{schema}

DataFrame sample (up to five rows):
{sample}

Discovered expectations:
{expectations}
""".format(
        context=json.dumps(business_context, default=str, ensure_ascii=False),
        schema=df.schema.json(),
        sample=json.dumps(sample, default=str, ensure_ascii=False),
        expectations=json.dumps(expectations, ensure_ascii=False),
    )
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=context.get("gemini_model", "gemini-2.5-flash"),
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )
    try:
        decisions = json.loads((response.text or "").strip())
    except json.JSONDecodeError as exc:
        raise ValueError("Gemini returned invalid JSON for rule actions") from exc
    if not isinstance(decisions, list):
        raise ValueError("Gemini must return a list of rule actions")

    by_name = {}
    for decision in decisions:
        if not isinstance(decision, dict):
            raise ValueError("Gemini returned an invalid rule action")
        name = decision.get("name")
        action = str(decision.get("action", "")).strip().lower()
        reason = str(decision.get("reason", "")).strip()
        if (not isinstance(name, str) or name not in expectations
                or name in by_name or action not in _ACTIONS or not reason):
            raise ValueError("Gemini returned an unknown, duplicate, or invalid rule action")
        by_name[name] = {"action": action, "reason": reason}
    if set(by_name) != set(expectations):
        raise ValueError("Gemini did not classify every exported rule")
    return by_name


def _value_anomaly_specs(rules):
    specs = {}
    for rule in rules:
        if rule.get("ruleType") != "Value Anomaly Check":
            continue
        try:
            threshold = float(rule.get("threshold"))
        except (TypeError, ValueError):
            raise ValueError("Value anomaly rule requires a numeric threshold")
        if not math.isfinite(threshold) or threshold <= 0:
            raise ValueError("Value anomaly threshold must be positive and finite")
        parameters = rule.get("parameters") or {}
        columns = parameters.get("columns") or [rule.get("columnName")]
        for raw_column in columns:
            if not isinstance(raw_column, str):
                continue
            column = raw_column.replace("`", "").strip()
            if not column:
                continue
            if column in specs and specs[column] != threshold:
                raise ValueError("Conflicting value anomaly thresholds for {!r}".format(column))
            specs[column] = threshold
    return tuple(specs.items())


def _date_pairs(rules):
    dates = {}
    for rule in rules:
        if rule.get("ruleType") != "Date Rule":
            continue
        column = rule.get("columnName")
        if isinstance(column, str):
            column = column.replace("`", "").strip()
            if column:
                dates[column] = (rule.get("parameters") or {}).get("dateFormat")
    return tuple((first, dates[first], second, dates[second])
                 for first, second in combinations(sorted(dates), 2))


def _parsed_date_expression(column, date_format):
    identifier = _quoted_column(column)
    if isinstance(date_format, str) and date_format.strip():
        return "TRY_TO_TIMESTAMP(CAST({} AS STRING), '{}')".format(
            identifier, date_format.strip().replace("'", "''"))
    return "TRY_CAST({} AS TIMESTAMP)".format(identifier)


def _anomaly_stat_columns(column):
    suffix = _NAME_PART.sub("_", column.lower()).strip("_")
    return "__databuck_va_mean_{}".format(suffix), "__databuck_va_stddev_{}".format(suffix)


def _quoted_column(column):
    name = column.replace("`", "").strip()
    if not name:
        raise ValueError("Cannot export a rule without a column name")
    return "`{}`".format(name)


def _sql_regex_literal(regex):
    # Ordinary Databricks SQL strings need doubled backslashes for Java regex.
    return "'{}'".format(regex.replace("\\", "\\\\").replace("'", "''"))


def _default_patterns(definition):
    """Read the frequency tagged or plain patterns accepted by the Spark check."""
    if not isinstance(definition, str) or not definition.strip():
        return []
    if "val:" in definition and "per:" in definition:
        candidates = (part.split("per:", 1)[0] for part in definition.split("val:")[1:]
                      if "per:" in part)
    else:
        candidates = definition.split(",")
    patterns = []
    for candidate in candidates:
        pattern = candidate.strip().replace("[", "").replace("]", "").strip()
        if pattern and pattern not in patterns:
            patterns.append(pattern)
    return patterns


def _expectation_name(prefix, column):
    name = _NAME_PART.sub("_", column.replace("`", "").strip().lower()).strip("_")
    if not name:
        raise ValueError("Cannot name expectation for column {!r}".format(column))
    return "{}_{}".format(prefix, name)


def _add(expectations, prefix, column, expression):
    name = _expectation_name(prefix, column)
    if name in expectations and expectations[name] != expression:
        raise ValueError("Conflicting expectation name: {}".format(name))
    expectations[name] = expression


def _export_format(path_where_needs_to_export):
    if (not isinstance(path_where_needs_to_export, (str, os.PathLike))
            or not os.fspath(path_where_needs_to_export)):
        raise ValueError("An output file or directory path is required")
    suffix = Path(path_where_needs_to_export).suffix.lower()
    return "yaml" if suffix in _YAML_SUFFIXES else "json"


def _validated_table_name(table_name):
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("table_name must be a non-empty string when provided")
    return table_name.strip()


def _yaml_scalar(value):
    return value if _YAML_PLAIN_SCALAR.fullmatch(value) else json.dumps(value, ensure_ascii=False)


def write_expectations(expectations, path_where_needs_to_export, *, table_name=None):
    export_format = _export_format(path_where_needs_to_export)
    output = Path(path_where_needs_to_export)
    if export_format == "yaml":
        if table_name is not None:
            table_name = _validated_table_name(table_name)
        if not expectations:
            raise ValueError("No Databricks expectations are available to export")
    else:
        if not any(expectations.get(action) for action in _ACTIONS):
            raise ValueError("No Databricks expectations are available to export")
    if export_format == "json" and output.suffix.lower() != ".json":
        output /= "databuck_expectations.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        if export_format == "yaml":
            if table_name is not None:
                stream.write("table: {}\n\n".format(_yaml_scalar(table_name)))
            stream.write("expectations:\n")
            for name, expression in expectations.items():
                stream.write("  {}: {}\n".format(
                    _yaml_scalar(name), json.dumps(expression, ensure_ascii=False)
                ))
        else:
            json.dump(expectations, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
    return str(output)
