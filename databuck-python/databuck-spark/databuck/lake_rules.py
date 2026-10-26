"""Export profile-discovered row rules as Databricks expectations."""

import hashlib
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
_CRITICAL_SELECTION_CACHE = {}
_DUPLICATE_SELECTION_CACHE = {}


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
        dataset_checks = {}
        for index, rule in enumerate(context_rules or (), start=1):
            name = "BuckGPT_Rule_{:03d}".format(index)
            if rule.get("check_type") == "uniqueness":
                if rule.get("audit_status") != "PASSED":
                    raise ValueError("{} must be an audited, passed context rule".format(name))
                from .agentic_rules import INTEL_SOURCE, _uniqueness_sql

                columns = rule.get("columns")
                if (rule.get("source_document") != INTEL_SOURCE
                        or not isinstance(columns, list) or not columns
                        or any(not isinstance(column, str) or not column.strip()
                               for column in columns)
                        or len(columns) != len(set(columns))
                        or rule.get("sql") != _uniqueness_sql(columns)):
                    raise ValueError("{} has an invalid uniqueness check".format(name))
                dataset_checks[name] = {
                    "name": name,
                    "description": rule.get("description"),
                    "source_document": rule.get("source_document"),
                    "reference_context": rule.get("reference_context"),
                    "columns": rule.get("columns"),
                    "sql": rule.get("sql"),
                }
                expectations[name] = rule["sql"]
            else:
                expectations[name] = _context_rule_expectation(rule, name)
        if isinstance(context, dict) and "domain_name" in context and expectations:
            if df is None:
                raise ValueError("A DataFrame is required to select critical rules")
            metadata = _profile_rule_metadata(self, expectations)
            buckgpt_metadata = {}
            for index, rule in enumerate(context_rules or (), start=1):
                name = "BuckGPT_Rule_{:03d}".format(index)
                buckgpt_metadata[name] = {
                    "source_document": rule.get("source_document"),
                    "reference_context": rule.get("reference_context"),
                    "description": rule.get("description"),
                    "check_type": rule.get("check_type") or "row",
                    "columns": rule.get("columns"),
                }
                metadata.append({"expectation_name": name, **buckgpt_metadata[name]})
            if len(expectations) > 1:
                print("DataBuck: reviewing duplicate rules ({} auto-discovered, "
                      "{} BuckGPT)".format(auto_expectation_count,
                                             len(context_rules or ())), flush=True)
                removed = _identify_duplicate_expectations(
                    expectations, df, context, metadata, auto_expectation_names,
                )
                expectations = {name: expression for name, expression in expectations.items()
                                if name not in removed}
                print("DataBuck: removed {} duplicate rules; {} remain".format(
                    len(removed), len(expectations)), flush=True)
            print("DataBuck: selecting critical rules ({} auto-discovered, "
                  "{} BuckGPT candidates)".format(
                      len(auto_expectation_names.intersection(expectations)),
                      len(set(expectations) - auto_expectation_names)), flush=True)
            selected = _select_critical_expectations(
                expectations, df, context, metadata,
            )
            expectations = {name: expression for name, expression in expectations.items()
                            if name in selected}
            print("DataBuck: selected {} auto-discovered and {} BuckGPT critical rules".format(
                      len(auto_expectation_names.intersection(expectations)),
                      len(set(expectations) - auto_expectation_names)), flush=True)
        elif context_rules:
            print("DataBuck: exporting {} auto-discovered and {} BuckGPT rules "
                  "({} total)".format(auto_expectation_count, len(context_rules),
                                      len(expectations)), flush=True)

        dataset_checks = {name: check for name, check in dataset_checks.items()
                          if name in expectations}
        expectations = {name: expression for name, expression in expectations.items()
                        if name not in dataset_checks}

        if not expectations and not dataset_checks:
            raise ValueError("No Databricks expectations are available to export")
        if export_format == "yaml":
            self.action_decisions = {}
            return write_expectations(
                expectations, path_where_needs_to_export, table_name=table_name,
                dataset_checks=dataset_checks,
            )
        if context is None:
            decisions = {name: {"action": "warn", "reason": "No action classification requested."}
                         for name in expectations}
        else:
            if df is None:
                raise ValueError("A DataFrame is required to classify rule actions")
            if expectations:
                print("DataBuck: assigning warn/drop/fail actions", flush=True)
                decisions = _classify_actions_with_llm(expectations, df, context)
            else:
                decisions = {}

        grouped = {action: {} for action in _ACTIONS}
        for name, expression in expectations.items():
            grouped[decisions[name]["action"]][name] = expression
        self.action_decisions = decisions
        return write_expectations(
            grouped, path_where_needs_to_export, dataset_checks=dataset_checks,
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


def _parse_duplicate_selection(response_text, candidate_names, dataset_names=()):
    try:
        result = json.loads(response_text.strip())
    except json.JSONDecodeError as exc:
        raise ValueError("response is not valid JSON") from exc
    if not isinstance(result, list):
        raise ValueError("response must be a list of duplicate pairs")
    removed = set()
    kept = set()
    for item in result:
        if not isinstance(item, dict):
            raise ValueError("each duplicate needs remove_id, keep_id, and reason")
        remove_id, keep_id, reason = (item.get("remove_id"), item.get("keep_id"),
                                      item.get("reason"))
        if (type(remove_id) is not int or type(keep_id) is not int
                or not 1 <= remove_id <= len(candidate_names)
                or not 1 <= keep_id <= len(candidate_names)
                or remove_id == keep_id):
            raise ValueError("duplicate pair contains invalid candidate IDs")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("duplicate pair needs a reason")
        if ((candidate_names[remove_id - 1] in dataset_names)
                != (candidate_names[keep_id - 1] in dataset_names)):
            raise ValueError("dataset and row checks cannot be duplicates")
        if remove_id in removed:
            raise ValueError("duplicate candidate was removed more than once")
        removed.add(remove_id)
        kept.add(keep_id)
    if removed & kept:
        raise ValueError("a duplicate representative cannot also be removed")
    return {candidate_names[rule_id - 1] for rule_id in removed}


def _identify_duplicate_expectations(expectations, df, context, metadata,
                                     auto_expectation_names):
    api_key = str(context.get("gemini_api_key") or os.environ.get("GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("Set context['gemini_api_key'] or GEMINI_API_KEY to review duplicate rules")
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise ImportError("Duplicate rule review requires google-genai") from exc

    metadata_by_name = {item["expectation_name"]: item for item in metadata
                        if item.get("expectation_name") in expectations}
    dataset_names = {name for name, item in metadata_by_name.items()
                     if item.get("check_type") == "uniqueness"}
    candidate_names = sorted(expectations)
    candidates = [{
        "id": index,
        "name": name,
        "source": "AutoDiscovery" if name in auto_expectation_names else "BuckGPT",
        "check": expectations[name],
        "metadata": metadata_by_name.get(name),
    } for index, name in enumerate(candidate_names, start=1)]
    prompt = """Review these exportable DataBuck auto-discovered and audited BuckGPT
rules for duplicate controls. A duplicate pair checks the same failure on the
same columns with equivalent conditions, even if names or SQL syntax differ.
The check field for row rules is a passing condition; dataset uniqueness rules
are duplicate-group queries. Compare their meaning, scope, null behavior,
thresholds, and conditional applicability. Pattern and length checks on the
same column are not automatically duplicates. Keep both if either catches a
distinct failure. Never merge a dataset uniqueness check with a row check.
For each true duplicate, keep the clearer, better supported, or more precise
control; use source metadata and document evidence when available. Do not
remove a rule merely because it is low priority or shares a column with
another rule. Criticality will be evaluated separately.

Return only a JSON array of objects with integer remove_id, integer keep_id,
and a short reason explaining the equivalent check. Example:
[{{"remove_id": 2, "keep_id": 1, "reason": "Same null check on the same column"}}]
Use only the candidate IDs below. Every keep_id must remain in the set; do
not remove a representative or the same candidate twice. Return [] if there
are no true duplicates. Do not create new rules.

Business domain: {domain}
DataFrame schema: {schema}
Candidates (no sample records): {candidates}
""".format(
        domain=context["domain_name"],
        schema=df.schema.json(),
        candidates=json.dumps(candidates, default=str, ensure_ascii=False,
                              sort_keys=True),
    )
    model = context.get("gemini_model", "gemini-2.5-flash")
    cache_key = hashlib.sha256(
        "{}\0{}\0{}".format(api_key, model, prompt).encode("utf-8")
    ).hexdigest()
    if cache_key in _DUPLICATE_SELECTION_CACHE:
        print("DataBuck: reusing duplicate rule review for unchanged inputs", flush=True)
        return set(_DUPLICATE_SELECTION_CACHE[cache_key])

    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        temperature=0.0, response_mime_type="application/json",
    )
    retry_reason = None
    for attempt in range(2):
        response = client.models.generate_content(
            model=model,
            contents=prompt if attempt == 0 else (
                prompt + "\nYour previous response was invalid: {}. Return only "
                "valid candidate IDs and a reason for each duplicate pair."
            ).format(retry_reason),
            config=config,
        )
        try:
            removed = _parse_duplicate_selection(response.text or "", candidate_names,
                                                 dataset_names)
            if len(_DUPLICATE_SELECTION_CACHE) >= 32:
                del _DUPLICATE_SELECTION_CACHE[next(iter(_DUPLICATE_SELECTION_CACHE))]
            _DUPLICATE_SELECTION_CACHE[cache_key] = frozenset(removed)
            return removed
        except ValueError as exc:
            retry_reason = str(exc)
    print("DataBuck: duplicate review returned an invalid selection after one retry; "
          "keeping all rules ({})".format(retry_reason), flush=True)
    return set()


def _select_critical_expectations(expectations, df, context, profile_metadata):
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
    candidates = [
        {"id": index, "name": name, "expression": expression,
         "metadata": metadata_by_name.get(name)}
        for index, (name, expression) in enumerate(sorted(expectations.items()), start=1)
    ]
    prompt = """Act as an independent review committee: a {domain_name} subject matter expert,
a {domain_name} data engineer, and a data quality governance lead.
Judge business value independently of the rule's origin.
Review the deduplicated DataBuck auto-discovered and audited BuckGPT candidates
below. Select only
the rules whose failure would materially affect business decisions, regulatory
or policy compliance, financial correctness, customer outcomes, or the ability
to use this dataset for its stated business purpose.

Evaluate each rule in the supplied business context. A generic completeness
check on a technical ID is often low value; do not select it merely because
the column is named ID or the rule exists. An account, customer, transaction,
or other key can still be critical when the context shows that missing values
would break identity, reconciliation, joins, or a required business process.
Likewise, do not assume every pattern, length, date, or outlier rule is
critical. Select a pattern or length check only when the expected format or
length is relevant to the supplied business context and its failure has a
material consequence. When pattern and length checks cover the same column,
compare them together: retain both only if each catches a distinct, important
failure; otherwise retain the stronger check. Use the schema, discovery
metadata, rule expression, and business context to judge impact. Do not infer
a business requirement from a column name alone.

Before selecting each rule, assess these points internally:
- Business materiality: could a violation materially affect balances,
  reporting, reconciliation, risk decisions, customers, compliance, or a
  significant operational process? Financial-sounding column names alone
  do not establish materiality.
- Business correctness: is the rule mathematically and semantically valid
  for this dataset's lifecycle? Check legitimate zero or missing values,
  product variations, reporting periods, precision, and rounding. Do not
  promote an observed pattern or unsupported threshold into a requirement.
- Production necessity: would this exact check produce an actionable alert,
  protect a critical data element or process, and add value beyond an
  upstream schema constraint or existing control?
- Incremental detection: does the rule detect a distinct failure mode, or
  is it redundant with a stronger candidate? Keep separate controls only
  when removing one would materially reduce coverage.
- False-positive risk: consider conditional applicability and legitimate
  exceptions. If an important rule needs a missing condition or unverified
  business definition, treat it as requiring confirmation rather than as a
  production-ready control.

Retain a Critical candidate only when it has high business impact, valid
{domain_name} semantics, clear applicability, acceptable false-positive risk, and
distinct detection value. Retain a High Value candidate when it is supported,
actionable, distinct, and meaningfully useful despite lower severity or
narrower applicability. Omit low-value, speculative, redundant, and
confirmation-dependent candidates from the retained set. A simple check can
be Critical; a complex check can be Low Value. Detection complexity is not
business priority. Do not assume a {domain_name} subdomain that the context does
not establish. Evidence from schema or sample patterns alone is inference,
not a verified business requirement.

Review each distinct SQL check. Retain it only when you can
identify a concrete business consequence if that exact check fails, supported
by the supplied business context, schema, and candidate metadata. A profiling
pattern or length learned from observed data is not automatically a business
requirement. Do not retain every rule merely because the domain is regulated.

Prioritize the most consequential rules for this dataset's business purpose.
Return the smallest defensible set that a business reviewer can examine and
act on. Compare candidates against one another: when several rules protect the
same outcome, keep the strongest, clearest check and omit weaker or redundant
ones. Keep distinct legal or business obligations when each has a material
consequence. Internally identify the consequence for every selected rule;
exclude any rule whose impact you cannot explain from the supplied context.
Do not aim for a fixed count or percentage, and do not select all candidates
by default.
For each selected rule, assign a relative business priority using your
{domain_name} expertise: HIGH for immediate regulatory, financial, customer,
or operational impact; MEDIUM for a material but less urgent impact; LOW for
a still worthwhile business review with lower impact. LOW does not mean a
routine or unsupported rule should be selected. LOW is a relative priority
among retained rules, not the Low Value classification above. Use the same business context
and rule evidence for both selection and priority.
Return only a JSON array of objects with integer id and string priority
(HIGH, MEDIUM, or LOW), for example [{{"id": 2, "priority": "HIGH"}}].
Use only the candidate IDs shown below. Do not create names, SQL, or
explanations. Return [] if none qualify.

Business context:
{context}

DataFrame schema:
{schema}

Auto-discovered and BuckGPT candidates (no sample records):
{candidates}
""".format(
        domain_name=domain_name.strip(),
        context=json.dumps(business_context, default=str, ensure_ascii=False,
                           sort_keys=True),
        schema=df.schema.json(),
        candidates=json.dumps(candidates, default=str, ensure_ascii=False,
                              sort_keys=True),
    )
    model = context.get("gemini_model", "gemini-2.5-flash")
    cache_key = hashlib.sha256(
        "{}\0{}\0{}".format(api_key, model, prompt).encode("utf-8")
    ).hexdigest()
    cached_priorities = _CRITICAL_SELECTION_CACHE.get(cache_key)
    if cached_priorities is not None:
        print("DataBuck: reusing critical rule selection for unchanged inputs",
              flush=True)
        for name, priority in cached_priorities.items():
            print("DataBuck: critical rule [{}] {}".format(priority, name),
                  flush=True)
        return set(cached_priorities)

    client = genai.Client(api_key=api_key)
    config = types.GenerateContentConfig(
        temperature=0.0,
        response_mime_type="application/json",
    )
    retry_reason = None
    for attempt in range(2):
        response = client.models.generate_content(
            model=model,
            contents=prompt if attempt == 0 else (
                prompt + ("\nYour previous response was invalid: {}. "
                          "Return a JSON array of objects with valid integer id "
                          "and priority HIGH, MEDIUM, or LOW.").format(retry_reason)
            ),
            config=config,
        )
        try:
            priorities = _parse_critical_profile_selection(response.text or "", expectations)
            if len(_CRITICAL_SELECTION_CACHE) >= 32:
                del _CRITICAL_SELECTION_CACHE[next(iter(_CRITICAL_SELECTION_CACHE))]
            _CRITICAL_SELECTION_CACHE[cache_key] = priorities
            for name, priority in priorities.items():
                print("DataBuck: critical rule [{}] {}".format(priority, name),
                      flush=True)
            return set(priorities)
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
    if not isinstance(result, list):
        raise ValueError("response must be a list of selected rule objects")

    candidate_names = sorted(expectations)
    priorities = {}
    for item in result:
        if not isinstance(item, dict):
            raise ValueError("each selected rule needs an id and priority")
        rule_id = item.get("id")
        priority = item.get("priority")
        if not isinstance(rule_id, int) or isinstance(rule_id, bool) or not 1 <= rule_id <= len(candidate_names):
            raise ValueError("candidate ID is outside the available range")
        if not isinstance(priority, str) or priority.upper() not in {"HIGH", "MEDIUM", "LOW"}:
            raise ValueError("priority must be HIGH, MEDIUM, or LOW")
        name = candidate_names[rule_id - 1]
        if name in priorities:
            raise ValueError("duplicate candidate ID: {}".format(rule_id))
        priorities[name] = priority.upper()
    return priorities


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


def write_expectations(expectations, path_where_needs_to_export, *, table_name=None,
                       dataset_checks=None):
    export_format = _export_format(path_where_needs_to_export)
    output = Path(path_where_needs_to_export)
    dataset_checks = dataset_checks or {}
    if export_format == "yaml":
        if table_name is not None:
            table_name = _validated_table_name(table_name)
        if not expectations and not dataset_checks:
            raise ValueError("No Databricks expectations are available to export")
    else:
        if not any(expectations.get(action) for action in _ACTIONS) and not dataset_checks:
            raise ValueError("No Databricks expectations are available to export")
    if export_format == "json" and output.suffix.lower() != ".json":
        output /= "databuck_expectations.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        if export_format == "yaml":
            if table_name is not None:
                stream.write("table: {}\n\n".format(_yaml_scalar(table_name)))
            stream.write("expectations:\n" if expectations else "expectations: {}\n")
            for name, expression in expectations.items():
                stream.write("  {}: {}\n".format(
                    _yaml_scalar(name), json.dumps(expression, ensure_ascii=False)
                ))
            if dataset_checks:
                stream.write("dataset_checks: {}\n".format(
                    json.dumps(list(dataset_checks.values()), ensure_ascii=False)))
        else:
            output_data = dict(expectations)
            if dataset_checks:
                output_data["dataset_checks"] = list(dataset_checks.values())
            json.dump(output_data, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
    return str(output)
