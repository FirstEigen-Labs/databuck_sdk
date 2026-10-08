"""Export profile-discovered row rules as Databricks expectation JSON."""

import json
import math
import os
import re
from pathlib import Path


_EXPORTABLE_TYPES = {
    "Null Check": "not_null",
    "Default Pattern Check": "valid_pattern",
    "Length Check": "valid_length",
}
_NAME_PART = re.compile(r"[^a-z0-9_]+")
_PROFILE_PATTERN = re.compile(r"val:(.*?)per:\s*\(?[0-9]+(?:\.[0-9]+)?%\)?")
_ACTIONS = ("warn", "drop", "fail")


class DiscoveredRules(list):
    """SDK rules that can write row-level Databricks expectations."""

    def prepare_for_lake(self, df, group_by=None):
        """Add current-run mean and standard deviation columns for value anomalies.

        Apply the exported expectations to this returned DataFrame, not the
        original one. Pass ``group_by`` when statistics are calculated per group.
        """
        from pyspark.sql import functions as F
        from pyspark.sql import Window

        specs = _value_anomaly_specs(self)
        if not specs:
            return df

        groups = [group_by] if isinstance(group_by, str) else list(group_by or ())
        for column in groups:
            if column.replace("`", "").strip() not in df.columns:
                raise ValueError("Group column {!r} is missing from the DataFrame".format(column))

        statistic_names = []
        for column, _ in specs:
            if column not in df.columns:
                raise ValueError("Value anomaly column {!r} is missing from the DataFrame".format(column))
            statistic_names.extend(_anomaly_stat_columns(column))
        if len(statistic_names) != len(set(statistic_names)) or any(
            name in df.columns for name in statistic_names
        ):
            raise ValueError("Value anomaly statistic columns would conflict with DataFrame columns")

        if groups:
            window = Window.partitionBy(*[F.col(_quoted_column(column)) for column in groups])
            prepared = df
            for column, _ in specs:
                mean_name, std_name = _anomaly_stat_columns(column)
                numeric = F.col(_quoted_column(column)).cast("double")
                prepared = prepared.withColumn(mean_name, F.avg(numeric).over(window))
                prepared = prepared.withColumn(std_name, F.stddev_samp(numeric).over(window))
        else:
            aggregates = []
            for column, _ in specs:
                mean_name, std_name = _anomaly_stat_columns(column)
                numeric = F.col(_quoted_column(column)).cast("double")
                aggregates.extend((F.avg(numeric).alias(mean_name),
                                   F.stddev_samp(numeric).alias(std_name)))
            prepared = df.crossJoin(F.broadcast(df.agg(*aggregates)))

        for column, _ in specs:
            _, std_name = _anomaly_stat_columns(column)
            std = F.col(_quoted_column(std_name))
            prepared = prepared.withColumn(
                std_name,
                F.when(std.isNull() | F.isnan(std) | (std == 0), F.lit(0.5)).otherwise(std),
            )

        self._prepared_value_anomaly = specs
        return prepared

    def to_lake(self, path_where_needs_to_export, *, df=None, context=None,
                context_rules=None):
        """Export expectations grouped by action.

        Pass both ``df`` and business ``context`` to have Gemini select actions
        using the discovered rules and a small sample of the DataFrame. Without
        context, every rule defaults to the non-destructive ``warn`` action.
        """
        expectations = {}
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
                definition = parameters.get("topPattern") or rule.get("expression") or ""
                patterns = _PROFILE_PATTERN.findall(str(definition))
                if not patterns:
                    continue
                alternatives = []
                for pattern in patterns:
                    regex = "".join("[A-Z]" if char == "A" else
                                    "[0-9]" if char == "#" else re.escape(char)
                                    for char in pattern)
                    alternatives.append("(?:^{}$)".format(regex))
                identifier = _quoted_column(column)
                _add(expectations, "valid_pattern", column,
                     "{} IS NULL OR CAST({} AS STRING) RLIKE {}".format(
                         identifier, identifier, _sql_regex_literal("|".join(alternatives))))

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

        for column, threshold in getattr(self, "_prepared_value_anomaly", ()):
            identifier = _quoted_column(column)
            mean_name, std_name = _anomaly_stat_columns(column)
            mean = _quoted_column(mean_name)
            std = _quoted_column(std_name)
            _add(expectations, "valid_value", column,
                 "{} IS NULL OR ({} IS NOT NULL AND ABS(CAST({} AS DOUBLE) - {}) <= {} * {})".format(
                     identifier, mean, identifier, mean, format(threshold, ".15g"), std))

        for index, rule in enumerate(context_rules or (), start=1):
            name = "BuckGPT_Rule_{:03d}".format(index)
            expectations[name] = _context_rule_expectation(rule, name)

        if not expectations:
            raise ValueError("No Databricks expectations are available to export")
        if context is None:
            decisions = {name: {"action": "warn", "reason": "No business context supplied."}
                         for name in expectations}
        else:
            if df is None:
                raise ValueError("A DataFrame is required to classify actions from business context")
            decisions = _classify_actions_with_llm(expectations, df, context)

        grouped = {action: {} for action in _ACTIONS}
        for name, expression in expectations.items():
            grouped[decisions[name]["action"]][name] = expression
        self.action_decisions = decisions
        return write_expectations(grouped, path_where_needs_to_export)


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
                        if key not in {"gemini_api_key", "gemini_model"}}
    prompt = """Classify each DataBuck row expectation for a Databricks pipeline.
Return only a JSON array with one object for every expectation, containing
exactly: name, action, reason. Use the exact expectation names provided.

Allowed actions:
- warn: keep invalid rows and record quality metrics.
- drop: discard invalid rows because the business context shows that those
  individual rows cannot safely continue, while the pipeline can continue.
- fail: stop the pipeline update because the business context shows that any
  violation makes the entire update unacceptable.

Use the business context to decide the action. The sample describes the data;
it does not by itself authorize dropping rows or failing an update. If the
context does not establish a clear enforcement consequence, choose warn.
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


def _add(expectations, prefix, column, expression):
    name = _NAME_PART.sub("_", column.replace("`", "").strip().lower()).strip("_")
    if not name:
        raise ValueError("Cannot name expectation for column {!r}".format(column))
    name = "{}_{}".format(prefix, name)
    if name in expectations and expectations[name] != expression:
        raise ValueError("Conflicting expectation name: {}".format(name))
    expectations[name] = expression


def write_expectations(expectations, path_where_needs_to_export):
    if not isinstance(path_where_needs_to_export, (str, os.PathLike)) or not os.fspath(path_where_needs_to_export):
        raise ValueError("An output file or directory path is required")
    if not any(expectations.get(action) for action in _ACTIONS):
        raise ValueError("No Databricks expectations are available to export")
    output = Path(path_where_needs_to_export)
    if output.suffix.lower() != ".json":
        output /= "databuck_expectations.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        json.dump(expectations, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    return str(output)
