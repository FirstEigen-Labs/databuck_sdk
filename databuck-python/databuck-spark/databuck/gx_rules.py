"""Write DataBuck row and dataset checks as a Great Expectations suite."""

import json
import re
from pathlib import Path


_COLUMN = r"`?([A-Za-z_][A-Za-z0-9_]*)`?"
_NUMBER = r"[-+]?\d+(?:\.\d+)?"
_STRING = r"'[^'\\]*'"
_LITERAL = rf"(?:{_NUMBER}|{_STRING})"
_SEVERITY = {"warn": "warning", "drop": "info", "fail": "critical"}
_NUMERIC_TYPES = {"byte", "short", "int", "integer", "bigint", "long"}


def _column_info(name, fields):
    field = fields.get(name.lower())
    if field is None:
        return None, None
    data_type = field.dataType.simpleString().lower()
    return field.name, data_type


def _literal_value(value):
    if value.startswith("'"):
        return value[1:-1]
    return float(value) if "." in value else int(value)


def _literal_matches(value, data_type):
    if value.startswith("'"):
        return data_type == "string"
    return data_type in _NUMERIC_TYPES or data_type.startswith("decimal(")


def _native_parts(expression, fields):
    """Map only simple checks with equivalent GX null and type semantics."""
    sql = expression.strip()
    if sql.startswith("(") and sql.endswith(")") and "(" not in sql[1:-1]:
        sql = sql[1:-1].strip()

    match = re.fullmatch(rf"{_COLUMN}\s+IS\s+NOT\s+NULL", sql, re.I)
    if match:
        column, _ = _column_info(match.group(1), fields)
        if column:
            return [("ExpectColumnValuesToNotBeNull", {"column": column})]
        return None

    match = re.fullmatch(rf"{_COLUMN}\s+BETWEEN\s+({_NUMBER})\s+AND\s+({_NUMBER})", sql, re.I)
    if match:
        column, data_type = _column_info(match.group(1), fields)
        if column and _literal_matches(match.group(2), data_type):
            return [
                ("ExpectColumnValuesToBeBetween", {
                    "column": column, "min_value": _literal_value(match.group(2)),
                    "max_value": _literal_value(match.group(3)),
                }),
                ("ExpectColumnValuesToNotBeNull", {"column": column}),
            ]
        return None

    match = re.fullmatch(rf"{_COLUMN}\s*(>=|<=|>|<)\s*({_NUMBER})", sql)
    if match:
        column, data_type = _column_info(match.group(1), fields)
        if column and _literal_matches(match.group(3), data_type):
            value = _literal_value(match.group(3))
            bounds = {
                ">=": {"min_value": value}, ">": {"min_value": value, "strict_min": True},
                "<=": {"max_value": value}, "<": {"max_value": value, "strict_max": True},
            }[match.group(2)]
            return [
                ("ExpectColumnValuesToBeBetween", {"column": column, **bounds}),
                ("ExpectColumnValuesToNotBeNull", {"column": column}),
            ]
        return None

    match = re.fullmatch(rf"{_COLUMN}\s*(=|==|<>|!=)\s*({_LITERAL})", sql)
    if match:
        column, data_type = _column_info(match.group(1), fields)
        if column and _literal_matches(match.group(3), data_type):
            kind = ("ExpectColumnValuesToBeInSet" if match.group(2) in {"=", "=="}
                    else "ExpectColumnValuesToNotBeInSet")
            return [
                (kind, {"column": column, "value_set": [_literal_value(match.group(3))]}),
                ("ExpectColumnValuesToNotBeNull", {"column": column}),
            ]
        return None

    match = re.fullmatch(rf"{_COLUMN}\s+(NOT\s+)?IN\s*\((.+)\)", sql, re.I | re.S)
    if match:
        column, data_type = _column_info(match.group(1), fields)
        tokens = [part.strip() for part in match.group(3).split(",")]
        if (column and tokens and all(re.fullmatch(_LITERAL, item) for item in tokens)
                and all(_literal_matches(item, data_type) for item in tokens)):
            kind = ("ExpectColumnValuesToNotBeInSet" if match.group(2)
                    else "ExpectColumnValuesToBeInSet")
            return [
                (kind, {"column": column,
                        "value_set": [_literal_value(item) for item in tokens]}),
                ("ExpectColumnValuesToNotBeNull", {"column": column}),
            ]
        return None

    match = re.fullmatch(rf"{_COLUMN}\s+(?:RLIKE|REGEXP)\s+({_STRING})", sql, re.I | re.S)
    if match:
        column, data_type = _column_info(match.group(1), fields)
        if column and data_type == "string":
            return [
                ("ExpectColumnValuesToMatchRegex",
                 {"column": column, "regex": _literal_value(match.group(2))}),
                ("ExpectColumnValuesToNotBeNull", {"column": column}),
            ]
    return None


def _escape_query_braces(sql):
    # GX formats {batch}; braces inside a regex or other SQL literal stay literal.
    return sql.replace("{", "{{").replace("}", "}}")


def write_gx_suite(grouped, path, *, df, dataset_checks=None, table_name=None):
    """Serialize a GX suite using the same selected rules as JSON export."""
    if df is None:
        raise ValueError("GX export requires the source DataFrame")
    try:
        import great_expectations as gx
    except ImportError as exc:
        raise ImportError(
            "GX export requires great_expectations; install databuck-sdk[gx]"
        ) from exc

    fields = {field.name.lower(): field for field in df.schema.fields}
    expectations = []
    for action in ("warn", "drop", "fail"):
        for name, expression in grouped.get(action, {}).items():
            if not isinstance(expression, str) or not expression.strip():
                raise ValueError("GX rule {} has no SQL expression".format(name))
            try:
                actual_type = df.selectExpr(
                    "({}) AS __databuck_gx_rule".format(expression)
                ).schema.fields[0].dataType.simpleString().lower()
            except Exception as exc:
                raise ValueError("GX rule {} has invalid SQL: {}".format(name, exc)) from exc
            if actual_type != "boolean":
                raise ValueError("GX rule {} must produce a boolean value".format(name))
            meta = {"databuck_rule": name, "databuck_action": action,
                    "databuck_expression": expression}
            parts = _native_parts(expression, fields)
            if parts:
                for index, (kind, kwargs) in enumerate(parts):
                    expectations.append(getattr(gx.expectations, kind)(
                        **kwargs, severity=_SEVERITY[action],
                        description="DataBuck rule {}".format(name),
                        meta={**meta, "part": index + 1},
                    ))
            else:
                query = "SELECT * FROM {{batch}} WHERE NOT COALESCE(({}), FALSE)".format(
                    _escape_query_braces(expression)
                )
                expectations.append(gx.expectations.UnexpectedRowsExpectation(
                    unexpected_rows_query=query, severity=_SEVERITY[action],
                    description="DataBuck rule {}".format(name), meta=meta,
                ))

    for name, check in (dataset_checks or {}).items():
        sql = check.get("sql")
        if not isinstance(sql, str) or "{{DATAFRAME}}" not in sql:
            raise ValueError("GX dataset check {} needs {{DATAFRAME}} SQL".format(name))
        query = "{batch}".join(
            _escape_query_braces(part) for part in sql.split("{{DATAFRAME}}")
        )
        expectations.append(gx.expectations.UnexpectedRowsExpectation(
            unexpected_rows_query=query,
            description="DataBuck dataset check {}".format(name),
            meta={"databuck_rule": name, "databuck_action": None,
                  "databuck_check_type": "dataset"},
        ))

    output = Path(path)
    if not output.name.lower().endswith(".gx.json"):
        raise ValueError("GX export path must end with .gx.json")
    suite_name = output.name[:-8]
    suite = gx.ExpectationSuite(
        name=suite_name,
        expectations=expectations,
        meta={"source": "DataBuck", **({"table": table_name} if table_name else {})},
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as stream:
        json.dump(suite.to_json_dict(), stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    return str(output)
