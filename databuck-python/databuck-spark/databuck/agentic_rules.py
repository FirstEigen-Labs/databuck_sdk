"""Lightweight, DataFrame-first context-aware rule generation for notebooks."""

import json
import os
import re
import zipfile
from typing import Any
from xml.etree import ElementTree


DEFAULT_MODEL = "gemini-2.5-flash"
DATAFRAME_PLACEHOLDER = "{{DATAFRAME}}"


def discover_rules(df: Any, context: dict) -> list[dict[str, str]]:
    """Generate portable SQL data-quality rules for the supplied DataFrame.

    This function uses the caller-owned DataFrame only. It does not create or
    stop a Spark session, connect to DataBuck databases, read source systems,
    or persist rules.
    """
    api_key = (
        context.get("gemini_api_key")
        or os.environ.get("GEMINI_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise ValueError(
            "A Gemini API key is required. Set context['gemini_api_key'], "
            "or the GEMINI_API_KEY environment variable."
        )

    rule_count = int(context.get("rule_count", 10))
    if rule_count < 1 or rule_count > 50:
        raise ValueError("context['rule_count'] must be between 1 and 50")

    reference_paths = _get_reference_paths(context)
    pdf_paths = [path for path in reference_paths if path.lower().endswith(".pdf")]
    word_documents = [
        {"name": os.path.basename(path), "text": _read_docx_text(path)}
        for path in reference_paths if path.lower().endswith(".docx")
    ]
    prompt = _build_prompt(df, context, rule_count, reference_paths, word_documents)
    response_text, audit_response_text = _generate_and_audit_with_gemini(
        api_key=api_key,
        model=context.get("gemini_model", DEFAULT_MODEL),
        prompt=prompt,
        pdf_paths=pdf_paths,
    )
    rules = _parse_rules(response_text)

    if not rules:
        raise ValueError("Gemini did not return any valid SQL rules")

    _apply_audit_results(rules, audit_response_text)
    returned_rules = rules if context.get("include_failed_rules", False) else [
        rule for rule in rules if rule["audit_status"] == "PASSED"
    ]

    if not returned_rules:
        raise ValueError("Gemini audit rejected every generated rule")

    for index, rule in enumerate(returned_rules, start=1):
        print("\nRule {}: {}".format(index, rule["name"]))
        print("Description: {}".format(rule["description"]))
        print("Reference context: {}".format(rule["reference_context"]))
        print("Audit status: {}".format(rule["audit_status"]))
        print("Audit reason: {}".format(rule["audit_reason"]))
        print(rule["sql"])

    return returned_rules


def _build_prompt(
    df: Any, context: dict, rule_count: int, reference_paths: list[str],
    word_documents: list[dict[str, str]]
) -> str:
    schema = [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in df.schema.fields
    ]

    safe_context = {
        key: value
        for key, value in context.items()
        if key not in {
            "gemini_api_key",
            "gemini_model",
            "rule_count",
            "pdf_paths",
            "include_failed_rules",
        }
    }
    document_names = [os.path.basename(path) for path in reference_paths]

    return """You are a data-quality rule generator.
Generate up to {rule_count} accurate data-quality SQL rules for the DataFrame
described below. It is acceptable to return fewer rules when the supplied
reference context does not define enough unambiguous controls.
The attached PDFs and Word document text are approved reference context. Use
them when they support a rule. If a rule is based on a document, identify the
relevant policy text in reference_context.

Return only a JSON array. Every item must contain exactly these string fields:
name, description, reference_context, sql.

Reference requirements:
- Every rule must be supported by exactly one explicit policy or business
  statement from the supplied context or reference documents.
- reference_context must quote that exact supporting statement. Do not
  paraphrase, combine statements, or claim a reference that does not exist.
- Do not infer additional business meaning, ID patterns, allowed values,
  thresholds, column relationships, or statuses that are not explicitly stated.
- Do not downgrade a "valid" requirement into a null/empty check. A validity
  rule requires an explicit format, allowed-value list, or validation condition.
- A null/empty check is permitted only when the reference explicitly says
  required, present, non-null, non-empty, or mandatory.
- If a reference is ambiguous or does not define an executable check, omit it.

SQL requirements:
- Use generic SQL only. Do not use BigQuery, Databricks, Spark, Snowflake, or
  database-vendor-specific syntax.
- Every SQL value must have exactly this shape:
  SELECT * FROM {placeholder} WHERE <invalid-row condition>
- The WHERE condition must use only columns from the supplied DataFrame.
- Do not use joins, grouping, ordering, subqueries, or additional statements.
- A rule must return invalid rows, so its WHERE clause must describe the
  condition that fails the rule.
- Use only columns in the supplied schema.
- Do not invent business values, tables, joins, columns, formats, thresholds,
  allowed values, or rule logic.
- Do not generate INSERT, UPDATE, DELETE, MERGE, DROP, CREATE, ALTER, or CALL.

Before returning each rule, internally verify that the SQL checks exactly what
the quoted reference_context states. Omit the rule if they do not match.

DataFrame schema:
{schema}

Business context:
{context}

Reference document names:
{documents}

Word document text:
{word_documents}
""".format(
        rule_count=rule_count,
        placeholder=DATAFRAME_PLACEHOLDER,
        schema=json.dumps(schema, indent=2),
        context=json.dumps(safe_context, default=str, indent=2),
        documents=json.dumps(document_names),
        word_documents=json.dumps(word_documents, ensure_ascii=False),
    )


def _generate_and_audit_with_gemini(
    api_key: str, model: str, prompt: str, pdf_paths: list[str]
) -> tuple[str, str]:
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise ImportError(
            "Gemini rule discovery requires google-genai. Install it with "
            "`pip install google-genai`."
        ) from exc

    client = genai.Client(api_key=api_key)
    uploaded_files = []
    try:
        for pdf_path in pdf_paths:
            uploaded_files.append(client.files.upload(file=pdf_path))

        response = client.models.generate_content(
            model=model,
            contents=[prompt, *uploaded_files],
            config=types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json",
            ),
        )
        candidate_response_text = (response.text or "").strip()
        candidate_rules = _parse_rules(candidate_response_text)
        if not candidate_rules:
            return candidate_response_text, "[]"

        audit_response = client.models.generate_content(
            model=model,
            contents=[_build_audit_prompt(prompt, candidate_rules), *uploaded_files],
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )
        return candidate_response_text, (audit_response.text or "").strip()
    finally:
        # The PDFs are needed only for this single rule-generation request.
        for uploaded_file in uploaded_files:
            try:
                client.files.delete(name=uploaded_file.name)
            except Exception:
                pass


def _build_audit_prompt(reference_prompt: str, candidate_rules: list[dict[str, str]]) -> str:
    return """You are an independent data-quality rule auditor. Do not create,
rewrite, repair, or improve rules. Audit every candidate rule against the
reference material below.

Return only a JSON array. Every item must contain exactly these fields:
rule_index (integer, starting at 1), audit_status (PASSED or FAILED), and
audit_reason (string).

Pass a rule only when all conditions are true:
- Its reference_context exactly quotes one explicit reference statement.
- Its SQL checks exactly the control stated in that reference.
- The SQL uses only the supplied DataFrame columns.
- The SQL has exactly this form: SELECT * FROM {placeholder} WHERE
  <invalid-row condition>. It contains no joins, grouping, ordering,
  subqueries, or additional statements.
- The SQL does not invent formats, values, thresholds, relationships, or logic.
- A "valid" reference has an explicit format, allowed-value list, or validation
  condition; null/empty SQL alone must fail such a rule.
- A null/empty SQL check is supported only by a reference that explicitly says
  required, present, non-null, non-empty, or mandatory.

Fail ambiguous, partly supported, over-broad, or mismatched rules. Explain the
specific mismatch in audit_reason.

Reference material:
{reference_prompt}

Candidate rules:
{candidate_rules}
""".format(
        placeholder=DATAFRAME_PLACEHOLDER,
        reference_prompt=reference_prompt,
        candidate_rules=json.dumps(candidate_rules, indent=2),
    )


def _apply_audit_results(rules: list[dict[str, str]], audit_response_text: str) -> None:
    audits_by_index = _parse_audit_results(audit_response_text, len(rules))
    for index, rule in enumerate(rules, start=1):
        audit = audits_by_index.get(index)
        if audit is None:
            rule["audit_status"] = "FAILED"
            rule["audit_reason"] = "Auditor did not return a valid audit result."
        else:
            rule["audit_status"] = audit["audit_status"]
            rule["audit_reason"] = audit["audit_reason"]


def _parse_audit_results(audit_response_text: str, rule_count: int) -> dict[int, dict[str, str]]:
    try:
        raw_audits = json.loads(_strip_markdown_fence(audit_response_text))
    except json.JSONDecodeError:
        return {}

    if not isinstance(raw_audits, list):
        return {}

    audits_by_index = {}
    for raw_audit in raw_audits:
        if not isinstance(raw_audit, dict):
            continue
        rule_index = raw_audit.get("rule_index")
        audit_status = str(raw_audit.get("audit_status", "")).strip().upper()
        audit_reason = str(raw_audit.get("audit_reason", "")).strip()
        if (
            not isinstance(rule_index, int)
            or rule_index < 1
            or rule_index > rule_count
            or audit_status not in {"PASSED", "FAILED"}
            or not audit_reason
        ):
            continue
        audits_by_index[rule_index] = {
            "audit_status": audit_status,
            "audit_reason": audit_reason,
        }
    return audits_by_index


def _get_reference_paths(context: dict) -> list[str]:
    reference_paths = context.get("pdf_paths", [])
    if reference_paths is None:
        return []
    if not isinstance(reference_paths, (list, tuple)) or not all(
        isinstance(path, str) and path.strip() for path in reference_paths
    ):
        raise TypeError("context['pdf_paths'] must be a list of non-empty file paths")

    resolved_paths = []
    for path in reference_paths:
        if not path.lower().endswith((".pdf", ".docx")):
            raise ValueError("Only PDF and DOCX files are supported in context['pdf_paths']")
        if not os.path.isfile(path):
            raise FileNotFoundError(
                "Reference document was not found on the notebook driver: {}".format(path)
            )
        resolved_paths.append(path)
    return resolved_paths


def _read_docx_text(path: str) -> str:
    """Read paragraph and table text from a Word document without extra dependencies."""
    paragraph_tag = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p"
    text_tag = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"
    try:
        with zipfile.ZipFile(path) as archive:
            document = ElementTree.fromstring(archive.read("word/document.xml"))
    except (OSError, KeyError, ValueError, zipfile.BadZipFile,
            ElementTree.ParseError) as exc:
        raise ValueError("Could not read Word document: {}".format(path)) from exc
    paragraphs = [
        "".join(node.text or "" for node in paragraph.iter(text_tag)).strip()
        for paragraph in document.iter(paragraph_tag)
    ]
    result = "\n".join(paragraph for paragraph in paragraphs if paragraph)
    if not result:
        raise ValueError("Word document contains no readable text: {}".format(path))
    return result


def _parse_rules(response_text: str) -> list[dict[str, str]]:
    try:
        raw_rules = json.loads(_strip_markdown_fence(response_text))
    except json.JSONDecodeError as exc:
        raise ValueError("Gemini returned invalid JSON for generated rules") from exc

    if not isinstance(raw_rules, list):
        raise ValueError("Gemini must return a JSON array of rules")

    rules = []
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            continue

        name = str(raw_rule.get("name", "")).strip()
        description = str(raw_rule.get("description", "")).strip()
        reference_context = str(raw_rule.get("reference_context", "")).strip()
        sql = str(raw_rule.get("sql", "")).strip()
        if (
            not name
            or not description
            or not reference_context
            or not re.match(r"^SELECT\b", sql, re.IGNORECASE)
        ):
            continue
        if DATAFRAME_PLACEHOLDER not in sql:
            continue
        if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|CALL)\b", sql, re.IGNORECASE):
            continue

        rules.append(
            {
                "name": name,
                "description": description,
                "reference_context": reference_context,
                "sql": sql,
            }
        )

    return rules


def _strip_markdown_fence(value: str) -> str:
    value = value.strip()
    if value.startswith("```") and value.endswith("```"):
        value = value.split("\n", 1)[1].rsplit("```", 1)[0]
    return value.strip()
