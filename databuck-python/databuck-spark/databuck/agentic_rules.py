"""Lightweight, DataFrame-first context-aware rule generation for notebooks."""

import json
import os
import re
import zipfile
from typing import Any
from xml.etree import ElementTree


DEFAULT_MODEL = "gemini-2.5-flash"
DATAFRAME_PLACEHOLDER = "{{DATAFRAME}}"
INTEL_SOURCE = "BuckGPT_Intel"
_CONTROL_CONTEXT_KEYS = {
    "gemini_api_key", "gemini_model", "rule_count", "pdf_paths",
    "include_failed_rules", "domain_name",
}


def has_rule_reference_context(context: dict) -> bool:
    """Whether context supplies a domain or statements for BuckGPT rules."""
    return bool(context.get("pdf_paths") or context.get("domain_name")) or any(
        value for key, value in context.items() if key not in _CONTROL_CONTEXT_KEYS
    )


def _allowed_rule_sources(context: dict, reference_paths: list[str]) -> set[str]:
    sources = {INTEL_SOURCE}
    sources.update(os.path.basename(path) for path in reference_paths)
    if any(value for key, value in context.items() if key not in _CONTROL_CONTEXT_KEYS):
        sources.add("business_context")
    return sources


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

    reference_paths = _get_reference_paths(context)
    allowed_sources = _allowed_rule_sources(context, reference_paths)
    pdf_paths = [path for path in reference_paths if path.lower().endswith(".pdf")]
    word_documents = [
        {"name": os.path.basename(path), "text": _read_docx_text(path)}
        for path in reference_paths if path.lower().endswith(".docx")
    ]
    schema_columns = [field.name for field in df.schema.fields]
    general_sample_rows = _sample_rows(df, limit=50)
    prompt = _build_prompt(df, context, reference_paths, word_documents,
                           sample_rows=general_sample_rows[:10])
    general_prompt = _build_general_prompt(df, context, general_sample_rows)
    cross_column_prompt = _build_cross_column_prompt(
        df, context, general_sample_rows, reference_paths, word_documents,
        allowed_sources,
    )
    response_text, audit_response_text = _generate_and_audit_with_gemini(
        api_key=api_key,
        model=context.get("gemini_model", DEFAULT_MODEL),
        prompt=prompt,
        general_prompt=general_prompt,
        cross_column_prompt=cross_column_prompt,
        pdf_paths=pdf_paths,
        allowed_sources=allowed_sources,
        schema_columns=schema_columns,
    )
    rules = _parse_rules(response_text, allowed_sources,
                         schema_columns=schema_columns)

    if not rules:
        print("BuckGPT: no eligible rules found", flush=True)
        return []

    _apply_audit_results(rules, audit_response_text)
    print("BuckGPT: audit passed {} document-backed and {} general rules".format(
        sum(rule["audit_status"] == "PASSED" for rule in rules
            if rule["source_document"] != INTEL_SOURCE),
        sum(rule["audit_status"] == "PASSED" for rule in rules
            if rule["source_document"] == INTEL_SOURCE),
    ), flush=True)
    for rule in rules:
        if rule["audit_status"] == "FAILED":
            print("BuckGPT: rejected rule from {}: {}".format(
                rule["source_document"], rule["audit_reason"]), flush=True)
    returned_rules = rules if context.get("include_failed_rules", False) else [
        rule for rule in rules if rule["audit_status"] == "PASSED"
    ]

    if not returned_rules:
        raise ValueError("Gemini audit rejected every generated rule")

    for index, rule in enumerate(returned_rules, start=1):
        print("\nRule {}: {}".format(index, rule["name"]))
        print("Description: {}".format(rule["description"]))
        print("Source document: {}".format(rule["source_document"]))
        print("Reference context: {}".format(rule["reference_context"]))
        print("Audit status: {}".format(rule["audit_status"]))
        print("Audit reason: {}".format(rule["audit_reason"]))
        print(rule["sql"])

    return returned_rules


def _sample_rows(df: Any, limit: int = 10) -> list[dict[str, Any]]:
    """Collect up to ``limit`` existing DataFrame rows for BuckGPT generation."""
    return [row.asDict(recursive=True) for row in df.limit(limit).collect()[:limit]]


def _compact_sample_rows(df: Any, sample_rows: list[dict[str, Any]]) -> str:
    """List column names once while preserving every sampled value."""
    columns = [field.name for field in df.schema.fields]
    return json.dumps({
        "columns": columns,
        "rows": [[row.get(column) for column in columns] for row in sample_rows],
    }, default=str, ensure_ascii=False)


def _build_prompt(
    df: Any, context: dict, reference_paths: list[str],
    word_documents: list[dict[str, str]],
    sample_rows: list[dict[str, Any]] | None = None,
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
        if key not in _CONTROL_CONTEXT_KEYS
    }
    domain_name = context.get("domain_name")
    if domain_name is not None and (
        not isinstance(domain_name, str) or not domain_name.strip()
    ):
        raise ValueError("context['domain_name'] must be a non-empty string")
    document_names = [os.path.basename(path) for path in reference_paths]
    allowed_sources = sorted(_allowed_rule_sources(context, reference_paths)
                             - {INTEL_SOURCE})

    return """You are a data-quality rule generator.
Generate accurate data-quality SQL rules for the DataFrame described below.
Choose the number of rules based on the distinct, well-supported checks you
find; there is no target count. Omit redundant or weak rules.
The attached PDFs and Word document text are approved reference context. Use
all of them when they support a rule. Generate only rules supported by an
explicit statement in a supplied document or Business context. Do not infer
rules from the schema or domain alone.
Use the sample rows to understand how schema columns represent policy concepts.
Read the attached reference documents first and derive rules from their exact
statements. Sample values are examples, not policy requirements or proof of
uniqueness. A policy rule may matter even when no sampled row violates it.
If a rule is based on a document, identify the relevant policy text in
reference_context.
Use the business domain to interpret the terminology and prioritize relevant
controls. The domain name alone is not a policy statement.
Exclude system-managed metadata dates and timestamps used only to record row
creation, last update, ingestion, loading, or ETL audit activity. Examples
include created_at, create_at, updated_at, update_at, ingested_at, load_ts,
and etl_timestamp. Never generate a BuckGPT rule whose SQL uses such a column,
even for completeness, freshness, chronology, or a cross-column relationship.
Business event dates such as loan issue and payment dates remain eligible.

Return a nonempty JSON array when at least one supported rule can be derived.
Do not return [] merely because some documents are irrelevant. Return [] only
when no supplied statement supports an executable check.
Every item must contain exactly these string fields:
name, description, source_document, reference_context, sql.

Reference requirements:
- For a context-aware rule, use exactly one explicit policy or business
  statement from the supplied context or reference documents.
- For a context-aware rule, reference_context must quote that exact supporting
  statement. Do not paraphrase, combine statements, or claim a nonexistent
  reference. source_document must be the exact filename containing the quote,
  or "business_context" for a quote from the Business context block.
- A policy may name a business concept rather than a DataFrame column. Map the
  concept to schema columns only when the meaning is unambiguous, and explain
  that mapping in description. The quote must still state a specific control;
  a generic call for accuracy does not justify an arbitrary SQL check.
- Never use a full path, URL, section title, an empty value, or an invented
  source_document value.
- Do not infer ID patterns, allowed values, thresholds, statuses, or business
  relationships from a column name alone. Do not assert that an ID must be
  non-null or is a primary/foreign key merely because its name ends in ID.
- Do not downgrade a "valid" requirement into a null/empty check. A validity
  rule requires an explicit format, allowed-value list, or validation condition.
- A context-aware null/empty check is permitted only when the reference
  explicitly says required, present, non-null, non-empty, or mandatory.
- Look for meaningful intercolumn relationships involving 3 to 5 columns in
  the schema. If one explicit statement defines such a relationship, create
  one rule checking the full relationship across all stated columns.
  Include every involved column in the invalid-row SQL condition; do not
  split a supported full relationship into weaker pairwise checks.
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
its quoted statement supports. Omit the rule if they do not match.

DataFrame schema:
{schema}

Business domain:
{domain_name}

Business context:
{context}

Reference document names:
{documents}

Allowed source_document values (copy one exactly for each rule):
{sources}

Word document text:
{word_documents}

Up to 10 sample records (columns listed once, rows in the same order):
{sample_rows}
""".format(
        placeholder=DATAFRAME_PLACEHOLDER,
        schema=json.dumps(schema, indent=2),
        domain_name=domain_name.strip() if domain_name is not None else "Not provided",
        context=json.dumps(safe_context, default=str, indent=2),
        documents=json.dumps(document_names),
        sources=json.dumps(allowed_sources),
        word_documents=json.dumps(word_documents, ensure_ascii=False),
        sample_rows=_compact_sample_rows(df, sample_rows or []),
    )


def _build_general_prompt(df: Any, context: dict,
                          sample_rows: list[dict[str, Any]]) -> str:
    schema = [{"name": field.name, "type": field.dataType.simpleString(),
               "nullable": field.nullable} for field in df.schema.fields]
    return """You are a business data-quality expert. Generate only important,
defensible general SQL rules for the supplied DataFrame. Review the full
schema systematically; for a wide schema, seek broad coverage, including 60
or more distinct rules when that many are genuinely justified. Do not pad the
list with redundant or speculative checks to reach a number. Use the business
domain, schema, and up to 50 sample records to understand the data, but do not
mistake a sample pattern for a universal contract. Return [] when a rule could
reject legitimate records.

Consider non-negative checks where negative values are semantically impossible,
and non-positive checks where positive values are semantically impossible.
For either direction, require a clear business rationale from the column meaning
and domain; do not assume all amounts, counts, balances, or adjustments have
the same sign constraint. Systematically inspect related amount, count, status,
date, rate, and balance columns for meaningful cross-column relationships
involving 2 to 4 columns. Create a rule only when the relationship follows
clearly from the column meanings and business domain; explain its rationale.
Also consider uniqueness of a clear business key
or meaningful combination of key columns. Do not create routine ID null
checks, duplicate policy rules, or invented thresholds, formats, categories,
or relationships. Do not infer a uniqueness contract merely because values in
50 rows differ. Do not require uniqueness of values that can legitimately recur.
Do not create standalone null checks for any column, including amounts, dates,
terms, and identifiers. DataBuck auto-discovery already handles null checks.
Do not claim a column is mandatory solely to justify an IS NULL rule.
Exclude system-managed metadata dates and timestamps used only for row creation,
last update, ingestion, loading, or ETL audit activity (for example created_at,
create_at, updated_at, update_at, ingested_at, load_ts, etl_timestamp). Do not
generate any BuckGPT rule whose SQL uses one of these columns, including
freshness, chronology, sign, or cross-column checks. Business event dates such
as loan issue and payment dates remain eligible.

Return a JSON array. Each row-level rule must have these string fields: name,
description, source_document, reference_context, sql. Set source_document to
"BuckGPT_Intel" and begin reference_context with "BuckGPT_Intel: " followed
by a short explanation of the business reason for the check. SQL must return
invalid rows in this form:
SELECT * FROM {placeholder} WHERE <invalid-row condition>
Use only the supplied columns. No joins, aggregation, windows, subqueries, or
additional statements.

For a defensible uniqueness check, instead include check_type "uniqueness",
columns as an array of the exact key column names, and sql as an empty string.
Keep name, description, source_document, and reference_context as above.
The SDK will construct the dataset-level duplicate query from those columns.
Only propose keys with a strong business identity rationale; sample uniqueness
alone is insufficient evidence.

Business domain: {domain}
DataFrame schema: {schema}
Sample records (columns listed once, rows in the same order): {sample_rows}
""".format(
        placeholder=DATAFRAME_PLACEHOLDER,
        domain=context.get("domain_name") or "Not provided",
        schema=json.dumps(schema, default=str),
        sample_rows=_compact_sample_rows(df, sample_rows),
    )


def _build_cross_column_prompt(
    df: Any, context: dict, sample_rows: list[dict[str, Any]],
    reference_paths: list[str], word_documents: list[dict[str, str]],
    allowed_sources: set[str],
) -> str:
    schema = [{"name": field.name, "type": field.dataType.simpleString(),
               "nullable": field.nullable} for field in df.schema.fields]
    business_context = {key: value for key, value in context.items()
                        if key not in _CONTROL_CONTEXT_KEYS}
    return """You are a {domain} subject matter expert and data engineer.
Generate additional, high-value SQL rules specifically for relationships
between 2 to 5 DataFrame columns. Examine the full schema and the supplied
reference documents together. Look for conditional population, chronology,
arithmetic reconciliation, subset or ordering constraints, and status-dependent
relationships. Check all relevant columns in each relationship. Do not create
single-column checks, standalone null checks, or uniqueness checks.
Exclude system-managed metadata dates and timestamps used only for row creation,
last update, ingestion, loading, or ETL audit activity (for example created_at,
create_at, updated_at, update_at, ingested_at, load_ts, etl_timestamp). Never
use them in cross-column SQL or infer business chronology from them. Business
event dates such as loan issue and payment dates remain eligible.

First use explicit statements from the attached PDFs, Word document text, or
Business context. For each document-backed rule, quote one exact supporting
statement in reference_context and set source_document to the exact filename,
or to "business_context" for an explicit Business context statement. A policy
may use business terms rather than column names; map them only when the match
to schema columns is unambiguous and explain the mapping in description.

When no supplied statement supports a defensible general relationship, use
source_document "BuckGPT_Intel" and begin reference_context with
"BuckGPT_Intel: " followed by the business rationale. Sample values can
clarify column meaning but cannot establish a universal requirement.
Never invent numeric bounds, tolerances, formulas, date parsing conventions,
statuses, or missing-value exceptions. Include an exact numeric tolerance or
condition only when it is explicitly supported by a reference or Business
context; otherwise omit that rule. Do not repeat the previously generated
rules. Return [] if no additional defensible cross-column rule exists.

Return only a JSON array of objects with string fields name, description,
source_document, reference_context, and sql. Use only these exact source_document
values: {sources}. Each sql must have exactly this form:
SELECT * FROM {placeholder} WHERE <invalid-row condition>
Use only supplied columns and generic SQL. No joins, aggregation, windows,
subqueries, or extra statements. The condition must return invalid rows.

Business domain: {domain}
DataFrame schema: {schema}
Business context: {context}
Reference document names: {documents}
Word document text: {word_documents}
Up to 50 sample records (columns listed once, rows in the same order): {sample_rows}
""".format(
        domain=context.get("domain_name") or "Not provided",
        schema=json.dumps(schema, default=str),
        context=json.dumps(business_context, default=str, ensure_ascii=False),
        documents=json.dumps([os.path.basename(path) for path in reference_paths]),
        word_documents=json.dumps(word_documents, ensure_ascii=False),
        sources=json.dumps(sorted(allowed_sources)),
        placeholder=DATAFRAME_PLACEHOLDER,
        sample_rows=_compact_sample_rows(df, sample_rows),
    )


def _referenced_schema_columns(sql: str, schema_columns: list[str]) -> set[str]:
    without_literals = re.sub(r"'(?:''|[^'])*'", "''", sql)
    return {column for column in schema_columns if re.search(
        r"(?<![A-Za-z0-9_]){}(?![A-Za-z0-9_])".format(re.escape(column)),
        without_literals, flags=re.IGNORECASE,
    )}


def _generate_and_audit_with_gemini(
    api_key: str, model: str, prompt: str, pdf_paths: list[str],
    allowed_sources: set[str],
    general_prompt: str | None = None,
    cross_column_prompt: str | None = None,
    schema_columns: list[str] | None = None,
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

        def generate_phase(phase_prompt: str, phase_sources: set[str],
                           files: list[Any], retry_on_empty: bool = False,
                           ) -> tuple[list[dict[str, str]], list[str]]:
            rejection_reasons = []
            for attempt in range(2):
                generation_prompt = phase_prompt
                if attempt:
                    source_guidance = ""
                    if INTEL_SOURCE in phase_sources:
                        source_guidance += (
                            "BuckGPT_Intel rules need a reference_context beginning "
                            "'BuckGPT_Intel: ' with a clear business rationale. "
                            "Uniqueness checks need check_type 'uniqueness', valid "
                            "columns, and empty sql; row rules need SELECT SQL. "
                        )
                    if phase_sources - {INTEL_SOURCE}:
                        source_guidance += (
                            "Document or Business context rules need an exact quoted "
                            "reference_context from the named source. "
                        )
                    generation_prompt += (
                        "\nThe previous response contained no valid rules: {}. "
                        "Try again. Use only these exact source_document values: {}. "
                        "{}"
                        "Row rules need SQL beginning SELECT * FROM {} WHERE. "
                        "Return [] only if no supported rule exists."
                    ).format(", ".join(sorted(set(rejection_reasons))) or "empty response",
                             json.dumps(sorted(phase_sources)), source_guidance,
                             DATAFRAME_PLACEHOLDER)
                response = client.models.generate_content(
                    model=model,
                    contents=[generation_prompt, *files],
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                    ),
                )
                response_text = (response.text or "").strip()
                rejection_reasons = []
                try:
                    rules = _parse_rules(response_text, phase_sources,
                                         rejection_reasons, schema_columns)
                except ValueError as exc:
                    rejection_reasons.append(str(exc))
                    rules = []
                if rules:
                    return rules, []
                if _strip_markdown_fence(response_text) == "[]":
                    if retry_on_empty and attempt == 0:
                        rejection_reasons = [
                            "no document-backed rule identified; review each reference "
                            "again for an explicit control relevant to the schema"
                        ]
                        continue
                    return [], []
            return [], rejection_reasons

        print("BuckGPT: generating rules", flush=True)
        document_sources = allowed_sources - {INTEL_SOURCE}
        document_rules = []
        rejection_reasons = []
        if document_sources:
            document_prompt = prompt + (
                "\nDocument-backed phase: review every attached PDF and all "
                "supplied Word document text together in this request for "
                "distinct executable controls involving "
                "the DataFrame schema. A policy need not use the exact DataFrame "
                "column names: map its business concepts to columns when the match "
                "is unambiguous, and explain the mapping in description. Return "
                "only rules supported by an exact quote from a supplied document "
                "or Business context statement. Use its exact filename as "
                "source_document, or business_context for an explicit statement "
                "there. Return [] only if none of the references supports a valid "
                "rule. Return every distinct, directly supported rule that "
                "applies to this schema; choose the number yourself."
            )
            generated_rules, source_rejections = generate_phase(
                document_prompt, document_sources, uploaded_files,
                retry_on_empty=True,
            )
            if not generated_rules and document_sources - {"business_context"}:
                print("BuckGPT: retrying document generation with a focused review",
                      flush=True)
                recovery_prompt = document_prompt + (
                    "\nFocused document review: the earlier response produced no "
                    "usable document rules. Inspect each attached PDF and each "
                    "supplied Word document for explicit controls that apply to "
                    "the schema. The 10 sample records illustrate column meaning "
                    "but do not need to contain a violation. Return every distinct "
                    "rule with an exact supporting quote and its correct filename. "
                    "Do not invent requirements or return BuckGPT_Intel rules. "
                    "Return [] only if no document contains an executable control."
                )
                generated_rules, recovery_rejections = generate_phase(
                    recovery_prompt, document_sources, uploaded_files,
                )
                source_rejections.extend(recovery_rejections)
            rejection_reasons.extend(source_rejections)
            seen_sql = set()
            for rule in generated_rules:
                sql_key = rule["sql"].strip().rstrip(";").lower()
                if sql_key not in seen_sql:
                    document_rules.append(rule)
                    seen_sql.add(sql_key)
            if not document_rules:
                print("BuckGPT: no document-backed rules found", flush=True)

        candidate_rules = list(document_rules)
        if general_prompt and INTEL_SOURCE in allowed_sources:
            print("BuckGPT: generating general rules", flush=True)
            general_phase_prompt = general_prompt + (
                "\nPreviously generated policy rules; do not duplicate them: {}"
            ).format(json.dumps([
                {"name": rule["name"], "sql": rule["sql"]}
                for rule in document_rules
            ]))
            general_rules, general_rejections = generate_phase(
                general_phase_prompt, {INTEL_SOURCE}, [],
            )
            rejection_reasons.extend(general_rejections)
            seen_sql = {rule["sql"].strip().rstrip(";").lower()
                        for rule in candidate_rules}
            for rule in general_rules:
                sql_key = rule["sql"].strip().rstrip(";").lower()
                if sql_key not in seen_sql:
                    candidate_rules.append(rule)
                    seen_sql.add(sql_key)
        if cross_column_prompt:
            print("BuckGPT: generating additional cross-column rules", flush=True)
            cross_phase_prompt = cross_column_prompt + (
                "\nPreviously generated rules; do not duplicate them: {}"
            ).format(json.dumps([
                {"name": rule["name"], "sql": rule["sql"]}
                for rule in candidate_rules
            ]))
            cross_rules, cross_rejections = generate_phase(
                cross_phase_prompt, allowed_sources, uploaded_files,
            )
            rejection_reasons.extend(cross_rejections)
            if schema_columns is not None:
                cross_rules = [rule for rule in cross_rules
                               if len(_referenced_schema_columns(
                                   rule["sql"], schema_columns)) >= 2]
            seen_sql = {rule["sql"].strip().rstrip(";").lower()
                        for rule in candidate_rules}
            added_cross_rules = 0
            for rule in cross_rules:
                sql_key = rule["sql"].strip().rstrip(";").lower()
                if sql_key not in seen_sql:
                    candidate_rules.append(rule)
                    seen_sql.add(sql_key)
                    added_cross_rules += 1
            print("BuckGPT: added {} cross-column candidates".format(
                added_cross_rules), flush=True)
        if not candidate_rules:
            if not rejection_reasons:
                return "[]", "[]"
            raise ValueError("Gemini did not return any valid SQL rules after one retry: {}"
                             .format(", ".join(sorted(set(rejection_reasons)))
                                     or "no rules were returned"))

        print("BuckGPT: generated {} document-backed and {} general candidates".format(
            sum(rule["source_document"] != INTEL_SOURCE for rule in candidate_rules),
            sum(rule["source_document"] == INTEL_SOURCE for rule in candidate_rules),
        ), flush=True)

        def audit_rules(rules: list[dict[str, str]]) -> str:
            response = client.models.generate_content(
                model=model,
                contents=[_build_audit_prompt(prompt + "\n" + (general_prompt or ""), rules),
                          *uploaded_files],
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            return (response.text or "").strip()

        print("BuckGPT: auditing rules", flush=True)
        audit_text = audit_rules(candidate_rules)
        audits = _parse_audit_results(audit_text, len(candidate_rules))
        document_indices = [
            index for index, rule in enumerate(candidate_rules, start=1)
            if rule["source_document"] != INTEL_SOURCE
        ]
        if document_indices and not any(
            audits.get(index, {}).get("audit_status") == "PASSED"
            for index in document_indices
        ):
            feedback = [
                "{}: {}".format(
                    candidate_rules[index - 1]["source_document"],
                    audits.get(index, {}).get(
                        "audit_reason", "Auditor returned no valid result."
                    ),
                )
                for index in document_indices
            ]
            print("BuckGPT: retrying document rules after audit rejection", flush=True)
            retry_prompt = document_prompt + (
                "\nThe previous document-backed candidates all failed audit: {}. "
                "Find different, directly supported controls in the supplied "
                "documents. Do not repeat the rejected SQL or infer a positivity, "
                "non-null, or threshold condition from a broad description. "
                "Return [] if no specific executable control is supported."
            ).format(json.dumps(feedback))
            revised_rules, retry_rejections = generate_phase(
                retry_prompt, document_sources, uploaded_files,
            )
            rejected_sql = {
                candidate_rules[index - 1]["sql"].strip().rstrip(";").lower()
                for index in document_indices
            }
            revised_rules = [
                rule for rule in revised_rules
                if rule["sql"].strip().rstrip(";").lower() not in rejected_sql
            ]
            if revised_rules:
                general_candidates = [
                    rule for rule in candidate_rules
                    if rule["source_document"] == INTEL_SOURCE
                ]
                candidate_rules = revised_rules + general_candidates
                print("BuckGPT: auditing revised rules", flush=True)
                audit_text = audit_rules(candidate_rules)
            elif retry_rejections:
                print("BuckGPT: document retry returned no valid rules: {}".format(
                    ", ".join(sorted(set(retry_rejections))),
                ), flush=True)
        return json.dumps(candidate_rules), audit_text
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
- A context-aware rule's reference_context exactly quotes one explicit
  statement in its named source_document. The filename must identify the
  document containing the quote; "business_context" must identify a statement
  in Business context.
- The document quote may use business concepts rather than column names. The
  description must explain an unambiguous mapping to DataFrame columns, and
  SQL must implement only the specific control in that quote. A broad accuracy
  statement alone does not support a specific SQL check.
- A BuckGPT_Intel rule must start reference_context with "BuckGPT_Intel: ".
  Its rationale must explain a defensible business check grounded in the
  schema and domain. Sample rows can clarify meaning but cannot establish
  universal non-negativity, uniqueness, or cross-column requirements alone.
  Reject speculative constraints and standalone null checks on any column for
  BuckGPT_Intel, even when the rationale calls that column fundamental.
- Reject any rule whose SQL uses a system-managed row creation, update,
  ingestion, load, or ETL audit timestamp (for example created_at, create_at,
  updated_at, update_at, ingested_at, load_ts, etl_timestamp). These are
  operational metadata, not business event dates. Business event dates such as
  loan issue and payment dates remain eligible.
- A BuckGPT_Intel uniqueness check has check_type "uniqueness", key columns
  from the schema, and a dataset-level SQL query that returns duplicate key
  groups. Pass it only when its key has a convincing business identity
  rationale. Do not infer uniqueness from sample distinctness alone.
  During generation the model leaves sql empty; before this audit the SDK
  fills sql with its canonical duplicate-group query. Do not reject the rule
  merely because sql is now nonempty.
- Its SQL checks exactly the quoted control or general rationale.
- The SQL uses only the supplied DataFrame columns.
- A row-level rule's SQL has exactly this form: SELECT * FROM {placeholder}
  WHERE <invalid-row condition>. It contains no joins, grouping, ordering,
  windows, subqueries, or additional statements. A uniqueness check instead
  groups by its declared key columns and returns groups with COUNT(*) > 1.
- The SQL does not invent formats, values, thresholds, or unsupported logic.
  Document-backed column relationships need an explicit reference.
- A "valid" reference has an explicit format, allowed-value list, or validation
  condition; null/empty SQL alone must fail such a rule.
- A context-aware null/empty SQL check needs a reference that explicitly says
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


def _uniqueness_sql(columns: list[str]) -> str:
    identifiers = ["`{}`".format(column.replace("`", "``")) for column in columns]
    keys = ", ".join(identifiers)
    present = " AND ".join("{} IS NOT NULL".format(name) for name in identifiers)
    return ("SELECT {}, COUNT(*) AS duplicate_count FROM {} WHERE {} "
            "GROUP BY {} HAVING COUNT(*) > 1").format(
                keys, DATAFRAME_PLACEHOLDER, present, keys)


def _is_standalone_null_check(sql: str) -> bool:
    match = re.fullmatch(
        r"SELECT\s+\*\s+FROM\s+\{\{DATAFRAME\}\}\s+WHERE\s+(.+?)\s*;?",
        sql.strip(), flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return False
    identifier = r'(?:`(?:``|[^`])+`|"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)'
    remaining, count = re.subn(rf"{identifier}\s+IS\s+NULL", "", match.group(1),
                               flags=re.IGNORECASE)
    return bool(count) and not re.sub(r"\b(?:AND|OR)\b|[\s()]", "", remaining,
                                      flags=re.IGNORECASE)


def _parse_rules(response_text: str, allowed_sources: set[str],
                 rejection_reasons: list[str] | None = None,
                 schema_columns: list[str] | None = None) -> list[dict[str, Any]]:
    try:
        raw_rules = json.loads(_strip_markdown_fence(response_text))
    except json.JSONDecodeError as exc:
        raise ValueError("Gemini returned invalid JSON for generated rules") from exc

    if not isinstance(raw_rules, list):
        raise ValueError("Gemini must return a JSON array of rules")
    if not raw_rules and rejection_reasons is not None:
        rejection_reasons.append("empty rule list")

    rules = []
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            if rejection_reasons is not None:
                rejection_reasons.append("rule is not a JSON object")
            continue

        name = str(raw_rule.get("name", "")).strip()
        description = str(raw_rule.get("description", "")).strip()
        source_document = str(raw_rule.get("source_document", "")).strip()
        reference_context = str(raw_rule.get("reference_context", "")).strip()
        sql = str(raw_rule.get("sql", "")).strip()
        if not name or not description or not reference_context:
            if rejection_reasons is not None:
                rejection_reasons.append("missing name, description, or reference_context")
            continue
        if source_document not in allowed_sources:
            if rejection_reasons is not None:
                rejection_reasons.append("source_document is not an allowed filename")
            continue
        if source_document == INTEL_SOURCE and not reference_context.startswith(
            INTEL_SOURCE + ": "
        ):
            if rejection_reasons is not None:
                rejection_reasons.append("BuckGPT_Intel rule needs a labeled rationale")
            continue
        check_type = raw_rule.get("check_type", "row")
        if check_type == "uniqueness":
            columns = raw_rule.get("columns")
            if (source_document != INTEL_SOURCE or not isinstance(columns, list)
                    or not columns or schema_columns is None
                    or any(not isinstance(column, str)
                           or column not in schema_columns for column in columns)
                    or len(columns) != len(set(columns))
                    or sql not in ("", _uniqueness_sql(columns))):
                if rejection_reasons is not None:
                    rejection_reasons.append(
                        "uniqueness check requires BuckGPT_Intel, valid key columns, and canonical sql"
                    )
                continue
            rules.append({
                "name": name, "description": description,
                "source_document": source_document,
                "reference_context": reference_context,
                "check_type": "uniqueness", "columns": columns,
                "sql": _uniqueness_sql(columns),
            })
            continue
        if check_type != "row":
            if rejection_reasons is not None:
                rejection_reasons.append("unknown check_type")
            continue
        if not re.match(r"^SELECT\b", sql, re.IGNORECASE):
            if rejection_reasons is not None:
                rejection_reasons.append("SQL does not begin with SELECT")
            continue
        if DATAFRAME_PLACEHOLDER not in sql:
            if rejection_reasons is not None:
                rejection_reasons.append("SQL is missing {{DATAFRAME}}")
            continue
        if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|CALL)\b", sql, re.IGNORECASE):
            if rejection_reasons is not None:
                rejection_reasons.append("SQL contains a forbidden statement")
            continue
        if source_document == INTEL_SOURCE and _is_standalone_null_check(sql):
            if rejection_reasons is not None:
                rejection_reasons.append("BuckGPT_Intel standalone null checks are not allowed")
            continue
        rules.append(
            {
                "name": name,
                "description": description,
                "source_document": source_document,
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
