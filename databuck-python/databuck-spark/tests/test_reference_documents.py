import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

os.environ.setdefault("DATABUCK_SDK_AUTO_DOWNLOAD", "0")

from databuck.agentic_rules import (
    _build_audit_prompt,
    _build_cross_column_prompt,
    _build_general_prompt,
    _build_prompt,
    _get_reference_paths,
    _generate_and_audit_with_gemini,
    _parse_rules,
    _referenced_schema_columns,
    _read_docx_text,
    _sample_rows,
    discover_rules,
    has_rule_reference_context,
)


class ReferenceDocumentTests(unittest.TestCase):
    def test_docx_paragraphs_and_table_cells_reach_generation_and_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.docx"
            document_xml = (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/'
                'wordprocessingml/2006/main"><w:body>'
                '<w:p><w:r><w:t>Subscriber ID is required.</w:t></w:r></w:p>'
                '<w:tbl><w:tr><w:tc><w:p><w:r><w:t>Account ID is required.'
                '</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'
                '</w:body></w:document>'
            )
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("word/document.xml", document_xml)

            self.assertEqual(_get_reference_paths({"pdf_paths": [str(path)]}), [str(path)])
            contents = _read_docx_text(str(path))
            self.assertEqual(contents, "Subscriber ID is required.\nAccount ID is required.")
            df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
            prompt = _build_prompt(
                df, {"pdf_paths": [str(path)]}, [str(path)],
                [{"name": path.name, "text": contents}],
            )
            self.assertIn("Account ID is required.", prompt)
            self.assertIn("policy.docx", prompt)
            self.assertIn('Allowed source_document values (copy one exactly for each rule):\n["policy.docx"]', prompt)
            self.assertIn("Subscriber ID is required.", _build_audit_prompt(prompt, []))
            self.assertIn("named source_document", _build_audit_prompt(prompt, []))

    def test_invalid_docx_has_clear_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.docx"
            path.write_text("not a Word document", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Could not read Word document"):
                _read_docx_text(str(path))

    def test_buckgpt_prompt_includes_domain_without_treating_it_as_policy(self):
        df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
        prompt = _build_prompt(
            df,
            {"domain_name": "banking", "business_context": "Balances must reconcile"},
            [], [],
        )
        self.assertIn("Business domain:\nbanking", prompt)
        self.assertIn("domain name alone is not a policy statement", prompt)
        self.assertIn("Balances must reconcile", prompt)
        self.assertIn("Business domain:\nbanking", _build_audit_prompt(prompt, []))

    def test_buckgpt_prompts_exclude_system_dates_but_allow_business_dates(self):
        df = Mock()
        df.schema.fields = [SimpleNamespace(
            name=name, dataType=SimpleNamespace(simpleString=lambda: "timestamp"),
            nullable=True,
        ) for name in ("created_at", "updated_at", "issue_d", "last_pymnt_d")]
        context = {"domain_name": "banking"}
        policy = _build_prompt(df, context, [], [], sample_rows=[])
        general = _build_general_prompt(df, context, [])
        cross = _build_cross_column_prompt(
            df, context, [], [], [], {"BuckGPT_Intel"},
        )
        audit = _build_audit_prompt(policy + general + cross, [])
        for prompt in (policy, general, cross, audit):
            self.assertIn("created_at", prompt)
            self.assertIn("updated_at", prompt)
            self.assertIn("ingested_at", prompt)
            self.assertIn("loan issue and payment dates remain eligible", prompt)
        self.assertIn("Reject any rule whose SQL uses a system-managed", audit)

    def test_policy_uses_ten_rows_and_general_uses_fifty(self):
        df = Mock()
        df.schema.fields = [SimpleNamespace(
            name="balance", dataType=SimpleNamespace(simpleString=lambda: "double"),
            nullable=True,
        )]
        df.limit.return_value.collect.return_value = [
            SimpleNamespace(asDict=lambda recursive, number=number: {"balance": number})
            for number in range(51)
        ]
        rows = _sample_rows(df, limit=50)
        df.limit.assert_called_once_with(50)
        self.assertEqual(len(rows), 50)
        self.assertEqual(rows[-1], {"balance": 49})
        context = {"domain_name": "banking"}
        policy = _build_prompt(df, context, [], [], sample_rows=rows[:10])
        general = _build_general_prompt(df, context, rows)
        cross_column = _build_cross_column_prompt(
            df, context, rows, [], [], {"BuckGPT_Intel"},
        )
        self.assertIn('"columns": ["balance"]', policy)
        self.assertIn('"rows": [[0], [1]', policy)
        self.assertIn('[9]]}', policy)
        self.assertIn('[49]]}', general)
        self.assertNotIn('[10]', policy)
        self.assertNotIn('[50]', general)
        for prompt in (policy, general, cross_column):
            self.assertNotIn("maximum selected-rule count", prompt)
            self.assertNotIn("1.25 times the column count", prompt)
        self.assertIn("60\nor more distinct rules", general)
        self.assertIn("involving 2 to 4 columns", general)
        self.assertIn("non-positive checks where positive values are semantically impossible", general)
        self.assertIn("do not assume all amounts, counts, balances, or adjustments", general)
        self.assertIn("BuckGPT_Intel", general)
        self.assertIn("relationships\nbetween 2 to 5 DataFrame columns", cross_column)
        self.assertIn('[49]]}', cross_column)
        self.assertIn("Never invent numeric bounds, tolerances", cross_column)
        self.assertNotIn("BuckGPT_Intel", policy)

    def test_discover_sends_sample_to_both_generation_phases(self):
        df = Mock()
        df.schema.fields = [SimpleNamespace(
            name="balance", dataType=SimpleNamespace(simpleString=lambda: "double"),
            nullable=True,
        )]
        df.limit.return_value.collect.return_value = [
            SimpleNamespace(asDict=lambda recursive, number=number: {"balance": number})
            for number in range(51)
        ]
        with patch("databuck.agentic_rules._generate_and_audit_with_gemini",
                   return_value=("[]", "[]")) as generate, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(discover_rules(df, {
                "gemini_api_key": "test-key", "domain_name": "banking",
            }), [])
        kwargs = generate.call_args.kwargs
        self.assertIn('[9]]}', kwargs["prompt"])
        self.assertNotIn('[10]', kwargs["prompt"])
        self.assertIn('[49]]}', kwargs["general_prompt"])
        self.assertIn('[49]]}', kwargs["cross_column_prompt"])
        self.assertNotIn('[50]', kwargs["general_prompt"])
        df.limit.assert_called_once_with(50)

    def test_cross_column_prompt_includes_policy_sources_without_api_key(self):
        df = Mock()
        df.schema.fields = [SimpleNamespace(
            name="funded_amnt_inv",
            dataType=SimpleNamespace(simpleString=lambda: "double"),
            nullable=True,
        )]
        prompt = _build_cross_column_prompt(
            df,
            {"domain_name": "banking", "gemini_api_key": "secret-key",
             "business_context": "Investor funding cannot exceed total funding."},
            [{"funded_amnt_inv": 10}], ["/some/policy.pdf"],
            [{"name": "terms.docx", "text": "Funding must reconcile."}],
            {"policy.pdf", "terms.docx", "business_context", "BuckGPT_Intel"},
        )
        self.assertIn("policy.pdf", prompt)
        self.assertIn("Funding must reconcile.", prompt)
        self.assertIn("Investor funding cannot exceed total funding.", prompt)
        self.assertIn("BuckGPT_Intel", prompt)
        self.assertNotIn("secret-key", prompt)

    def test_policy_and_general_candidates_share_audit(self):
        policy_rule = {
            "name": "Required balance", "description": "Balance is required.",
            "source_document": "policy.pdf", "reference_context": "Balance is required.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance IS NULL",
        }
        general_rule = {
            "name": "Nonnegative balance", "description": "Balance cannot be negative.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Negative balances are invalid for this data.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance < 0",
        }
        client = Mock()
        uploaded = SimpleNamespace(name="uploaded-policy")
        client.files.upload.return_value = uploaded
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps([policy_rule])),
            SimpleNamespace(text=json.dumps([general_rule])),
            SimpleNamespace(text=json.dumps([
                {"rule_index": i, "audit_status": "PASSED", "audit_reason": "Valid."}
                for i in (1, 2)
            ])),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, _ = _generate_and_audit_with_gemini(
                "test-key", "test-model", "policy prompt", ["/some/policy.pdf"],
                {"policy.pdf", "BuckGPT_Intel"},
                general_prompt="general prompt",
            )
        self.assertEqual(json.loads(response_text), [policy_rule, general_rule])
        calls = client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0].kwargs["contents"][1:], [uploaded])
        self.assertEqual(len(calls[1].kwargs["contents"]), 1)
        self.assertIn('"source_document": "policy.pdf"',
                      calls[2].kwargs["contents"][0])
        self.assertIn('"source_document": "BuckGPT_Intel"',
                      calls[2].kwargs["contents"][0])

    def test_dedicated_cross_column_call_uses_documents_and_shares_audit(self):
        policy_rule = {
            "name": "Required balance", "description": "Balance is required.",
            "source_document": "policy.pdf", "reference_context": "Balance is required.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance IS NULL",
        }
        general_rule = {
            "name": "Nonnegative balance", "description": "Balance cannot be negative.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Negative balances are invalid.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance < 0",
        }
        cross_rule = {
            "name": "Investor funding cap",
            "description": "Investor funding cannot exceed total funding.",
            "source_document": "policy.pdf",
            "reference_context": "Investor funding cannot exceed total funding.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt_inv > funded_amnt",
        }
        single_column_rule = {
            "name": "Positive funded amount",
            "description": "Funding must be positive.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Funding must be positive.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt <= 0",
        }
        client = Mock()
        uploaded = SimpleNamespace(name="uploaded-policy")
        client.files.upload.return_value = uploaded
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps([policy_rule])),
            SimpleNamespace(text=json.dumps([general_rule])),
            SimpleNamespace(text=json.dumps([
                policy_rule, cross_rule, single_column_rule,
            ])),
            SimpleNamespace(text=json.dumps([
                {"rule_index": i, "audit_status": "PASSED", "audit_reason": "Valid."}
                for i in (1, 2, 3)
            ])),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, _ = _generate_and_audit_with_gemini(
                "test-key", "test-model", "policy prompt", ["/some/policy.pdf"],
                {"policy.pdf", "BuckGPT_Intel"}, general_prompt="general prompt",
                cross_column_prompt="cross-column prompt",
                schema_columns=["balance", "funded_amnt", "funded_amnt_inv"],
            )
        self.assertEqual(json.loads(response_text),
                         [policy_rule, general_rule, cross_rule])
        calls = client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertIn("cross-column prompt", calls[2].kwargs["contents"][0])
        self.assertIn(general_rule["name"], calls[2].kwargs["contents"][0])
        self.assertEqual(calls[2].kwargs["contents"][1:], [uploaded])
        self.assertIn(cross_rule["name"], calls[3].kwargs["contents"][0])
        client.files.delete.assert_called_once_with(name="uploaded-policy")

    def test_cross_column_detection_ignores_string_literals(self):
        columns = _referenced_schema_columns(
            "SELECT * FROM {{DATAFRAME}} WHERE status = 'funded_amnt' "
            "AND funded_amnt_inv > 0",
            ["status", "funded_amnt", "funded_amnt_inv"],
        )
        self.assertEqual(columns, {"status", "funded_amnt_inv"})

    def test_general_uniqueness_uses_dataset_query_and_schema_columns(self):
        uniqueness = {
            "name": "Unique loan ID", "description": "One row per loan.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Loan ID identifies a loan record.",
            "check_type": "uniqueness", "columns": ["loan_id"], "sql": "",
        }
        parsed = _parse_rules(json.dumps([uniqueness]), {"BuckGPT_Intel"},
                              schema_columns=["loan_id", "amount"])
        self.assertEqual(parsed[0]["sql"],
                         "SELECT `loan_id`, COUNT(*) AS duplicate_count FROM "
                         "{{DATAFRAME}} WHERE `loan_id` IS NOT NULL GROUP BY "
                         "`loan_id` HAVING COUNT(*) > 1")
        self.assertEqual(_parse_rules(json.dumps(parsed), {"BuckGPT_Intel"},
                                      schema_columns=["loan_id", "amount"]), parsed)
        invented = dict(uniqueness, columns=["missing_id"])
        self.assertEqual(_parse_rules(json.dumps([invented]), {"BuckGPT_Intel"},
                                      schema_columns=["loan_id", "amount"]), [])
        policy = dict(uniqueness, source_document="policy.pdf")
        self.assertEqual(_parse_rules(json.dumps([policy]),
                                      {"policy.pdf", "BuckGPT_Intel"},
                                      schema_columns=["loan_id"]), [])
        self.assertIn("Do not reject the rule\n  merely because sql is now nonempty",
                      _build_audit_prompt("policy and general prompts", parsed))

    def test_general_null_checks_are_rejected_without_affecting_policy_rules(self):
        general_null = {
            "name": "Loan amount present", "description": "Loan amount is required.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Loan amount is fundamental.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE loan_amnt IS NULL",
        }
        policy_null = dict(general_null, source_document="policy.pdf",
                           reference_context="Loan amount is required.")
        conditional = dict(general_null, name="Conditional term",
                           sql="SELECT * FROM {{DATAFRAME}} WHERE "
                               "funded_amnt > 0 AND term IS NULL")
        reasons = []
        parsed = _parse_rules(json.dumps([
            general_null,
            dict(general_null, sql="SELECT * FROM {{DATAFRAME}} WHERE "
                                   "(`funded_amnt` IS NULL OR term IS NULL)"),
            policy_null, conditional,
        ]), {"BuckGPT_Intel", "policy.pdf"}, reasons)
        self.assertEqual(parsed, [policy_null, conditional])
        self.assertIn("BuckGPT_Intel standalone null checks are not allowed", reasons)

    def test_discovered_rule_prints_its_document_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "banking_policy.pdf"
            path.write_bytes(b"%PDF-1.4")
            rule = {
                "name": "Funded amount limit",
                "description": "Investor funding cannot exceed total funding.",
                "source_document": path.name,
                "reference_context": "Investor funding cannot exceed total funding.",
                "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt_inv > funded_amnt",
            }
            audit = [{"rule_index": 1, "audit_status": "PASSED",
                      "audit_reason": "Quote verified in named document."}]
            df = SimpleNamespace(schema=SimpleNamespace(fields=[
                SimpleNamespace(name=name,
                                dataType=SimpleNamespace(simpleString=lambda: "double"),
                                nullable=True)
                for name in ("funded_amnt_inv", "funded_amnt")
            ]))
            output = io.StringIO()
            with patch("databuck.agentic_rules._sample_rows", return_value=[]), \
                 patch("databuck.agentic_rules._generate_and_audit_with_gemini",
                       return_value=(json.dumps([rule]), json.dumps(audit))) as generate:
                with contextlib.redirect_stdout(output):
                    rules = discover_rules(df, {
                        "gemini_api_key": "test-key", "pdf_paths": [str(path)],
                    })
            self.assertEqual(rules[0]["source_document"], path.name)
            self.assertIn("Source document: banking_policy.pdf", output.getvalue())
            self.assertEqual(generate.call_args.kwargs["allowed_sources"],
                             {path.name, "BuckGPT_Intel"})
            self.assertNotIn("maximum selected-rule count",
                             generate.call_args.kwargs["prompt"])
            self.assertNotIn("rule_count", generate.call_args.kwargs)

    def test_policy_prompt_excludes_general_source(self):
        df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
        prompt = _build_prompt(df, {"domain_name": "banking"}, [], [])
        self.assertIn("Do not infer\nrules from the schema or domain alone", prompt)
        self.assertIn('Allowed source_document values (copy one exactly for each rule):\n[]', prompt)
        rule = {
            "name": "Funding consistency", "description": "Funding reconciles.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "Funding components reconcile.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt <> funded_amnt_inv + other_funding",
        }
        self.assertEqual(_parse_rules(json.dumps([rule]), {"policy.pdf"}), [])
        self.assertTrue(has_rule_reference_context({"domain_name": "banking"}))

    def test_rule_with_unknown_document_source_is_rejected(self):
        rule = {
            "name": "Funded amount limit",
            "description": "Funding check",
            "source_document": "invented.pdf",
            "reference_context": "Investor funding cannot exceed total funding.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt_inv > funded_amnt",
        }
        self.assertEqual(_parse_rules(json.dumps([rule]), {"banking_policy.pdf"}), [])

    def test_generation_retries_when_source_document_is_invalid(self):
        rule = {
            "name": "Required account",
            "description": "Account is required.",
            "source_document": "policy.pdf",
            "reference_context": "Account is required.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE account_id IS NULL",
        }
        invalid = dict(rule, source_document="/Volumes/policy.pdf")
        audit = [{"rule_index": 1, "audit_status": "PASSED",
                  "audit_reason": "Source and SQL match."}]
        client = Mock()
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps([invalid])),
            SimpleNamespace(text=json.dumps([rule])),
            SimpleNamespace(text=json.dumps(audit)),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, audit_text = _generate_and_audit_with_gemini(
                "test-key", "test-model", "prompt", [], {"policy.pdf"},
            )
        self.assertEqual(json.loads(response_text), [rule])
        self.assertEqual(json.loads(audit_text), audit)
        self.assertEqual(client.models.generate_content.call_count, 3)
        retry_prompt = client.models.generate_content.call_args_list[1].kwargs["contents"][0]
        self.assertIn("source_document is not an allowed filename", retry_prompt)
        self.assertIn('["policy.pdf"]', retry_prompt)

    def test_document_rules_are_generated_and_audited_without_general_call(self):
        document_rule = {
            "name": "Required account", "description": "Account is required.",
            "source_document": "policy.pdf", "reference_context": "Account is required.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE account_id IS NULL",
        }
        audit = [{"rule_index": 1, "audit_status": "PASSED", "audit_reason": "Valid."}]
        client = Mock()
        uploaded_file = SimpleNamespace(name="uploaded-policy")
        client.files.upload.return_value = uploaded_file
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps([document_rule])),
            SimpleNamespace(text=json.dumps(audit)),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, _ = _generate_and_audit_with_gemini(
                "test-key", "test-model", "prompt", ["/some/policy.pdf"],
                {"policy.pdf"},
            )
        self.assertEqual(json.loads(response_text), [document_rule])
        calls = client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIn("Document-backed phase", calls[0].kwargs["contents"][0])
        self.assertIn('"source_document": "policy.pdf"', calls[1].kwargs["contents"][0])
        self.assertEqual(calls[0].kwargs["contents"][1:], [uploaded_file])
        self.assertEqual(calls[1].kwargs["contents"][1:], [uploaded_file])
        client.files.delete.assert_called_once_with(name="uploaded-policy")

    def test_all_pdfs_share_one_policy_generation_call(self):
        policy_rules = [
            {
                "name": "Rule " + source,
                "description": "A supported control.",
                "source_document": source,
                "reference_context": "The supporting policy statement.",
                "sql": "SELECT * FROM {{DATAFRAME}} WHERE " + column + " IS NULL",
            }
            for source, column in (("a.pdf", "account_id"), ("b.pdf", "member_id"))
        ]
        client = Mock()
        uploaded = [SimpleNamespace(name="uploaded-a"), SimpleNamespace(name="uploaded-b")]
        client.files.upload.side_effect = uploaded
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps(policy_rules)),
            SimpleNamespace(text=json.dumps([
                {"rule_index": index, "audit_status": "PASSED",
                 "audit_reason": "Valid."} for index in (1, 2)
            ])),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, _ = _generate_and_audit_with_gemini(
                "test-key", "test-model", "prompt", ["/some/a.pdf", "/some/b.pdf"],
                {"a.pdf", "b.pdf"},
            )
        self.assertEqual(json.loads(response_text), policy_rules)
        calls = client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].kwargs["contents"][1:], uploaded)
        self.assertEqual(calls[1].kwargs["contents"][1:], uploaded)

    def test_empty_document_phase_returns_no_rules(self):
        client = Mock()
        client.models.generate_content.side_effect = [
            SimpleNamespace(text="[]"),
            SimpleNamespace(text="[]"),
            SimpleNamespace(text="[]"),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            with contextlib.redirect_stdout(io.StringIO()):
                response_text, _ = _generate_and_audit_with_gemini(
                    "test-key", "test-model", "prompt", [],
                    {"policy.docx"},
                )
        self.assertEqual(json.loads(response_text), [])
        self.assertEqual(client.models.generate_content.call_count, 3)
        self.assertIn("review each reference again",
                      client.models.generate_content.call_args_list[1]
                      .kwargs["contents"][0])
        self.assertIn("Focused document review",
                      client.models.generate_content.call_args_list[2]
                      .kwargs["contents"][0])

    def test_focused_document_recovery_can_restore_policy_rules(self):
        document_rule = {
            "name": "Required account", "description": "Account is required.",
            "source_document": "policy.pdf",
            "reference_context": "Account is required.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE account_id IS NULL",
        }
        audit = [{"rule_index": 1, "audit_status": "PASSED",
                  "audit_reason": "Quoted policy requires account."}]
        client = Mock()
        uploaded = SimpleNamespace(name="uploaded-policy")
        client.files.upload.return_value = uploaded
        client.models.generate_content.side_effect = [
            SimpleNamespace(text="[]"),
            SimpleNamespace(text="[]"),
            SimpleNamespace(text=json.dumps([document_rule])),
            SimpleNamespace(text=json.dumps(audit)),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, audit_text = _generate_and_audit_with_gemini(
                "test-key", "test-model", "policy prompt with 10 sample rows",
                ["/some/policy.pdf"], {"policy.pdf"},
            )
        self.assertEqual(json.loads(response_text), [document_rule])
        self.assertEqual(json.loads(audit_text), audit)
        self.assertEqual(client.models.generate_content.call_count, 4)
        self.assertEqual(client.models.generate_content.call_args_list[2]
                         .kwargs["contents"][1:], [uploaded])

    def test_failed_document_audit_retries_with_reason_and_reaudits(self):
        rejected_rule = {
            "name": "Installment positive", "description": "Installment is positive.",
            "source_document": "til.pdf",
            "reference_context": "The APR is a function of the payment schedule.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE installment <= 0",
        }
        revised_rule = {
            "name": "Funding reconciliation", "description": "Funding components reconcile.",
            "source_document": "til.pdf",
            "reference_context": "Total funding equals investor and other funding.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt <> funded_amnt_inv + other_funding",
        }
        first_audit = [
            {"rule_index": 1, "audit_status": "FAILED",
             "audit_reason": "APR quote does not require positive installments."},
        ]
        second_audit = [{"rule_index": 1, "audit_status": "PASSED",
                         "audit_reason": "Valid."}]
        client = Mock()
        uploaded_file = SimpleNamespace(name="uploaded-til")
        client.files.upload.return_value = uploaded_file
        client.models.generate_content.side_effect = [
            SimpleNamespace(text=json.dumps([rejected_rule])),
            SimpleNamespace(text=json.dumps(first_audit)),
            SimpleNamespace(text=json.dumps([revised_rule])),
            SimpleNamespace(text=json.dumps(second_audit)),
        ]
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        with patch.dict(sys.modules, {"google": google, "google.genai": genai}):
            response_text, audit_text = _generate_and_audit_with_gemini(
                "test-key", "test-model", "prompt", ["/some/til.pdf"],
                {"til.pdf"},
            )
        self.assertEqual(json.loads(response_text), [revised_rule])
        self.assertEqual(json.loads(audit_text), second_audit)
        calls = client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertIn("APR quote does not require positive installments.",
                      calls[2].kwargs["contents"][0])
        self.assertEqual(calls[2].kwargs["contents"][1:], [uploaded_file])

if __name__ == "__main__":
    unittest.main()
