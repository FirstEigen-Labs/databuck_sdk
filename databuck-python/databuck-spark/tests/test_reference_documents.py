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

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck.agentic_rules import (
    _build_audit_prompt,
    _build_prompt,
    _get_reference_paths,
    _generate_and_audit_with_gemini,
    _parse_rules,
    _read_docx_text,
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
            df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
            output = io.StringIO()
            with patch("databuck.agentic_rules._generate_and_audit_with_gemini",
                       return_value=(json.dumps([rule]), json.dumps(audit))) as generate:
                with contextlib.redirect_stdout(output):
                    rules = discover_rules(df, {
                        "gemini_api_key": "test-key", "pdf_paths": [str(path)],
                    })
            self.assertEqual(rules[0]["source_document"], path.name)
            self.assertIn("Source document: banking_policy.pdf", output.getvalue())
            self.assertEqual(generate.call_args.kwargs["allowed_sources"],
                             {path.name})
            self.assertIn("there is no target count", generate.call_args.kwargs["prompt"])
            self.assertNotIn("rule_count", generate.call_args.kwargs)

    def test_only_document_or_explicit_business_context_sources_are_allowed(self):
        df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
        prompt = _build_prompt(df, {"domain_name": "banking"}, [], [])
        self.assertIn("Do not infer\nrules from the schema or domain alone", prompt)
        self.assertNotIn("BuckGPT_Intel", prompt)
        rule = {
            "name": "Funding consistency", "description": "Funding reconciles.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "Funding components reconcile.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt <> funded_amnt_inv + other_funding",
        }
        self.assertEqual(_parse_rules(json.dumps([rule]), {"policy.pdf"}), [])
        self.assertFalse(has_rule_reference_context({"domain_name": "banking"}))

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
        self.assertEqual(client.models.generate_content.call_count, 2)
        self.assertIn("review each reference again",
                      client.models.generate_content.call_args_list[1]
                      .kwargs["contents"][0])

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
