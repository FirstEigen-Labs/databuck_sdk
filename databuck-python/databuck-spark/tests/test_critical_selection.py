import contextlib
import io
import json
import os
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck.agentic_rules import has_rule_reference_context
from databuck.lake_rules import (
    _CRITICAL_SELECTION_CACHE, _profile_rule_metadata, _select_critical_expectations,
)


class CriticalSelectionTests(unittest.TestCase):
    def setUp(self):
        _CRITICAL_SELECTION_CACHE.clear()
        self.df = Mock()
        self.df.schema.json.return_value = '{"fields":[]}'
        self.expectations = {
            "not_null_technical_id": "`technical_id` IS NOT NULL",
            "not_null_account_id": "`account_id` IS NOT NULL",
        }
        self.metadata = [{
            "expectation_name": "not_null_account_id",
            "rule_type": "Null Check", "column_name": "account_id",
            "threshold": 0, "parameters": {"nullThreshold": 0},
        }]
        self.client = Mock()
        google = ModuleType("google")
        genai = ModuleType("google.genai")
        genai.Client = Mock(return_value=self.client)
        genai.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        google.genai = genai
        self.modules = {"google": google, "google.genai": genai}

    def test_domain_enables_buckgpt_generation_and_critical_selection(self):
        self.assertTrue(has_rule_reference_context({"domain_name": "banking"}))
        self.client.models.generate_content.return_value.text = '[{"id":1,"priority":"HIGH"}]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df, {
                    "domain_name": "banking", "gemini_api_key": "test-key",
                    "business_context": "Account records must be linkable",
                }, self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn("banking subject matter expert", prompt)
        self.assertIn("technical ID is often low value", prompt)
        self.assertIn("Account records must be linkable", prompt)
        self.assertIn('"rule_type": "Null Check"', prompt)
        self.assertIn('"nullThreshold": 0', prompt)
        self.assertIn("smallest defensible set", prompt)
        self.assertIn("strongest, clearest check", prompt)
        self.assertIn("HIGH, MEDIUM, or LOW", prompt)
        self.assertIn("banking data engineer, and a data quality governance lead", prompt)
        self.assertIn("Business correctness", prompt)
        self.assertIn("Production necessity", prompt)
        self.assertIn("Incremental detection", prompt)
        self.assertIn("False-positive risk", prompt)
        self.assertIn("Retain a Critical candidate only when", prompt)
        self.assertIn("Retain a High Value candidate", prompt)
        self.assertIn("confirmation-dependent candidates", prompt)
        self.assertIn("Detection complexity is not", prompt)
        self.assertNotIn("test-key", prompt)
        self.assertNotIn("DataFrame sample", prompt)
        self.df.limit.assert_not_called()

    def test_selection_rejects_unknown_names(self):
        self.client.models.generate_content.return_value.text = '[{"id":3,"priority":"HIGH"}]'
        with patch.dict(sys.modules, self.modules):
            with self.assertRaisesRegex(ValueError, "candidate ID is outside the available range"):
                _select_critical_expectations(
                    self.expectations, self.df,
                    {"domain_name": "banking", "gemini_api_key": "test-key"},
                    self.metadata,
                )
        self.assertEqual(self.client.models.generate_content.call_count, 2)

    def test_selection_prints_priorities_without_changing_rules(self):
        self.client.models.generate_content.return_value.text = (
            '[{"id":1,"priority":"MEDIUM"}]'
        )
        output = io.StringIO()
        with patch.dict(sys.modules, self.modules), contextlib.redirect_stdout(output):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        self.assertIn("DataBuck: critical auto-discovered rule [MEDIUM] not_null_account_id",
                      output.getvalue())
        self.client.models.generate_content.assert_called_once()

    def test_selection_accepts_candidate_ids(self):
        self.client.models.generate_content.return_value.text = '[{"id":1,"priority":"LOW"}]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn('"id": 1', prompt)
        self.assertIn('"name": "not_null_account_id"', prompt)

    def test_selection_prompt_contains_only_auto_discovered_rules(self):
        self.client.models.generate_content.return_value.text = '[{"id":1,"priority":"HIGH"}]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn("Review only the DataBuck auto-discovered candidates", prompt)
        self.assertNotIn("BuckGPT", prompt)

    def test_identical_candidates_reuse_selection_even_when_order_changes(self):
        self.client.models.generate_content.return_value.text = '[{"id":1,"priority":"HIGH"}]'
        context = {"domain_name": "banking", "gemini_api_key": "test-key"}
        output = io.StringIO()
        with patch.dict(sys.modules, self.modules), contextlib.redirect_stdout(output):
            first = _select_critical_expectations(self.expectations, self.df,
                                                  context, self.metadata)
            reordered = dict(reversed(list(self.expectations.items())))
            second = _select_critical_expectations(reordered, self.df,
                                                   context, self.metadata)
        self.assertEqual(first, {"not_null_account_id"})
        self.assertEqual(second, first)
        self.client.models.generate_content.assert_called_once()
        self.assertIn("reusing critical auto-discovered rule selection", output.getvalue())

    def test_changed_business_context_requests_fresh_selection(self):
        self.client.models.generate_content.side_effect = [
            SimpleNamespace(text='[{"id":1,"priority":"HIGH"}]'),
            SimpleNamespace(text='[{"id":2,"priority":"LOW"}]'),
        ]
        with patch.dict(sys.modules, self.modules), contextlib.redirect_stdout(io.StringIO()):
            first = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"}, self.metadata,
            )
            second = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key",
                 "business_context": "New policy requirement"}, self.metadata,
            )
        self.assertEqual(first, {"not_null_account_id"})
        self.assertEqual(second, {"not_null_technical_id"})
        self.assertEqual(self.client.models.generate_content.call_count, 2)

    def test_selection_retries_a_malformed_response_once(self):
        self.client.models.generate_content.side_effect = [
            SimpleNamespace(text='{"unexpected":"shape"}'),
            SimpleNamespace(text='[{"id":1,"priority":"HIGH"}]'),
        ]
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        self.assertEqual(self.client.models.generate_content.call_count, 2)

    def test_selection_retries_missing_priority(self):
        self.client.models.generate_content.side_effect = [
            SimpleNamespace(text='[{"id":1}]'),
            SimpleNamespace(text='[{"id":1,"priority":"HIGH"}]'),
        ]
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        self.assertEqual(self.client.models.generate_content.call_count, 2)

    def test_selection_can_return_no_critical_profile_rules(self):
        self.client.models.generate_content.return_value.text = '[]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, set())

    def test_selection_has_no_fixed_count_limit(self):
        expectations = {"not_null_field_{}".format(index): "`field_{}` IS NOT NULL".format(index)
                        for index in range(107)}
        self.client.models.generate_content.return_value.text = json.dumps([
            {"id": index, "priority": "LOW"} for index in range(1, 108)
        ])
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"}, [],
            )
        self.assertEqual(selected, set(expectations))
        self.client.models.generate_content.assert_called_once()
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn("Do not aim for a fixed count or percentage", prompt)
        self.assertNotIn("BuckGPT_Rule_", prompt)

    def test_profile_metadata_excludes_sample_values(self):
        metadata = _profile_rule_metadata([
            {"ruleType": "Null Check", "columnName": "account_id",
             "threshold": 0,
             "parameters": {"nullThreshold": 0, "sampleValues": ["customer-123"]}},
            {"ruleType": "Null Check", "columnName": "chargeoff_within_12_mths",
             "threshold": 5,
             "parameters": {"nullThreshold": 5}},
        ], self.expectations)
        self.assertEqual(metadata, [{
            "expectation_name": "not_null_account_id",
            "rule_type": "Null Check", "column_name": "account_id",
            "threshold": 0, "parameters": {"nullThreshold": 0},
        }])


if __name__ == "__main__":
    unittest.main()
