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
    _CRITICAL_SELECTION_CACHE, _DUPLICATE_SELECTION_CACHE,
    _classify_actions_with_llm, _parse_rule_action_overrides,
    _identify_duplicate_expectations, _parse_duplicate_selection,
    _profile_rule_metadata, _select_critical_expectations,
)


class CriticalSelectionTests(unittest.TestCase):
    def setUp(self):
        _CRITICAL_SELECTION_CACHE.clear()
        _DUPLICATE_SELECTION_CACHE.clear()
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
        self.assertIn("When pattern and length checks cover the same column", prompt)
        self.assertIn("retain both only if each catches a distinct, important", prompt)
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

    def test_selection_uses_supplied_domain_in_reviewer_roles(self):
        self.client.models.generate_content.return_value.text = "[]"
        with patch.dict(sys.modules, self.modules):
            _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "healthcare", "gemini_api_key": "test-key"},
                self.metadata,
            )
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn("healthcare subject matter expert", prompt)
        self.assertIn("healthcare data engineer", prompt)
        self.assertIn("healthcare semantics", prompt)
        self.assertNotIn("banking", prompt)

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
        self.assertIn("DataBuck: critical rule [MEDIUM] not_null_account_id",
                      output.getvalue())
        self.client.models.generate_content.assert_called_once()

    def test_selection_provides_priorities_for_fresh_and_cached_results(self):
        self.client.models.generate_content.return_value.text = (
            '[{"id":1,"priority":"HIGH"},'
            '{"id":2,"priority":"MEDIUM"}]'
        )
        first_priorities = {}
        cached_priorities = {}
        context = {"domain_name": "banking", "gemini_api_key": "test-key"}
        with patch.dict(sys.modules, self.modules), contextlib.redirect_stdout(io.StringIO()):
            first = _select_critical_expectations(
                self.expectations, self.df, context, self.metadata,
                priority_output=first_priorities,
            )
            cached = _select_critical_expectations(
                self.expectations, self.df, context, self.metadata,
                priority_output=cached_priorities,
            )
        self.assertEqual(first, cached)
        self.assertEqual(first_priorities, {
            "not_null_account_id": "HIGH",
            "not_null_technical_id": "MEDIUM",
        })
        self.assertEqual(cached_priorities, first_priorities)
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

    def test_selection_prompt_accepts_both_rule_sources(self):
        self.client.models.generate_content.return_value.text = '[{"id":1,"priority":"HIGH"}]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, {"not_null_account_id"})
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn("auto-discovered and audited BuckGPT candidates", prompt)

    def test_duplicate_review_compares_auto_and_buckgpt_rules(self):
        expectations = {
            "not_null_account_id": "`account_id` IS NOT NULL",
            "BuckGPT_Rule_001": "NOT COALESCE((account_id IS NULL), FALSE)",
        }
        metadata = self.metadata + [{
            "expectation_name": "BuckGPT_Rule_001",
            "source_document": "policy.pdf",
            "reference_context": "Account ID is required.",
            "check_type": "row",
        }]
        self.client.models.generate_content.return_value.text = (
            '[{"remove_id": 1, "keep_id": 2, "reason": "Same required-ID check"}]'
        )
        with patch.dict(sys.modules, self.modules):
            removed = _identify_duplicate_expectations(
                expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                metadata, {"not_null_account_id"},
            )
        self.assertEqual(removed, {"BuckGPT_Rule_001"})
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn('"source": "AutoDiscovery"', prompt)
        self.assertIn('"source": "BuckGPT"', prompt)
        self.assertIn("policy.pdf", prompt)
        self.assertNotIn("test-key", prompt)
        self.df.limit.assert_not_called()

    def test_duplicate_review_rejects_removed_representative(self):
        with self.assertRaisesRegex(ValueError, "representative cannot also be removed"):
            _parse_duplicate_selection(
                '[{"remove_id": 1, "keep_id": 2, "reason": "same"}, '
                '{"remove_id": 2, "keep_id": 3, "reason": "same"}]',
                ["one", "two", "three"],
            )

    def test_duplicate_review_keeps_dataset_checks_separate(self):
        with self.assertRaisesRegex(ValueError, "dataset and row checks"):
            _parse_duplicate_selection(
                '[{"remove_id": 1, "keep_id": 2, "reason": "same key"}]',
                ["dataset_check", "row_check"], {"dataset_check"},
            )

    def test_action_review_uses_ids_and_defaults_omitted_rules_to_warn(self):
        self.df.limit.return_value.toJSON.return_value.collect.return_value = [
            '{"account_id":"A"}'
        ]
        self.client.models.generate_content.return_value.text = (
            '[{"id":1,"action":"fail","reason":"Critical key"}]'
        )
        with patch.dict(sys.modules, self.modules):
            decisions = _classify_actions_with_llm(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
            )
        self.assertEqual(decisions["not_null_account_id"]["action"], "fail")
        self.assertEqual(decisions["not_null_technical_id"]["action"], "warn")
        prompt = self.client.models.generate_content.call_args.kwargs["contents"]
        self.assertIn('"id": 1', prompt)
        self.assertIn("Every omitted rule defaults to warn", prompt)
        self.assertNotIn("test-key", prompt)

    def test_action_review_retries_invalid_ids(self):
        self.df.limit.return_value.toJSON.return_value.collect.return_value = []
        self.client.models.generate_content.side_effect = [
            SimpleNamespace(text='[{"id":3,"action":"fail","reason":"Wrong"}]'),
            SimpleNamespace(text='[{"id":2,"action":"drop","reason":"Fix row"}]'),
        ]
        with patch.dict(sys.modules, self.modules):
            decisions = _classify_actions_with_llm(
                self.expectations, self.df, {"gemini_api_key": "test-key"},
            )
        self.assertEqual(decisions["not_null_technical_id"]["action"], "drop")
        self.assertEqual(self.client.models.generate_content.call_count, 2)

    def test_action_review_falls_back_to_warn_after_invalid_retry(self):
        self.df.limit.return_value.toJSON.return_value.collect.return_value = []
        self.client.models.generate_content.return_value.text = (
            '[{"id":1,"action":"fail","reason":"A"},'
            '{"id":1,"action":"drop","reason":"B"}]'
        )
        output = io.StringIO()
        with patch.dict(sys.modules, self.modules), contextlib.redirect_stdout(output):
            decisions = _classify_actions_with_llm(
                self.expectations, self.df, {"gemini_api_key": "test-key"},
            )
        self.assertEqual({value["action"] for value in decisions.values()}, {"warn"})
        self.assertIn("action review invalid after one retry", output.getvalue())

    def test_action_review_rejects_duplicate_ids(self):
        with self.assertRaisesRegex(ValueError, "duplicate rule action ID"):
            _parse_rule_action_overrides(
                '[{"id":1,"action":"drop","reason":"A"},'
                '{"id":1,"action":"fail","reason":"B"}]',
                ["rule_one", "rule_two"],
            )

    def test_action_review_accepts_explicit_warn(self):
        self.assertEqual(_parse_rule_action_overrides(
            '[{"id":1,"action":"warn"},'
            '{"id":2,"action":"fail","reason":"Required control"}]',
            ["rule_one", "rule_two"],
        ), {"rule_two": {"action": "fail", "reason": "Required control"}})

    def test_duplicate_review_retries_invalid_ids(self):
        self.client.models.generate_content.side_effect = [
            SimpleNamespace(text='[{"remove_id":3,"keep_id":1,"reason":"same"}]'),
            SimpleNamespace(text='[]'),
        ]
        with patch.dict(sys.modules, self.modules):
            removed = _identify_duplicate_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata, set(self.expectations),
            )
        self.assertEqual(removed, set())
        self.assertEqual(self.client.models.generate_content.call_count, 2)

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
        self.assertIn("reusing critical rule selection", output.getvalue())

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
