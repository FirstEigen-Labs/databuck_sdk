import os
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck.agentic_rules import has_rule_reference_context
from databuck.lake_rules import (
    _profile_rule_metadata, _select_critical_profile_expectations,
)


class CriticalSelectionTests(unittest.TestCase):
    def setUp(self):
        self.df = Mock()
        self.df.schema.json.return_value = '{"fields":[]}'
        self.expectations = {
            "not_null_technical_id": "`technical_id` IS NOT NULL",
            "not_null_account_id": "`account_id` IS NOT NULL",
        }
        self.metadata = [{
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

    def test_domain_is_not_a_buckgpt_policy_and_selection_uses_business_impact(self):
        self.assertFalse(has_rule_reference_context({"domain_name": "banking"}))
        self.client.models.generate_content.return_value.text = '["not_null_account_id"]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_profile_expectations(
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
        self.assertNotIn("test-key", prompt)
        self.assertNotIn("DataFrame sample", prompt)
        self.df.limit.assert_not_called()

    def test_selection_rejects_unknown_names(self):
        self.client.models.generate_content.return_value.text = '["invented_rule"]'
        with patch.dict(sys.modules, self.modules):
            with self.assertRaisesRegex(ValueError, "invalid critical profiling rule selection"):
                _select_critical_profile_expectations(
                    self.expectations, self.df,
                    {"domain_name": "banking", "gemini_api_key": "test-key"},
                    self.metadata,
                )

    def test_selection_can_return_no_critical_profile_rules(self):
        self.client.models.generate_content.return_value.text = '[]'
        with patch.dict(sys.modules, self.modules):
            selected = _select_critical_profile_expectations(
                self.expectations, self.df,
                {"domain_name": "banking", "gemini_api_key": "test-key"},
                self.metadata,
            )
        self.assertEqual(selected, set())

    def test_profile_metadata_excludes_sample_values(self):
        metadata = _profile_rule_metadata([{
            "ruleType": "Null Check", "columnName": "account_id",
            "threshold": 0,
            "parameters": {"nullThreshold": 0, "sampleValues": ["customer-123"]},
        }])
        self.assertEqual(metadata, [{
            "rule_type": "Null Check", "column_name": "account_id",
            "threshold": 0, "parameters": {"nullThreshold": 0},
        }])


if __name__ == "__main__":
    unittest.main()
