import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck import DataBuck
from databuck.lake_rules import DiscoveredRules, _context_rule_expectation


class CombinedExportTests(unittest.TestCase):
    def test_without_context_exports_only_profiling_rules_as_warn(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "discover") as discover_context:
                DataBuck.discover_and_export(object(), str(destination))
            discover_context.assert_not_called()
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {
                "warn": {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"},
                "drop": {},
                "fail": {},
            })

    def test_single_call_exports_both_rule_sources(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        buckgpt_rules = [{
            "name": "Required account",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE account_id IS NULL",
            "audit_status": "PASSED",
        }]
        decisions = {
            "not_null_subscriber_id": {"action": "drop", "reason": "Required ID"},
            "BuckGPT_Rule_001": {"action": "fail", "reason": "Required account"},
        }
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "discover", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value=decisions):
                result = DataBuck.discover_and_export(
                    object(), str(destination), context={"business_context": "test"}
                )
            self.assertEqual(result, str(destination))
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {
                "warn": {},
                "drop": {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"},
                "fail": {"BuckGPT_Rule_001": "NOT COALESCE((account_id IS NULL), FALSE)"},
            })

    def test_context_query_null_is_a_passing_row(self):
        expression = _context_rule_expectation({
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE status = 'INVALID'",
            "audit_status": "PASSED",
        }, "BuckGPT_Rule_001")
        self.assertEqual(expression, "NOT COALESCE((status = 'INVALID'), FALSE)")

    def test_unconvertible_context_rule_fails_before_writing(self):
        rules = DiscoveredRules()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with self.assertRaisesRegex(ValueError, "BuckGPT_Rule_001 must use"):
                rules.to_lake(str(destination), context_rules=[{
                    "sql": "SELECT account_id FROM {{DATAFRAME}} WHERE account_id IS NULL",
                    "audit_status": "PASSED",
                }])
            self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
