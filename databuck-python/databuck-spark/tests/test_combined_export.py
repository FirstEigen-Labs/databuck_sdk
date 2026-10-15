import json
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck import DataBuck
from databuck.lake_rules import (
    DiscoveredRules, _context_rule_expectation, _value_anomaly_specs,
)


class CombinedExportTests(unittest.TestCase):
    def test_table_name_loads_dataframe_for_json_discovery(self):
        dataframe = object()
        session = Mock()
        session.read.table.return_value = dataframe
        sql_module = ModuleType("pyspark.sql")
        sql_module.SparkSession = Mock()
        sql_module.SparkSession.getActiveSession.return_value = session
        pyspark_module = ModuleType("pyspark")
        pyspark_module.sql = sql_module
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.dict("sys.modules", {"pyspark": pyspark_module,
                                            "pyspark.sql": sql_module}), \
                 patch.object(DataBuck, "discover_rules", return_value=profiling_rules) as discover:
                DataBuck.discover("catalog.schema.telco", str(destination))
            session.read.table.assert_called_once_with("catalog.schema.telco")
            discover.assert_called_once_with(dataframe)
            self.assertIn("not_null_subscriber_id", json.loads(destination.read_text())['warn'])

    def test_table_name_supplies_yaml_table_field(self):
        dataframe = object()
        session = Mock()
        session.read.table.return_value = dataframe
        sql_module = ModuleType("pyspark.sql")
        sql_module.SparkSession = Mock()
        sql_module.SparkSession.getActiveSession.return_value = session
        pyspark_module = ModuleType("pyspark")
        pyspark_module.sql = sql_module
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.yaml"
            with patch.dict("sys.modules", {"pyspark": pyspark_module,
                                            "pyspark.sql": sql_module}), \
                 patch.object(DataBuck, "discover_rules", return_value=profiling_rules):
                DataBuck.discover("catalog.schema.telco", str(destination))
            self.assertTrue(destination.read_text(encoding="utf-8").startswith(
                "table: catalog.schema.telco\n\nexpectations:\n"
            ))

    def test_blank_table_name_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "table name cannot be empty"):
            DataBuck.discover("  ", "expectations.json")

    def test_without_context_exports_only_profiling_rules_as_warn(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            output = io.StringIO()
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules") as discover_context, \
                 patch("databuck.sdk.time.perf_counter", side_effect=[10.0, 12.5]):
                with redirect_stdout(output):
                    DataBuck.discover(object(), str(destination))
            discover_context.assert_not_called()
            self.assertIn("Auto-discovery time: 2.50 seconds", output.getvalue())
            self.assertIn("Auto-discovered DataBuck rules (1)", output.getvalue())
            self.assertIn("subscriber_id", output.getvalue())
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {
                "warn": {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"},
                "drop": {},
                "fail": {},
            })

    def test_api_key_only_classifies_profiling_rules_without_buckgpt_generation(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules") as discover_context, \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value={
                     "not_null_subscriber_id": {"action": "warn", "reason": "Monitor ID"}
                 }) as classify:
                DataBuck.discover(object(), str(destination),
                                  context={"gemini_api_key": "test-key"})
            discover_context.assert_not_called()
            classify.assert_called_once()
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8"))["warn"],
                             {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"})

    def test_value_anomaly_embeds_discovered_baseline(self):
        rule = {
            "ruleType": "Value Anomaly Check",
            "columnName": "monthly_charge_usd",
            "threshold": 3.0,
            "expression": "",
            "parameters": {"columns": ["`monthly_charge_usd`", "`data_usage_gb`"]},
        }
        profiling_rules = DiscoveredRules([rule])
        source_df = object()
        prepared_df = object()

        def prepare(actual_df):
            self.assertIs(actual_df, source_df)
            profiling_rules._prepared_value_anomaly = _value_anomaly_specs(profiling_rules)
            profiling_rules._value_anomaly_baselines = {
                "monthly_charge_usd": (50.0, 5.0),
                "data_usage_gb": (12.0, 2.0),
            }
            return prepared_df

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(profiling_rules, "prepare_for_lake", side_effect=prepare) as prepare_mock, \
                 patch.object(DataBuck, "_discover_context_rules", return_value=[]), \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value={
                     "valid_value_monthly_charge_usd": {
                         "action": "drop", "reason": "Outlier"
                     },
                     "valid_value_data_usage_gb": {
                         "action": "drop", "reason": "Outlier"
                     },
                 }) as classify:
                DataBuck.discover(source_df, str(destination), context={"gemini_api_key": "test"})

            prepare_mock.assert_called_once_with(source_df)
            self.assertIs(classify.call_args.args[1], prepared_df)
            exported = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(set(exported["drop"]), {
                "valid_value_monthly_charge_usd", "valid_value_data_usage_gb"
            })
            self.assertIn("ABS(CAST(`monthly_charge_usd` AS DOUBLE) - 50) <= 3 * 5",
                          exported["drop"]["valid_value_monthly_charge_usd"])
            self.assertNotIn("preparation", exported)

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
                 patch.object(DataBuck, "_discover_context_rules", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value=decisions):
                result = DataBuck.discover(
                    object(), str(destination), context={"business_context": "test"}
                )
            self.assertEqual(result, str(destination))
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {
                "warn": {},
                "drop": {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"},
                "fail": {"BuckGPT_Rule_001": "NOT COALESCE((account_id IS NULL), FALSE)"},
            })

    def test_yaml_export_combines_auto_and_buckgpt_rules(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        buckgpt_rules = [{
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE status = 'INVALID'",
            "audit_status": "PASSED",
        }]
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.yaml"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._classify_actions_with_llm") as classify:
                result = DataBuck.discover(
                    object(), str(destination), context={"business_context": "test"},
                    table_name="telco_customer_subscription"
                )
            classify.assert_not_called()
            self.assertEqual(result, str(destination))
            self.assertEqual(destination.read_text(encoding="utf-8"),
                             'table: telco_customer_subscription\n\n'
                             'expectations:\n'
                             '  not_null_subscriber_id: "`subscriber_id` IS NOT NULL"\n'
                             '  BuckGPT_Rule_001: "NOT COALESCE((status = \'INVALID\'), FALSE)"\n')

    def test_yml_export_from_dataframe_omits_unknown_table_name(self):
        dataframe = object()
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.yml"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules) as discover:
                DataBuck.discover(dataframe, str(destination))
            discover.assert_called_once_with(dataframe)
            self.assertEqual(destination.read_text(encoding="utf-8"),
                             'expectations:\n'
                             '  not_null_subscriber_id: "`subscriber_id` IS NOT NULL"\n')

    def test_yml_export_supports_qualified_table_name(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check",
            "columnName": "subscriber_id",
            "parameters": {"nullThreshold": 0},
        }])
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.YML"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules):
                DataBuck.discover(
                    object(), str(destination), table_name="catalog.schema.telco"
                )
            self.assertTrue(destination.read_text(encoding="utf-8").startswith(
                "table: catalog.schema.telco\n\nexpectations:\n"
            ))

    def test_legacy_discover_with_context_dictionary_still_returns_rules(self):
        context = {"gemini_api_key": "test-key"}
        expected = [{"name": "Required account"}]
        with patch.object(DataBuck, "_discover_context_rules", return_value=expected) as generate:
            self.assertIs(DataBuck.discover(object(), context), expected)
            self.assertIs(DataBuck.discover(object(), context=context), expected)
        self.assertEqual(generate.call_count, 2)

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
