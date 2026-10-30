import csv
import json
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

os.environ.setdefault("DATABUCK_SDK_AUTO_DOWNLOAD", "0")

from databuck import DataBuck
from databuck.lake_rules import (
    DiscoveredRules, _context_rule_expectation, _value_anomaly_specs,
)


class CombinedExportTests(unittest.TestCase):
    def test_buckgpt_uniqueness_is_exported_as_dataset_check(self):
        from databuck.agentic_rules import _uniqueness_sql

        check = {
            "name": "Unique loan ID", "description": "One row per loan.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Loan ID identifies a loan.",
            "check_type": "uniqueness", "columns": ["loan_id"],
            "sql": _uniqueness_sql(["loan_id"]),
            "audit_status": "PASSED", "audit_reason": "Key is justified.",
        }
        with tempfile.TemporaryDirectory() as directory:
            json_path = Path(directory) / "checks.json"
            yaml_path = Path(directory) / "checks.yaml"
            DiscoveredRules([]).to_lake(str(json_path), context_rules=[check])
            DiscoveredRules([]).to_lake(str(yaml_path), context_rules=[check])
            exported = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(exported["warn"], {})
            self.assertEqual(exported["dataset_checks"][0]["sql"], check["sql"])
            yaml_text = yaml_path.read_text(encoding="utf-8")
            self.assertIn("expectations: {}", yaml_text)
            self.assertIn("dataset_checks:", yaml_text)
            self.assertIn("duplicate_count", yaml_text)

    def test_buckgpt_uniqueness_can_be_selected_as_critical(self):
        from databuck.agentic_rules import _uniqueness_sql

        check = {
            "name": "Unique loan ID", "description": "One row per loan.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Loan ID identifies a loan.",
            "check_type": "uniqueness", "columns": ["loan_id"],
            "sql": _uniqueness_sql(["loan_id"]),
            "audit_status": "PASSED", "audit_reason": "Key is justified.",
        }
        def select_high(expectations, df, context, metadata, priority_output):
            priority_output["BuckGPT_Rule_001"] = "HIGH"
            return {"BuckGPT_Rule_001"}

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checks.json"
            with patch("databuck.lake_rules._select_critical_expectations",
                       side_effect=select_high) as select, \
                 patch("databuck.lake_rules._classify_actions_with_llm") as classify:
                DiscoveredRules([]).to_lake(
                    str(destination), df=Mock(),
                    context={"domain_name": "banking", "gemini_api_key": "test-key"},
                    context_rules=[check],
                )
            self.assertEqual(set(select.call_args.args[0]), {"BuckGPT_Rule_001"})
            classify.assert_not_called()
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8"))
                             ["dataset_checks"][0]["source_document"], "BuckGPT_Intel")

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
            self.assertIn("Auto-discovered DataBuck rules: 1", output.getvalue())
            csv_path = destination.with_name("expectations_all_rules.csv")
            self.assertIn(str(csv_path), output.getvalue())
            with csv_path.open(encoding="utf-8-sig", newline="") as stream:
                csv_rules = list(csv.DictReader(stream))
            self.assertEqual(len(csv_rules), 1)
            self.assertEqual(csv_rules[0]["column_name"], "subscriber_id")
            self.assertEqual(csv_rules[0]["source"], "AutoDiscovery")
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
            with destination.with_name("expectations_all_rules.csv").open(
                encoding="utf-8-sig", newline=""
            ) as stream:
                csv_rules = list(csv.DictReader(stream))
            self.assertEqual([row["source"] for row in csv_rules],
                             ["AutoDiscovery", "BuckGPT"])
            self.assertEqual(csv_rules[1]["sql"], buckgpt_rules[0]["sql"])
            self.assertEqual(json.loads(destination.read_text(encoding="utf-8")), {
                "warn": {},
                "drop": {"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"},
                "fail": {"BuckGPT_Rule_001": "NOT COALESCE((account_id IS NULL), FALSE)"},
            })

    def test_domain_selection_considers_auto_and_buckgpt_rules(self):
        profiling_rules = DiscoveredRules([
            {"ruleType": "Null Check", "columnName": "technical_id",
             "parameters": {"nullThreshold": 0}},
            {"ruleType": "Null Check", "columnName": "account_id",
             "parameters": {"nullThreshold": 0}},
        ])
        buckgpt_rules = [{
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance < 0",
            "audit_status": "PASSED",
            "source_document": "banking_policy.pdf",
            "reference_context": "Balances must not be negative.",
        }]
        def select_high(expectations, df, context, metadata, priority_output):
            priority_output.update({
                "not_null_account_id": "HIGH",
                "BuckGPT_Rule_001": "HIGH",
            })
            return set(priority_output)

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._identify_duplicate_expectations",
                       return_value=set()) as deduplicate, \
                 patch("databuck.lake_rules._select_critical_expectations",
                       side_effect=select_high) as select, \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value={
                     "not_null_account_id": {"action": "drop", "reason": "Business key"},
                     "BuckGPT_Rule_001": {"action": "fail", "reason": "Policy control"},
                 }) as classify:
                DataBuck.discover(object(), str(destination), context={
                    "gemini_api_key": "test-key", "domain_name": "banking",
                    "pdf_paths": [], "business_context": "Account records must be linkable",
                })
            self.assertEqual(set(select.call_args.args[0]),
                             {"not_null_technical_id", "not_null_account_id",
                              "BuckGPT_Rule_001"})
            self.assertEqual(set(deduplicate.call_args.args[0]),
                             set(select.call_args.args[0]))
            self.assertEqual({item["column_name"] for item in select.call_args.args[3]
                              if "column_name" in item},
                             {"technical_id", "account_id"})
            self.assertIn("banking_policy.pdf", str(select.call_args.args[3]))
            self.assertEqual(set(classify.call_args.args[0]),
                             {"not_null_account_id", "BuckGPT_Rule_001"})
            exported = json.loads(destination.read_text(encoding="utf-8"))
            self.assertNotIn("not_null_technical_id", str(exported))
            self.assertIn("not_null_account_id", exported["drop"])
            self.assertIn("BuckGPT_Rule_001", exported["fail"])
            with destination.with_name("expectations_all_rules.csv").open(
                encoding="utf-8-sig", newline=""
            ) as stream:
                csv_rules = list(csv.DictReader(stream))
            self.assertEqual([row["column_name"] for row in csv_rules[:2]],
                             ["technical_id", "account_id"])
            self.assertEqual(csv_rules[2]["source_document"], "banking_policy.pdf")

    def test_duplicate_review_runs_before_critical_selection_and_logs_counts(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check", "columnName": "account_id",
            "parameters": {"nullThreshold": 0},
        }])
        buckgpt_rules = [{
            "name": "Required account ID",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE account_id IS NULL",
            "audit_status": "PASSED", "source_document": "policy.pdf",
            "reference_context": "Account ID is required.",
        }]
        def select_high(expectations, df, context, metadata, priority_output):
            priority_output["BuckGPT_Rule_001"] = "HIGH"
            return {"BuckGPT_Rule_001"}

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.json"
            output = io.StringIO()
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._identify_duplicate_expectations",
                       return_value={"not_null_account_id"}) as deduplicate, \
                 patch("databuck.lake_rules._select_critical_expectations",
                       side_effect=select_high) as select, \
                 patch("databuck.lake_rules._classify_actions_with_llm", return_value={
                     "BuckGPT_Rule_001": {"action": "fail", "reason": "Policy control"},
                 }), redirect_stdout(output):
                DataBuck.discover(object(), str(destination), context={
                    "gemini_api_key": "test-key", "domain_name": "banking",
                    "business_context": "Account ID is required",
                })
            self.assertEqual(set(deduplicate.call_args.args[0]),
                             {"not_null_account_id", "BuckGPT_Rule_001"})
            self.assertEqual(set(select.call_args.args[0]), {"BuckGPT_Rule_001"})
            self.assertIn("reviewing duplicate rules (1 auto-discovered, 1 BuckGPT)",
                          output.getvalue())
            self.assertIn("removed 1 duplicate rules; 1 remain", output.getvalue())
            self.assertEqual(set(json.loads(destination.read_text())["fail"]),
                             {"BuckGPT_Rule_001"})
            with destination.with_name("expectations_all_rules.csv").open(
                encoding="utf-8-sig", newline=""
            ) as stream:
                self.assertEqual(len(list(csv.DictReader(stream))), 2)

    def test_final_csv_and_export_contain_high_and_medium_rules(self):
        profiling_rules = DiscoveredRules([
            {"ruleType": "Null Check", "columnName": "account_id",
             "parameters": {"nullThreshold": 0}},
            {"ruleType": "Null Check", "columnName": "technical_id",
             "parameters": {"nullThreshold": 0}},
            {"ruleType": "Null Check", "columnName": "low_value",
             "parameters": {"nullThreshold": 0}},
        ])
        buckgpt_rules = [{
            "name": "Funding cap", "description": "Investor funding is capped.",
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE funded_amnt_inv > funded_amnt",
            "audit_status": "PASSED", "source_document": "policy.pdf",
            "reference_context": "Investor funding cannot exceed total funding.",
        }]

        def select(expectations, df, context, metadata, priority_output):
            priority_output.update({
                "not_null_account_id": "HIGH",
                "not_null_technical_id": "MEDIUM",
                "not_null_low_value": "LOW",
                "BuckGPT_Rule_001": "HIGH",
            })
            return set(priority_output)

        with tempfile.TemporaryDirectory() as directory:
            for suffix in ("json", "yaml"):
                destination = Path(directory) / ("expectations." + suffix)
                with patch("databuck.lake_rules._identify_duplicate_expectations",
                           return_value=set()), \
                     patch("databuck.lake_rules._select_critical_expectations",
                           side_effect=select), \
                     patch("databuck.lake_rules._classify_actions_with_llm", return_value={
                         "not_null_account_id": {"action": "drop", "reason": "Key"},
                         "not_null_technical_id": {"action": "warn", "reason": "Technical"},
                         "BuckGPT_Rule_001": {"action": "fail", "reason": "Policy"},
                     }) as classify:
                    profiling_rules.to_lake(
                        str(destination), df=Mock(),
                        context={"domain_name": "banking", "gemini_api_key": "test-key"},
                        context_rules=buckgpt_rules,
                    )
                if suffix == "json":
                    self.assertEqual(set(classify.call_args.args[0]),
                                     {"not_null_account_id", "not_null_technical_id",
                                      "BuckGPT_Rule_001"})
                    exported = json.loads(destination.read_text(encoding="utf-8"))
                    exported_names = set().union(*(set(exported[action])
                                                   for action in ("warn", "drop", "fail")))
                else:
                    classify.assert_not_called()
                    exported_text = destination.read_text(encoding="utf-8")
                    exported_names = {"not_null_account_id", "not_null_technical_id",
                                      "BuckGPT_Rule_001"}
                    for name in exported_names:
                        self.assertIn(name, exported_text)
                    self.assertNotIn("not_null_low_value", exported_text)
                self.assertEqual(exported_names,
                                 {"not_null_account_id", "not_null_technical_id",
                                  "BuckGPT_Rule_001"})
                with destination.with_name("expectations_high_medium_rules.csv").open(
                    encoding="utf-8-sig", newline=""
                ) as stream:
                    selected_rules = list(csv.DictReader(stream))
                self.assertEqual({row["rule_name"] for row in selected_rules},
                                 exported_names)
                self.assertEqual({row["priority"] for row in selected_rules},
                                 {"HIGH", "MEDIUM"})
                self.assertEqual({row["source"] for row in selected_rules},
                                 {"AutoDiscovery", "BuckGPT"})
                policy = next(row for row in selected_rules
                              if row["rule_name"] == "BuckGPT_Rule_001")
                self.assertEqual(policy["source_document"], "policy.pdf")
                self.assertIn("funded_amnt_inv > funded_amnt", policy["sql"])
                self.assertEqual(policy["action"], "fail" if suffix == "json" else "")

    def test_no_high_or_medium_rules_writes_empty_export_and_csv(self):
        rules = DiscoveredRules([{
            "ruleType": "Null Check", "columnName": "technical_id",
            "parameters": {"nullThreshold": 0},
        }])

        def select_low(expectations, df, context, metadata, priority_output):
            priority_output["not_null_technical_id"] = "LOW"
            return {"not_null_technical_id"}

        with tempfile.TemporaryDirectory() as directory:
            for suffix in ("json", "yaml"):
                destination = Path(directory) / ("checks." + suffix)
                with patch("databuck.lake_rules._select_critical_expectations",
                           side_effect=select_low), \
                     patch("databuck.lake_rules._classify_actions_with_llm") as classify:
                    rules.to_lake(
                        str(destination), df=Mock(),
                        context={"domain_name": "banking", "gemini_api_key": "test-key"},
                    )
                classify.assert_not_called()
                if suffix == "json":
                    self.assertEqual(json.loads(destination.read_text()),
                                     {"warn": {}, "drop": {}, "fail": {}})
                else:
                    self.assertIn("expectations: {}", destination.read_text())
                with destination.with_name("checks_high_medium_rules.csv").open(
                    encoding="utf-8-sig", newline=""
                ) as stream:
                    self.assertEqual(list(csv.DictReader(stream)), [])

    def test_selected_csv_includes_dataset_uniqueness_check(self):
        from databuck.agentic_rules import _uniqueness_sql

        check = {
            "name": "Unique loan ID", "description": "One row per loan.",
            "source_document": "BuckGPT_Intel",
            "reference_context": "BuckGPT_Intel: Loan ID identifies a loan.",
            "check_type": "uniqueness", "columns": ["loan_id"],
            "sql": _uniqueness_sql(["loan_id"]),
            "audit_status": "PASSED", "audit_reason": "Key is justified.",
        }

        def select(expectations, df, context, metadata, priority_output):
            priority_output["BuckGPT_Rule_001"] = "HIGH"
            return {"BuckGPT_Rule_001"}

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checks.json"
            with patch("databuck.lake_rules._select_critical_expectations",
                       side_effect=select):
                DiscoveredRules([]).to_lake(
                    str(destination), df=Mock(),
                    context={"domain_name": "banking", "gemini_api_key": "test-key"},
                    context_rules=[check],
                )
            with destination.with_name("checks_high_medium_rules.csv").open(
                encoding="utf-8-sig", newline=""
            ) as stream:
                high_rules = list(csv.DictReader(stream))
            self.assertEqual(len(high_rules), 1)
            self.assertEqual(high_rules[0]["rule_type"], "uniqueness")
            self.assertEqual(high_rules[0]["sql"], check["sql"])

    def test_domain_selection_can_leave_only_buckgpt_rules_in_yaml(self):
        profiling_rules = DiscoveredRules([{
            "ruleType": "Null Check", "columnName": "technical_id",
            "parameters": {"nullThreshold": 0},
        }])
        buckgpt_rules = [{
            "sql": "SELECT * FROM {{DATAFRAME}} WHERE balance < 0",
            "audit_status": "PASSED",
        }]
        def select_high(expectations, df, context, metadata, priority_output):
            priority_output["BuckGPT_Rule_001"] = "HIGH"
            return {"BuckGPT_Rule_001"}

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "expectations.yaml"
            with patch.object(DataBuck, "discover_rules", return_value=profiling_rules), \
                 patch.object(DataBuck, "_discover_context_rules", return_value=buckgpt_rules), \
                 patch("databuck.lake_rules._identify_duplicate_expectations",
                       return_value=set()), \
                 patch("databuck.lake_rules._select_critical_expectations",
                       side_effect=select_high) as select, \
                 patch("databuck.lake_rules._classify_actions_with_llm") as classify:
                DataBuck.discover(object(), str(destination), context={
                    "domain_name": "banking", "gemini_api_key": "test-key",
                    "business_context": "Balances must not be negative",
                })
            classify.assert_not_called()
            self.assertEqual(set(select.call_args.args[0]),
                             {"not_null_technical_id", "BuckGPT_Rule_001"})
            output = destination.read_text(encoding="utf-8")
            self.assertNotIn("not_null_technical_id", output)
            self.assertIn("BuckGPT_Rule_001", output)

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
