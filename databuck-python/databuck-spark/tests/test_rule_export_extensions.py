import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from databuck import DataBuck
from databuck.lake_rules import DiscoveredRules


class RuleExportExtensionTests(unittest.TestCase):
    def make_rules(self):
        return DiscoveredRules([
            {"ruleType": "Duplicate Check PrimaryFields", "columnName": "device_id", "threshold": 0},
            {"ruleType": "Duplicate Check PrimaryFields", "columnName": "event_id", "threshold": 0},
            {"ruleType": "Date Rule", "columnName": "event_date",
             "parameters": {"dateFormat": "yyyy-MM-dd"}},
            {"ruleType": "Value Anomaly Check", "columnName": "amount", "threshold": 3,
             "parameters": {"columns": ["amount"]}},
        ])

    def test_preparation_is_required(self):
        rules = self.make_rules()
        with self.assertRaisesRegex(ValueError, "prepare_for_lake"):
            rules.to_lake("unused.yaml")

    def test_both_formats_export_only_self_contained_rules(self):
        rules = self.make_rules()
        rules._prepared_value_anomaly = (("amount", 3.0),)
        rules._value_anomaly_baselines = {"amount": (15.0, 7.0710678118654755)}
        with tempfile.TemporaryDirectory() as directory:
            json_doc = json.loads(Path(rules.to_lake(Path(directory) / "rules.json")).read_text())
            yaml_text = Path(rules.to_lake(Path(directory) / "rules.yaml",
                                           table_name="catalog.schema.events")).read_text()

        names = {"valid_date_event_date", "valid_value_amount"}
        self.assertEqual(set(json_doc["warn"]), names)
        self.assertIn("TRY_TO_TIMESTAMP", json_doc["warn"]["valid_date_event_date"])
        self.assertIn("ABS(CAST(`amount` AS DOUBLE) - 15) <= 3 * 7.0710678118654755",
                      json_doc["warn"]["valid_value_amount"])
        self.assertNotIn("preparation", json_doc)
        self.assertIn("table: catalog.schema.events", yaml_text)
        self.assertIn("expectations:", yaml_text)
        self.assertNotIn("preparation:", yaml_text)
        self.assertNotIn("unique_identity", yaml_text)
        for name in names:
            self.assertIn(name + ":", yaml_text)

    def test_discover_captures_value_baseline_for_yaml(self):
        rules = self.make_rules()
        source = object()
        prepared = object()

        def prepare(actual):
            self.assertIs(actual, source)
            rules._prepared_value_anomaly = (("amount", 3.0),)
            rules._value_anomaly_baselines = {"amount": (15.0, 7.0710678118654755)}
            return prepared

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "rules.yaml"
            with patch.object(DataBuck, "discover_rules", return_value=rules), \
                 patch.object(rules, "prepare_for_lake", side_effect=prepare) as prepare_mock:
                DataBuck.discover(source, str(destination))
            prepare_mock.assert_called_once_with(source)
            self.assertIn("valid_value_amount:", destination.read_text())
            self.assertNotIn("preparation:", destination.read_text())

    def test_date_pair_exports_consistency_range(self):
        rules = DiscoveredRules([
            {"ruleType": "Date Rule", "columnName": "authorization_date",
             "parameters": {"dateFormat": "yyyy-MM-dd"}},
            {"ruleType": "Date Rule", "columnName": "transaction_date",
             "parameters": {"dateFormat": "yyyy-MM-dd"}},
        ])
        with self.assertRaisesRegex(ValueError, "date consistency"):
            rules.to_lake("unused.json")
        rules._date_pair_bounds = {("authorization_date", "transaction_date"): (-1.0, 3.0)}
        with tempfile.TemporaryDirectory() as directory:
            result = json.loads(Path(rules.to_lake(Path(directory) / "rules.json")).read_text())
        expression = result["warn"]["consistent_date_authorization_date_transaction_date"]
        self.assertIn("DATEDIFF(", expression)
        self.assertIn("BETWEEN -1 AND 3", expression)
        self.assertNotIn("preparation", result)

    def test_default_pattern_exports_frequency_tagged_patterns(self):
        rules = DiscoveredRules([
            {"ruleType": "Default Pattern Check", "columnName": "device_id",
             "parameters": {"topPattern": "[val:AA-####per: (80%), val:###per: (20%)]"}},
        ])
        with tempfile.TemporaryDirectory() as directory:
            json_doc = json.loads(Path(rules.to_lake(Path(directory) / "rules.json")).read_text())
            yaml_text = Path(rules.to_lake(Path(directory) / "rules.yaml")).read_text()

        expression = json_doc["warn"]["valid_pattern_device_id"]
        self.assertIn("REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(`device_id` AS STRING))", expression)
        self.assertIn("IN ('AA-####', '###')", expression)
        self.assertIn("TRIM(CAST(`device_id` AS STRING)) = ''", expression)
        self.assertIn("valid_pattern_device_id:", yaml_text)
        self.assertIn("AA-####", yaml_text)

    def test_default_pattern_exports_plain_patterns(self):
        rules = DiscoveredRules([
            {"ruleType": "Default Pattern Check", "columnName": "reference",
             "parameters": {"topPattern": "[AA-####, ###]"}},
        ])
        with tempfile.TemporaryDirectory() as directory:
            result = json.loads(Path(rules.to_lake(Path(directory) / "rules.json")).read_text())
        self.assertIn("IN ('AA-####', '###')", result["warn"]["valid_pattern_reference"])


if __name__ == "__main__":
    unittest.main()
