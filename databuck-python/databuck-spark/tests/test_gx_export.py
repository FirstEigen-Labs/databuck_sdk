import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABUCK_SDK_AUTO_DOWNLOAD", "0")

from databuck.gx_rules import write_gx_suite
from databuck.lake_rules import DiscoveredRules, _export_format


class FakeExpectation:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeSuite:
    def __init__(self, *, name, expectations, meta):
        self.name = name
        self.expectations = expectations
        self.meta = meta

    def to_json_dict(self):
        return {
            "name": self.name,
            "meta": self.meta,
            "expectations": [
                {"type": item.__class__.__name__, **item.kwargs}
                for item in self.expectations
            ],
        }


class FakeDataFrame:
    def __init__(self):
        self.schema = SimpleNamespace(fields=[
            SimpleNamespace(name="account_id", dataType=SimpleNamespace(
                simpleString=lambda: "string")),
            SimpleNamespace(name="amount", dataType=SimpleNamespace(
                simpleString=lambda: "integer")),
            SimpleNamespace(name="funded_amnt_inv", dataType=SimpleNamespace(
                simpleString=lambda: "double")),
            SimpleNamespace(name="funded_amnt", dataType=SimpleNamespace(
                simpleString=lambda: "double")),
        ])

    def selectExpr(self, expression):
        return SimpleNamespace(schema=SimpleNamespace(fields=[
            SimpleNamespace(dataType=SimpleNamespace(simpleString=lambda: "boolean"))
        ]))


def fake_gx_module():
    gx = ModuleType("great_expectations")
    kinds = (
        "ExpectColumnValuesToNotBeNull", "ExpectColumnValuesToBeBetween",
        "UnexpectedRowsExpectation",
    )
    gx.expectations = SimpleNamespace(**{
        kind: type(kind, (FakeExpectation,), {}) for kind in kinds
    })
    gx.ExpectationSuite = FakeSuite
    return gx


class GXExportTests(unittest.TestCase):
    def test_gx_suffix_is_separate_from_json_and_yaml(self):
        self.assertEqual(_export_format("loan.gx.json"), "gx")
        self.assertEqual(_export_format("loan.json"), "json")
        self.assertEqual(_export_format("loan.yaml"), "yaml")

    def test_suite_contains_native_sql_and_dataset_checks(self):
        grouped = {
            "warn": {"required_account": "account_id IS NOT NULL"},
            "drop": {"positive_amount": "amount > 0"},
            "fail": {"funding_limit": "funded_amnt_inv <= funded_amnt"},
        }
        dataset_checks = {
            "unique_account": {
                "sql": "SELECT account_id, COUNT(*) FROM {{DATAFRAME}} "
                       "GROUP BY account_id HAVING COUNT(*) > 1"
            }
        }
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(sys.modules, {"great_expectations": fake_gx_module()}):
            output = Path(directory) / "loan.gx.json"
            self.assertEqual(write_gx_suite(
                grouped, output, df=FakeDataFrame(),
                dataset_checks=dataset_checks, table_name="catalog.schema.loan",
            ), str(output))
            result = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(result["name"], "loan")
        self.assertEqual(result["meta"]["table"], "catalog.schema.loan")
        items = result["expectations"]
        self.assertEqual(len(items), 5)
        self.assertEqual(items[0]["type"], "ExpectColumnValuesToNotBeNull")
        self.assertEqual(items[0]["meta"]["databuck_action"], "warn")
        self.assertEqual(items[1]["type"], "ExpectColumnValuesToBeBetween")
        self.assertTrue(items[1]["strict_min"])
        self.assertEqual(items[3]["type"], "UnexpectedRowsExpectation")
        self.assertIn("FROM {batch} WHERE NOT COALESCE", items[3]["unexpected_rows_query"])
        self.assertEqual(items[3]["meta"]["databuck_action"], "fail")
        self.assertIn("FROM {batch} GROUP BY", items[4]["unexpected_rows_query"])

    def test_to_lake_routes_only_gx_suffix_to_gx_writer(self):
        rules = DiscoveredRules([{
            "ruleType": "Null Check", "columnName": "account_id",
            "parameters": {"nullThreshold": 0},
        }])
        with patch("databuck.gx_rules.write_gx_suite", return_value="rules.gx.json") as writer:
            self.assertEqual(rules.to_lake("rules.gx.json", df=FakeDataFrame()),
                             "rules.gx.json")
        self.assertEqual(writer.call_args.args[0]["warn"],
                         {"not_null_account_id": "`account_id` IS NOT NULL"})
        self.assertIsInstance(writer.call_args.kwargs["df"], FakeDataFrame)

    def test_gx_export_keeps_classified_actions_as_metadata_input(self):
        rules = DiscoveredRules([{
            "ruleType": "Null Check", "columnName": "account_id",
            "parameters": {"nullThreshold": 0},
        }])
        decisions = {"not_null_account_id": {"action": "fail", "reason": "Required key"}}
        with patch("databuck.lake_rules._classify_actions_with_llm",
                   return_value=decisions), \
             patch("databuck.gx_rules.write_gx_suite", return_value="rules.gx.json") as writer:
            rules.to_lake("rules.gx.json", df=FakeDataFrame(),
                          context={"gemini_api_key": "test-key"})
        self.assertEqual(writer.call_args.args[0]["fail"],
                         {"not_null_account_id": "`account_id` IS NOT NULL"})


if __name__ == "__main__":
    unittest.main()
