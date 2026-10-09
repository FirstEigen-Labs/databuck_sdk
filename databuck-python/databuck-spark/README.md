# databuck-spark-sdk

Python wrapper for the DataBuck Spark SDK. It discovers data-quality rules from
an existing Spark DataFrame and exports expectations that Databricks Lakeflow
Declarative Pipelines can execute before downstream data is published.

## Install

```bash
python -m pip install databuck-spark-sdk
python -c "import databuck"
```

Importing `databuck` downloads `databuck-spark-sdk.jar` from the DataBuck S3
URL to `databuck/jars/` in the installed Python package, with progress output.
The downloaded path is set in `DATABUCK_SPARK_SDK_JAR` for the current Python
process. A later import reuses the existing JAR.
The JAR is not included in the wheel because it exceeds PyPI's
default per-file upload limit. An internet connection and write access to the
installed package directory are required for the first download.

`python -m databuck` is an explicit download command that also prints the
local path. Set `DATABUCK_SPARK_SDK_AUTO_DOWNLOAD=0` before import to defer the
download when offline. `DataBuck.jar_path()` returns the path after download.
Downloading the JAR does not install it as a Databricks compute library.

To use an existing JAR or another writable destination, set
`DATABUCK_SPARK_SDK_JAR` to that file path. If the download URL changes, set
`DATABUCK_SPARK_SDK_JAR_URL` to the new HTTPS URL before importing or running
`python -m databuck`. The download is checked against
the published JAR's SHA-256 hash. If the JAR content changes, publish a new
Python package version with its new hash, or set
`DATABUCK_SPARK_SDK_JAR_SHA256` to the expected hash.

For a Databricks notebook, install the Python distribution as a library and
make the JAR available on the cluster. If the package directory is read-only,
configure `DATABUCK_SPARK_SDK_JAR` to a writable driver path before importing.

## Usage

```python
from pyspark.sql import SparkSession
import os

from databuck import DataBuck

spark = (
    SparkSession.builder
    .config("spark.jars", DataBuck.jar_path())
    .getOrCreate()
)

df = spark.read.csv("customers.csv", header=True)

print(DataBuck.count(df))
```

## Discover and export rules for Databricks

Run discovery in a Databricks notebook or Spark job against the DataFrame already
in your pipeline. Pass PDF and DOCX policy files for context-aware BuckGPT rules.
The SDK exports generated rules as JSON (`.json`) or YAML (`.yaml` / `.yml`).
A Lakeflow Declarative Pipeline then loads and executes those rules as
expectations. The example below uses JSON; the YAML workflow appears below.

```python
from databuck import DataBuck

json_path = DataBuck.discover(
    df,
    "/Volumes/catalog/schema/volume/expectations.json",
    context={
        "gemini_api_key": dbutils.secrets.get(scope="databuck", key="gemini-api-key"),
        "pdf_paths": [
            "/Volumes/catalog/schema/volume/customer_policy.pdf",
            "/Volumes/catalog/schema/volume/data_contract.docx",
        ],
    },
)
print(json_path)
```

Replace the example Volume paths with files available in your workspace.

The single call runs profile discovery and context-aware BuckGPT discovery,
then writes both sets of row expectations into one file. The output extension
selects JSON or YAML. Context-aware expectations are named `BuckGPT_Rule_001`,
`BuckGPT_Rule_002`, etc. Their
invalid-row SQL queries are converted to Lakeflow pass conditions. Only
audited, passed BuckGPT rules in the required
`SELECT * FROM {{DATAFRAME}} WHERE <invalid-row condition>` shape can be
exported; an unsupported query fails the export.
The call prints the auto-discovered rules and the BuckGPT rules before it
exports them. Some auto-discovered rules may not be exportable as row-level
Lakeflow expectations; the export contains only those that can be converted.
The `pdf_paths` field accepts both `.pdf` and `.docx` files accessible on the
notebook driver. PDF files are uploaded to Gemini; text from Word documents is
extracted locally and included in the generation and audit prompts. Images or
scanned pages in a Word document are not read.

To export only automatic profiling rules, omit `context`:

```python
json_path = DataBuck.discover(
    df, "/Volumes/catalog/schema/volume/expectations.json"
)
```

Without context, BuckGPT is not called. In JSON, all exported rules use `warn`.

The JSON contains three dictionaries: `warn`, `drop`, and `fail`. Gemini
classifies every exported expectation using the rule expression, DataFrame
schema, and up to five sample rows. The PDF and DOCX files support BuckGPT rule
generation and audit; they are not passed again to action classification. The
schema and sample are sent to Gemini. If its response is incomplete or invalid,
export fails instead of assigning a destructive action.
In a Databricks Lakeflow Declarative Pipeline, use the dictionaries with
`dp.expect_all`, `dp.expect_all_or_drop`, and `dp.expect_all_or_fail`,
respectively.

For example, add this to the Lakeflow Declarative Pipeline source file. The
expectations run when the pipeline updates; the SDK's discovery call above
generates the file beforehand:

```python
import json
from pyspark import pipelines as dp

with open("/Volumes/catalog/schema/volume/expectations.json", encoding="utf-8") as stream:
    expectations = json.load(stream)

@dp.table
@dp.expect_all(expectations["warn"])
@dp.expect_all_or_drop(expectations["drop"])
@dp.expect_all_or_fail(expectations["fail"])
def customers_checked():
    return spark.read.table("catalog.schema.customers_source")
```

### What the pipeline results look like

`DataBuck.discover(df, output_path, ...)` prints the rules and returns the path to the rules
file. The row results appear **after the Lakeflow Declarative Pipeline runs**,
in the validated table's **Expectations** view. For example, one telco demo run
showed:

| Pipeline result | Records |
| --- | ---: |
| Written | 58 (58%) |
| Dropped | 42 (42%) |

| Expectation | Action shown in Databricks | Failed records |
| --- | --- | ---: |
| `BuckGPT_Rule_006` | ALLOW | 60 |
| `BuckGPT_Rule_007` | DROP | 40 |
| `BuckGPT_Rule_009` | DROP | 20 |
| `valid_length_network_type` | DROP | 4 |
| `valid_pattern_network_type` | DROP | 4 |

`ALLOW` corresponds to `dp.expect_all`: the violation is recorded without
dropping the row. `DROP` corresponds to `dp.expect_all_or_drop`: invalid rows
are excluded from the validated output. A `FAIL` expectation stops the pipeline
update when violated. A row can fail more than one rule, so the per-rule failed
record counts do not add up to the dropped-row total. These numbers are from one
demo update; actual results depend on the source data and exported rules.

The pipeline must read a DataFrame with the columns used by the exported
expectations. Regenerate the rules file when the source schema or business
policy changes, then refresh the pipeline.

```json
{
  "warn": {"not_null_customer_email": "`customer_email` IS NOT NULL"},
  "drop": {"not_null_order_id": "`order_id` IS NOT NULL"},
  "fail": {"valid_pattern_currency_code": "`currency_code` IS NULL OR CAST(`currency_code` AS STRING) RLIKE '(?:^[A-Z][A-Z][A-Z]$)'"}
}
```

The example shows the file shape; the actual actions are chosen by Gemini.

## Export flat YAML expectations

Pass a `.yaml` or `.yml` output file and a `table_name` to write the same
exportable rules as a flat table-scoped YAML document:

```python
yaml_path = DataBuck.discover(
    df,
    "/Volumes/catalog/schema/volume/telco_expectations.yaml",
    table_name="telco_customer_subscription",
    context={
        "gemini_api_key": dbutils.secrets.get(scope="databuck", key="gemini-api-key"),
        "pdf_paths": [
            "/Volumes/catalog/schema/volume/telco_policy.pdf",
            "/Volumes/catalog/schema/volume/telco_contract.docx",
        ],
    },
)
```

For example, the generated file has this shape (the names and SQL depend on the
discovered rules):

```yaml
table: telco_customer_subscription

expectations:
  not_null_subscriber_id: "`subscriber_id` IS NOT NULL"
  BuckGPT_Rule_001: "NOT COALESCE((subscription_status_cd = 'INVALID'), FALSE)"
```

YAML combines automatic and audited BuckGPT rules under `expectations` without
`warn`, `drop`, or `fail` groups. It does not call the action classifier. Use
JSON when the pipeline needs those action groups. A Spark DataFrame does not
reliably retain its source table name, so `table_name` is required for YAML.
Omit `context` to write only automatically discovered rules.

To run the flat YAML expectations in a Lakeflow Declarative Pipeline, add
`PyYAML` to the pipeline's Python dependencies and load the `expectations`
mapping:

```python
import yaml
from pyspark import pipelines as dp

with open("/Volumes/catalog/schema/volume/telco_expectations.yaml", encoding="utf-8") as stream:
    rules_doc = yaml.safe_load(stream)

@dp.table(name="telco_customers_checked")
@dp.expect_all(rules_doc["expectations"])
def telco_customers_checked():
    return spark.read.table(f"catalog.schema.{rules_doc['table']}")
```

In this example, YAML rules are monitored with `dp.expect_all`: invalid rows
remain in the output and their failures are recorded. Use the action-grouped
JSON format if rules must drop rows or fail the pipeline update.

`DataBuck.discover_rules(df)` still returns automatic profiling rules without
exporting. For compatibility, `DataBuck.discover(df, context_dict)` still
returns BuckGPT rules without exporting. Pass an output file path as the second
argument to `DataBuck.discover(...)` to combine discovery and export.

In JSON, each action dictionary maps expectation names to Spark SQL pass conditions,
such as `{"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"}`. Null, pattern,
and length rules are converted from their profiling metadata, including rules
from older SDK JARs whose `expression` field is empty. Dataset-level rules
and catalog rules without a row condition remain in `rules` but are omitted
from the file. Null rules are exported only when their threshold is 0%.
The export does not encode aggregate failure thresholds. Profiling pattern
`A` maps to `[A-Z]`, and `#` maps to `[0-9]`.
