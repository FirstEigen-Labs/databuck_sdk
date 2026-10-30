# databuck-sdk

Python wrapper for the DataBuck Spark SDK. It discovers data-quality rules from
an existing Spark DataFrame or a registered Spark table and exports expectations
that Databricks Lakeflow Declarative Pipelines can execute before downstream
data is published.

## Install

```bash
python -m pip install databuck-sdk
python -c "import databuck"
```

If `databuck-spark-sdk` is installed, remove it before installing this package;
both distributions provide the same `databuck` Python import:

```bash
python -m pip uninstall databuck-spark-sdk
python -m pip install --upgrade databuck-sdk
```

Importing `databuck` downloads `databuck-sdk.jar` from the DataBuck S3
URL to `databuck/jars/` in the installed Python package, with progress output.
The downloaded path is set in `DATABUCK_SDK_JAR` for the current Python
process. A later import reuses the existing JAR.
The JAR is not included in the wheel because it exceeds PyPI's
default per-file upload limit. An internet connection and write access to the
installed package directory are required for the first download.

`python -m databuck` is an explicit download command that also prints the
local path. Set `DATABUCK_SDK_AUTO_DOWNLOAD=0` before import to defer the
download when offline. `DataBuck.jar_path()` returns the path after download.
Downloading the JAR does not install it as a Databricks compute library.

To use an existing JAR or another writable destination, set
`DATABUCK_SDK_JAR` to that file path. If the download URL changes, set
`DATABUCK_SDK_JAR_URL` to the new HTTPS URL before importing or running
`python -m databuck`. The download is checked against
the published JAR's SHA-256 hash. If the JAR content changes, publish a new
Python package version with its new hash, or set
`DATABUCK_SDK_JAR_SHA256` to the expected hash.

### Databricks notebook setup

Run this once in the notebook to install the Python package:

```python
%pip install databuck-sdk
```

After installing or upgrading, restart Python in the next cell so the notebook
loads the new package version:

```python
dbutils.library.restartPython()
```

Then verify the version and set a writable Unity Catalog Volume **file path**
before importing `databuck`:

```python
import os
from importlib.metadata import version

print(version("databuck-sdk"))

os.environ["DATABUCK_SDK_JAR"] = (
    "/Volumes/catalog/schema/volume/databuck-sdk.jar"
)

from databuck import DataBuck  # Downloads the JAR to that path on first import.

print(DataBuck.jar_path())
```

Use the full filename ending in `.jar`, not just the Volume directory. Importing
`databuck` downloads or reuses the JAR at that path. Setting the variable after
the import is too late to choose the initial download location. A Volume keeps
the JAR available beyond the current notebook driver session.

Before calling `DataBuck.discover(...)`, attach the downloaded JAR to the
**compute that runs discovery**: in Databricks, open **Compute** > select that
compute > **Libraries** > **Install New**, select the Volume as the library
source, choose the JAR path printed above, and install it. If the compute is
already running, restart it or reattach the notebook so Spark loads the new
library. Then read your Spark DataFrame and run the discovery example below.
If the old `databuck-spark-sdk.jar` is attached to that compute, remove it
before attaching `databuck-sdk.jar`.
The Python import downloads the file; it does not attach the JAR to compute.
See [Databricks compute-scoped library instructions](https://docs.databricks.com/aws/en/libraries/cluster-libraries).

## Local PySpark usage

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

Run discovery in a Databricks notebook or Spark job against an existing DataFrame,
or pass a Spark table name and the SDK will read it into a DataFrame internally.
Pass PDF and DOCX policy files for context-aware BuckGPT rules.
The SDK exports generated rules as JSON (`.json`), YAML (`.yaml` / `.yml`),
or a Great Expectations suite (`.gx.json`).
A Lakeflow Declarative Pipeline then loads and executes those rules as
expectations. Choose one of the two JSON inputs below; both use the same
discovery and export flow. The YAML workflow appears below.

```python
from databuck import DataBuck

context = {
    "gemini_api_key": "YOUR_GEMINI_API_KEY",
    "domain_name": "banking",
    "pdf_paths": [
        "/Volumes/catalog/schema/volume/customer_policy.pdf",
        "/Volumes/catalog/schema/volume/data_contract.docx",
    ],
}

# Option 1: use a DataFrame already available in your pipeline.
json_path = DataBuck.discover(
    df, "/Volumes/catalog/schema/volume/expectations.json", context=context
)
print(json_path)
```

Or pass a registered Spark table name. The SDK calls `spark.read.table(...)`
internally, then passes that DataFrame through the same discovery and export:

```python
json_path = DataBuck.discover(
    "catalog.schema.telco_customer_subscription",
    "/Volumes/catalog/schema/volume/expectations.json",
    context=context,
)
print(json_path)
```

Replace the example Volume paths and table name with files and a table available
in your workspace. The table-name option uses the notebook's Spark session and
requires read access to the registered table; it does not take separate database
connection details. Passing an existing DataFrame avoids reading the table again.

The call combines automatic and policy-informed rules. The output extension
selects JSON or YAML. With `domain_name`, JSON and YAML contain selected HIGH
and MEDIUM rules from both sources. The SDK writes `*_all_rules.csv` for review
and `*_high_medium_rules.csv` beside the output file for the selected rules.
The selected-rules CSV includes rule names, expressions, sources, and
priorities.

`pdf_paths` accepts PDF and DOCX files available to the notebook. For a
dataset-level uniqueness rule, the output includes a `dataset_checks` section.
Run its SQL against the full dataset separately, replacing `{{DATAFRAME}}` with
your source table or view. Dataset checks are not Lakeflow row expectations.

To export only automatic profiling rules, omit `context`:

```python
json_path = DataBuck.discover(
    df, "/Volumes/catalog/schema/volume/expectations.json"
)
```

Without context, the output contains automatic rules and JSON places them in
the `warn` group. To include policy-informed rules, pass a business statement
or policy document along with the Gemini API key.

The JSON contains three action dictionaries: `warn`, `drop`, and `fail`.
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

@dp.materialized_view(name="telco_customers_checked")
@dp.expect_all(expectations["warn"])
@dp.expect_all_or_drop(expectations["drop"])
@dp.expect_all_or_fail(expectations["fail"])
def customers_checked():
    return spark.read.table("catalog.schema.telco_customer_subscription")
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

The example shows the file shape; actual actions depend on the generated rules.

## Export flat YAML expectations

Pass a `.yaml` or `.yml` output file to write a flat YAML document. Pass either
an existing DataFrame or a table name. Choose one of these calls:

```python
from databuck import DataBuck

context = {
    "gemini_api_key": "YOUR_GEMINI_API_KEY",
    "pdf_paths": [
        "/Volumes/catalog/schema/volume/customer_policy.pdf",
        "/Volumes/catalog/schema/volume/data_contract.docx",
    ],
}

# Option 1: use a DataFrame already available in your pipeline.
yaml_path = DataBuck.discover(
    df,
    "/Volumes/catalog/schema/volume/telco_expectations.yaml",
    context=context,
)
print(yaml_path)
```

Or pass a registered Spark table name. The SDK reads it into a DataFrame
internally before discovering and exporting the rules:

```python
yaml_path = DataBuck.discover(
    "catalog.schema.telco_customer_subscription",
    "/Volumes/catalog/schema/volume/telco_expectations.yaml",
    context=context,
)
print(yaml_path)
```

No separate `table_name=` argument is needed. When you pass a table name, YAML
includes a `table:` field. When you pass a DataFrame, YAML contains
`expectations:` without a `table:` field because the original table name is
not reliably available from a DataFrame.

For example, the generated file has this shape (the names and SQL depend on the
discovered rules):

```yaml
table: catalog.schema.telco_customer_subscription

expectations:
  not_null_subscriber_id: "`subscriber_id` IS NOT NULL"
  BuckGPT_Rule_001: "NOT COALESCE((subscription_status_cd = 'INVALID'), FALSE)"
```

YAML combines automatic and policy-informed rules under `expectations` without
`warn`, `drop`, or `fail` groups. It does not call the action classifier. Use
JSON when the pipeline needs those action groups.
Omit `context` to write only automatically discovered rules.

To run the flat YAML expectations in a Lakeflow Declarative Pipeline, add
`PyYAML` to the pipeline's Python dependencies and load the `expectations`
mapping:

```python
import yaml
from pyspark import pipelines as dp

with open("/Volumes/catalog/schema/volume/telco_expectations.yaml", encoding="utf-8") as stream:
    rules_doc = yaml.safe_load(stream)

source_table = rules_doc.get("table") or "catalog.schema.telco_customer_subscription"

@dp.materialized_view(name="telco_customers_checked")
@dp.expect_all(rules_doc["expectations"])
def telco_customers_checked():
    return spark.read.table(source_table)
```

If you discovered from a DataFrame, set the fallback `source_table` to the
table or transformation that produces the same columns and rows.

In this example, YAML rules are monitored with `dp.expect_all`: invalid rows
remain in the output and their failures are recorded. Use the action-grouped
JSON format if rules must drop rows or fail the pipeline update.

`DataBuck.discover_rules(df)` still returns automatic profiling rules without
exporting. For compatibility, `DataBuck.discover(df, context_dict)` still
returns BuckGPT rules without exporting. Pass an output file path as the second
argument to `DataBuck.discover(...)` to combine discovery and export.

In JSON, each action dictionary maps expectation names to Spark SQL pass
conditions, such as `{"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"}`.

## Export a Great Expectations suite

Install the GX extra, then give `DataBuck.discover` a path ending in `.gx.json`:

```python
%pip install "databuck-sdk[gx]"
```

```python
gx_path = DataBuck.discover(
    "catalog.schema.loan_table",
    "/Volumes/catalog/schema/volume/loan_expectations.gx.json",
    context=context,
)
print(gx_path)
```

The GX file contains the selected HIGH and MEDIUM rules when `domain_name` is
provided. With no domain, it contains all exportable rules. It includes
dataset-level checks. Each row rule keeps its DataBuck action (`warn`, `drop`,
or `fail`) in GX metadata. GX validation reports failures; it does not apply
Lakeflow drop or fail actions to the data.
