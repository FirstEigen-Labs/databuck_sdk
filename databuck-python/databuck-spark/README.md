# databuck-spark-sdk

Python wrapper for the DataBuck Spark SDK, used from PySpark and Databricks.

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

```python
json_path = DataBuck.discover_and_export(
    df,
    "/Volumes/catalog/schema/volume/expectations.json",
    context={
        "business_context": (
            "Missing customer email should be monitored. "
            "Orders without an order ID must be discarded. "
            "An unknown currency must stop publication."
        ),
        "gemini_api_key": dbutils.secrets.get(scope="databuck", key="gemini-api-key"),
    },
)
print(json_path)
```

The single call runs profile discovery and context-aware BuckGPT discovery,
then writes both sets of row expectations into one JSON file. Context-aware
expectations are named `BuckGPT_Rule_001`, `BuckGPT_Rule_002`, etc. Their
invalid-row SQL queries are converted to Lakeflow pass conditions. Only
audited, passed BuckGPT rules in the required
`SELECT * FROM {{DATAFRAME}} WHERE <invalid-row condition>` shape can be
exported; an unsupported query fails the export.
`business_context` is optional when `context` contains a Gemini key and any
reference PDFs. Paths in `pdf_paths` must point to existing `.pdf` files;
convert `.docx` documents to PDF before using them here.

To export only automatic profiling rules, omit `context`:

```python
json_path = DataBuck.discover_and_export(
    df, "/Volumes/catalog/schema/volume/expectations.json"
)
```

Without context, BuckGPT is not called and all exported rules use `warn`.

The JSON contains three dictionaries: `warn`, `drop`, and `fail`. Gemini
classifies every exported expectation using the rule expression, DataFrame
schema, up to five sample rows, and any optional business context. PDF files
are used for BuckGPT rule generation, not passed again to action classification.
This sends the schema and sample to Gemini. If an
LLM response is incomplete or invalid, export fails instead of assigning a
destructive action.
In a Lakeflow pipeline, use the dictionaries with `dp.expect_all`,
`dp.expect_all_or_drop`, and `dp.expect_all_or_fail`, respectively.

For example, in the Lakeflow pipeline source file:

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

The pipeline must read a DataFrame with the columns used by the exported
expectations. Regenerate the JSON when the source schema or business policy
changes, then refresh the pipeline.

```json
{
  "warn": {"not_null_customer_email": "`customer_email` IS NOT NULL"},
  "drop": {"not_null_order_id": "`order_id` IS NOT NULL"},
  "fail": {"valid_pattern_currency_code": "`currency_code` IS NULL OR CAST(`currency_code` AS STRING) RLIKE '(?:^[A-Z][A-Z][A-Z]$)'"}
}
```

The example shows the file shape; the actual actions are chosen by Gemini.
`DataBuck.discover_rules(df)` and
`DataBuck.discover(df, context)` are still available separately. The new
`DataBuck.discover_and_export(...)` call combines them.

Each action dictionary maps expectation names to Spark SQL pass conditions,
such as `{"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"}`. Null, pattern,
and length rules are converted from their profiling metadata, including rules
from older SDK JARs whose `expression` field is empty. Dataset-level rules
and catalog rules without a row condition remain in `rules` but are omitted
from the file. Null rules are exported only when their threshold is 0%.
The export does not encode aggregate failure thresholds. Profiling pattern
`A` maps to `[A-Z]`, and `#` maps to `[0-9]`.
