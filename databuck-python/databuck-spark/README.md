# databuck-spark

Python wrapper for the DataBuck Spark SDK, used from PySpark and Databricks.

## Install

```bash
python -m pip install databuck-spark
```

The Python distribution does not currently bundle the Java SDK JAR. Set
`DATABUCK_SPARK_SDK_JAR` to a local copy of `databuck-spark-sdk.jar` before
creating the Spark session. `DataBuck.jar_path()` checks that the file exists.
The DataBuck Spark SDK JAR in this repository is about 566 MB, above PyPI's
default per-file upload limit; it must be distributed separately or rebuilt
small enough for the project limit before it can be included in the wheel.

For a Databricks notebook, install the Python distribution as a library and
make the JAR available on the cluster. Configure the local driver path before
calling `DataBuck.jar_path()`.

## Usage

```python
from pyspark.sql import SparkSession
import os

from databuck import DataBuck

os.environ["DATABUCK_SPARK_SDK_JAR"] = "/path/to/databuck-spark-sdk.jar"
spark = (
    SparkSession.builder
    .config("spark.jars", DataBuck.jar_path())
    .getOrCreate()
)

df = spark.read.csv("customers.csv", header=True)

print(DataBuck.count(df))
```

## Export profiling rules for Databricks

```python
rules = DataBuck.discover_rules(df)
json_path = rules.to_lake(
    "/Volumes/catalog/schema/volume/expectations.json",
    df=df,
    context={
        "business_context": (
            "Missing customer email should be monitored. "
            "Orders without an order ID must be discarded. "
            "An unknown currency must stop publication."
        ),
        "gemini_api_key": "<your API key>",
    },
)
print(rules.action_decisions)  # action and reason for each exported rule
```

The JSON contains three dictionaries: `warn`, `drop`, and `fail`. Gemini
classifies each exported row expectation using the business context, DataFrame
schema, and up to five sample rows. This sends those inputs to Gemini. If no
context is passed, all exported rules go under `warn`. If an LLM response is
incomplete or invalid, export fails instead of assigning a destructive action.
In a Lakeflow pipeline, use the dictionaries with `dp.expect_all`,
`dp.expect_all_or_drop`, and `dp.expect_all_or_fail`, respectively.

```json
{
  "warn": {"not_null_customer_email": "`customer_email` IS NOT NULL"},
  "drop": {"not_null_order_id": "`order_id` IS NOT NULL"},
  "fail": {"valid_pattern_currency_code": "`currency_code` IS NULL OR CAST(`currency_code` AS STRING) RLIKE '(?:^[A-Z][A-Z][A-Z]$)'"}
}
```

The example shows the file shape; the actual actions come from the supplied
business context. `DataBuck.discover(df, context)` generates a separate list
of invalid-row SQL queries and does not automatically add those queries to the
expectation JSON.

Each action dictionary maps expectation names to Spark SQL pass conditions,
such as `{"not_null_subscriber_id": "`subscriber_id` IS NOT NULL"}`. Null, pattern,
and length rules are converted from their profiling metadata, including rules
from older SDK JARs whose `expression` field is empty. Dataset-level rules
and catalog rules without a row condition remain in `rules` but are omitted
from the file. Null rules are exported only when their threshold is 0%.
The export does not encode aggregate failure thresholds. Profiling pattern
`A` maps to `[A-Z]`, and `#` maps to `[0-9]`.
