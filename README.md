# DataBuck Spark SDK

Python package for discovering data quality rules from a Spark DataFrame or a
registered Spark table and exporting Databricks Lakeflow expectations. The PyPI
package is `databuck-spark-sdk`; the Python import is `databuck`.

## Install

```python
%pip install databuck-spark-sdk
```

After installing or upgrading in Databricks, run
`dbutils.library.restartPython()` before importing `databuck`. Check the
installed version with `importlib.metadata.version("databuck-spark-sdk")`.
The first import downloads the SDK JAR. Attach that JAR to the compute running
discovery before calling `DataBuck.discover(...)`. See the
[Python SDK setup guide](databuck-python/databuck-spark/README.md#databricks-notebook-setup)
for the JAR path and compute library steps.

## Discover and export rules

```python
from databuck import DataBuck

context = {
    "gemini_api_key": "YOUR_GEMINI_API_KEY",
    "domain_name": "banking",
    "pdf_paths": ["/Volumes/catalog/schema/volume/banking_policy.pdf"],
}

output_path = DataBuck.discover(
    "catalog.schema.loan_table",
    "/Volumes/catalog/schema/volume/expectations.json",
    context=context,
)
print(output_path)
```

You can pass an existing Spark DataFrame in place of the table name. An output
path ending in `.json` creates `warn`, `drop`, and `fail` expectation groups;
`.yaml` or `.yml` creates a flat `expectations` mapping. PDF and DOCX policy
files are accepted in `pdf_paths`.

Discovery generates and audits BuckGPT rules from business context. When
`domain_name` is provided, it also selects business critical **auto-discovered
profiling rules** for export using schema and rule metadata. This selection
does not filter BuckGPT rules or send sample records. For JSON output, a later
action assignment uses up to five sample rows to classify exported rules as
`warn`, `drop`, or `fail`; YAML output has no action assignment.

See the [Python SDK README](databuck-python/databuck-spark/README.md) for full
setup, JSON and YAML examples, and Lakeflow pipeline usage.
