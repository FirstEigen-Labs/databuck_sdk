# DataBuck SDK

Python package for discovering data quality rules from a Spark DataFrame or a
registered Spark table and exporting Databricks Lakeflow expectations. The PyPI
package is `databuck-sdk`; the Python import is `databuck`.

## Install

```python
%pip install databuck-sdk
```

After installing or upgrading in Databricks, run
`dbutils.library.restartPython()` before importing `databuck`. Check the
installed version with `importlib.metadata.version("databuck-sdk")`.
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

Discovery combines automatic and policy-informed rules. With `domain_name`,
the output contains selected HIGH and MEDIUM rules. JSON groups row rules by
`warn`, `drop`, and `fail`; YAML lists them without action groups. The SDK also
writes a CSV of the selected rules beside the output file.

See the [Python SDK README](databuck-python/databuck-spark/README.md) for full
setup, JSON and YAML examples, and Lakeflow pipeline usage.
