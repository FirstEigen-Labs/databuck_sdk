# DataBuck SDK

[![PyPI version](https://img.shields.io/pypi/v/databuck-sdk.svg)](https://pypi.org/project/databuck-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/databuck-sdk.svg)](databuck-python/databuck-spark/pyproject.toml)
[![Publish to PyPI](https://github.com/FirstEigen-Labs/databuck_sdk/actions/workflows/publish.yml/badge.svg)](https://github.com/FirstEigen-Labs/databuck_sdk/actions/workflows/publish.yml)
[![License: customer license](https://img.shields.io/badge/license-customer%20license-blue.svg)](LICENSE)

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
The first import downloads the signed customer properties file and SDK JAR when
they are missing. Attach the JAR to the compute running discovery before calling
`DataBuck.discover(...)`. See the
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
`.yaml` or `.yml` creates a flat `expectations` mapping; `.gx.json` creates a
Great Expectations suite. For GX export in Databricks, install Great
Expectations separately:

```python
%pip install "great-expectations>=1.0,<2"
```

PDF and DOCX policy files are accepted in `pdf_paths`.

Discovery combines automatic and policy-informed rules. With `domain_name`,
the output contains selected HIGH and MEDIUM rules. JSON groups row rules by
`warn`, `drop`, and `fail`; YAML lists them without action groups.

See the [Python SDK README](databuck-python/databuck-spark/README.md) for full
setup, JSON and YAML examples, and Lakeflow pipeline usage.

## Licensing

DataBuck uses a signed customer license in `databucksdk.properties`. The SDK
checks its customer ID, issue date, expiry date, and signature at import and
before public SDK operations. The signed file is valid through its expiry
date, inclusive. See the [license notice](LICENSE) for the repository's
licensing status; contact DataBuck for customer terms or renewal.
