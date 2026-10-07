# Build a PyPI release

Run these commands from `databuck-spark/` after setting the intended version
in `pyproject.toml`:

```bash
python -m pip install --upgrade build twine
python -m build --outdir dist-release-NEW_VERSION
python -m twine check dist-release-NEW_VERSION/*
```

Inspect both generated files (`.whl` and `.tar.gz`) before upload. The wheel
must contain the `databuck` package and its metadata. If you deliberately
stage a JAR at `databuck/jars/databuck-spark-sdk.jar`, verify it is the intended
runtime artifact and that both distributions remain below the project's PyPI
file-size limit. Otherwise install the JAR separately and configure
`DATABUCK_SPARK_SDK_JAR` at runtime.

Once the release is reviewed, upload with:

```bash
python -m twine upload dist-release-NEW_VERSION/*
```

Each PyPI release needs a new version number; files for an existing version
cannot be overwritten.
Use a fresh output directory for each build. On Windows, rebuilding into a
directory that already contains the same archive can fail with `WinError 5`.
