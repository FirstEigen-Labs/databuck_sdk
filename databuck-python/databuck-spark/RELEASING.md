# Build a PyPI release

Run these commands from `databuck-spark/` after setting the intended version
in `pyproject.toml`:

```bash
python -m pip install --upgrade build twine
python -m build --outdir dist-release-NEW_VERSION
python -m twine check dist-release-NEW_VERSION/*
```

Inspect both generated files (`.whl` and `.tar.gz`) before upload. The wheel
must contain the `databuck` package and its metadata. The JAR is downloaded
when `databuck` is imported or by `python -m databuck` after installation. Check that its
public S3 URL works without credentials before publishing. Do not stage the
large JAR in the Python distribution.

Once the release is reviewed, upload with:

```bash
python -m twine upload dist-release-NEW_VERSION/*
```

Each PyPI release needs a new version number; files for an existing version
cannot be overwritten.
Use a fresh output directory for each build. On Windows, rebuilding into a
directory that already contains the same archive can fail with `WinError 5`.
