# Publish with GitHub Actions trusted publishing

The publishing workflow is `.github/workflows/publish.yml` at the repository
root. It runs only when manually started on `main`. It tests and builds the
Python package from `databuck-python/databuck-spark/`, checks the distributions,
and publishes them to PyPI without a manually created API token.

For the first release, add a pending trusted publisher on PyPI with:

- PyPI project name: `databuck-spark-sdk`
- Owner: `FirstEigen-Labs`
- Repository name: `databuck_sdk`
- Workflow name: `publish.yml`
- Environment name: leave blank

Commit and push the workflow to `main` before running it. Then in GitHub, open
**Actions → Publish databuck-spark-sdk to PyPI → Run workflow**, select `main`,
and start the run. Check its result and the new release on PyPI.

Each PyPI release needs a new version in `pyproject.toml`; release files for an
existing version cannot be overwritten. Review the version and source changes
before starting the workflow.

## Optional local build check

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

Use a fresh output directory for each build. On Windows, rebuilding into a
directory that already contains the same archive can fail with `WinError 5`.
