import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck.sdk import DataBuck, JAR_URL


def jar_bytes():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
    return output.getvalue()


class JarDownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.jar = Path(self.temp.name) / "jars" / "databuck-spark-sdk.jar"

    def test_explicit_download_to_configured_path_once(self):
        payload = jar_bytes()
        environment = {
            "DATABUCK_SPARK_SDK_JAR": str(self.jar),
            "DATABUCK_SPARK_SDK_JAR_SHA256": hashlib.sha256(payload).hexdigest(),
        }
        with patch.dict(os.environ, environment, clear=False):
            with patch("databuck.sdk.urllib.request.urlopen", return_value=io.BytesIO(payload)) as open_url:
                self.assertEqual(DataBuck.download_jar(), str(self.jar.resolve()))
                self.assertEqual(DataBuck.download_jar(), str(self.jar.resolve()))
                self.assertEqual(DataBuck.jar_path(), str(self.jar.resolve()))
        self.assertEqual(self.jar.read_bytes(), payload)
        open_url.assert_called_once_with(JAR_URL, timeout=60)

    def test_cli_downloads_jar_and_prints_path(self):
        payload = jar_bytes()
        source = Path(self.temp.name) / "source.jar"
        source.write_bytes(payload)
        environment = os.environ.copy()
        environment.pop("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", None)
        environment["DATABUCK_SPARK_SDK_JAR"] = str(self.jar)
        environment["DATABUCK_SPARK_SDK_JAR_URL"] = source.as_uri()
        environment["DATABUCK_SPARK_SDK_JAR_SHA256"] = hashlib.sha256(payload).hexdigest()
        result = subprocess.run(
            [sys.executable, "-m", "databuck"],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), str(self.jar.resolve()))
        self.assertEqual(self.jar.read_bytes(), payload)

    def test_import_downloads_jar_and_sets_path(self):
        payload = jar_bytes()
        source = Path(self.temp.name) / "source.jar"
        source.write_bytes(payload)
        environment = os.environ.copy()
        environment.pop("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", None)
        environment["DATABUCK_SPARK_SDK_JAR"] = str(self.jar)
        environment["DATABUCK_SPARK_SDK_JAR_URL"] = source.as_uri()
        environment["DATABUCK_SPARK_SDK_JAR_SHA256"] = hashlib.sha256(payload).hexdigest()
        result = subprocess.run(
            [sys.executable, "-c", "import databuck, os; print(os.environ['DATABUCK_SPARK_SDK_JAR'])"],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), str(self.jar.resolve()))
        self.assertIn("JAR ready", result.stderr)
        self.assertEqual(self.jar.read_bytes(), payload)

    def test_jar_path_requires_download(self):
        with patch.dict(os.environ, {"DATABUCK_SPARK_SDK_JAR": str(self.jar)}, clear=False):
            with self.assertRaisesRegex(FileNotFoundError, "python -m databuck"):
                DataBuck.jar_path()

    def test_http_error_does_not_leave_partial_jar(self):
        error = urllib.error.HTTPError(JAR_URL, 403, "Forbidden", {}, None)
        with patch("databuck.sdk.urllib.request.urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "403"):
                DataBuck.download_jar(self.jar)
        self.assertFalse(self.jar.exists())
        self.assertEqual(list(self.jar.parent.iterdir()), [])

    def test_invalid_download_does_not_leave_partial_jar(self):
        with patch("databuck.sdk.urllib.request.urlopen", return_value=io.BytesIO(b"not a jar")):
            with self.assertRaisesRegex(RuntimeError, "not a valid JAR"):
                DataBuck.download_jar(self.jar)
        self.assertFalse(self.jar.exists())
        self.assertEqual(list(self.jar.parent.iterdir()), [])

    def test_checksum_mismatch_does_not_leave_partial_jar(self):
        with patch("databuck.sdk.urllib.request.urlopen", return_value=io.BytesIO(jar_bytes())):
            with self.assertRaisesRegex(RuntimeError, "SHA-256"):
                DataBuck.download_jar(self.jar)
        self.assertFalse(self.jar.exists())
        self.assertEqual(list(self.jar.parent.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
