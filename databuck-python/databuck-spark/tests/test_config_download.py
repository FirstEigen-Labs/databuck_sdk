import io
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from urllib.error import URLError
from unittest.mock import patch

os.environ.setdefault("DATABUCK_SDK_AUTO_DOWNLOAD", "0")

from databuck.license import CONFIG_FILE_URL, LicenseError, download_config_file


class ConfigDownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "databucksdk.properties"
        self.signed_config = Path(os.environ["DATABUCK_SDK_CONFIG_FILE"])

    def test_default_properties_url(self):
        self.assertEqual(
            CONFIG_FILE_URL,
            "https://tmplog1-pub.s3.us-east-1.amazonaws.com/databucksdk.properties",
        )

    def test_downloads_signed_properties_to_configured_path(self):
        payload = self.signed_config.read_bytes()
        with patch.dict(os.environ, {"DATABUCK_SDK_CONFIG_FILE": str(self.config)}):
            with patch("databuck.license.urllib.request.urlopen",
                       return_value=io.BytesIO(payload)) as open_url:
                self.assertEqual(download_config_file(), str(self.config.resolve()))
        self.assertEqual(self.config.read_bytes(), payload)
        open_url.assert_called_once_with(CONFIG_FILE_URL, timeout=60)

    def test_existing_config_is_reused(self):
        self.config.write_bytes(self.signed_config.read_bytes())
        with patch.dict(os.environ, {"DATABUCK_SDK_CONFIG_FILE": str(self.config)}):
            with patch("databuck.license.urllib.request.urlopen") as open_url:
                self.assertEqual(download_config_file(), str(self.config.resolve()))
        open_url.assert_not_called()

    def test_invalid_download_does_not_leave_config(self):
        with patch("databuck.license.urllib.request.urlopen",
                   return_value=io.BytesIO(b"not a signed license")):
            with self.assertRaises(LicenseError):
                download_config_file(self.config)
        self.assertFalse(self.config.exists())
        self.assertEqual(list(self.config.parent.iterdir()), [])

    def test_unreachable_url_does_not_leave_config(self):
        with patch("databuck.license.urllib.request.urlopen",
                   side_effect=URLError("network unavailable")):
            with self.assertRaisesRegex(RuntimeError, "Could not download"):
                download_config_file(self.config)
        self.assertFalse(self.config.exists())
        self.assertEqual(list(self.config.parent.iterdir()), [])

    def test_import_downloads_config_before_jar(self):
        jar_source = Path(self.temp.name) / "source.jar"
        with zipfile.ZipFile(jar_source, "w") as archive:
            archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        jar_path = Path(self.temp.name) / "databuck-sdk.jar"
        environment = os.environ.copy()
        environment.pop("DATABUCK_SDK_AUTO_DOWNLOAD", None)
        environment["DATABUCK_SDK_CONFIG_FILE"] = str(self.config)
        environment["DATABUCK_SDK_CONFIG_FILE_URL"] = self.signed_config.as_uri()
        environment["DATABUCK_SDK_JAR"] = str(jar_path)
        environment["DATABUCK_SDK_JAR_URL"] = jar_source.as_uri()
        result = subprocess.run(
            [sys.executable, "-c", "import databuck; print(databuck.DataBuck.jar_path())"],
            cwd=Path(__file__).resolve().parents[1], env=environment,
            capture_output=True, text=True, check=True,
        )
        self.assertEqual(result.stdout.strip(), str(jar_path.resolve()))
        self.assertEqual(self.config.read_bytes(), self.signed_config.read_bytes())
        self.assertTrue(zipfile.is_zipfile(jar_path))


if __name__ == "__main__":
    unittest.main()
