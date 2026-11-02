import base64
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization

from databuck.license import LicenseError, require_license
from databuck.sdk import DataBuck


class LicenseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "databucksdk.properties"
        key = Ed25519PrivateKey.generate()
        self.private_key = key
        self.public_key = base64.b64encode(key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )).decode("ascii")
        document = {
            "customer_id": "customer-a",
            "issued_on": "2026-01-01",
            "expires_on": "2027-12-31",
        }
        payload = json.dumps(document, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False).encode("utf-8")
        document["license_key"] = base64.b64encode(key.sign(payload)).decode("ascii")
        self.write_properties(document)
        self.environment = {
            "DATABUCK_SDK_CONFIG_FILE": str(self.path),
        }

    def verify(self, today):
        with patch.dict(os.environ, self.environment, clear=True), \
                patch("databuck.license.PUBLIC_KEY", self.public_key):
            return require_license(today=today)

    def write_properties(self, document):
        self.path.write_text("".join(
            f"{key}={value}\n" for key, value in document.items()
        ), encoding="utf-8")

    def test_valid_through_last_day(self):
        self.assertEqual(self.verify(date(2027, 12, 31))["customer_id"], "customer-a")

    def test_missing_config_is_rejected(self):
        environment = {"DATABUCK_SDK_CONFIG_FILE": str(self.path.with_name("missing.properties"))}
        with patch.dict(os.environ, environment, clear=True), \
                patch("databuck.license.PUBLIC_KEY", self.public_key):
            with self.assertRaisesRegex(LicenseError, "config.*missing or invalid"):
                require_license(today=date(2027, 1, 1))

    def test_expired_next_day(self):
        with self.assertRaisesRegex(LicenseError, "expired on 2027-12-31"):
            self.verify(date(2028, 1, 1))

    def test_sdk_operation_rechecks_license(self):
        with patch("databuck.sdk.require_license", side_effect=LicenseError("expired")):
            with self.assertRaisesRegex(LicenseError, "expired"):
                DataBuck.count(object())

    def test_tampered_customer_is_rejected(self):
        self.path.write_text(
            self.path.read_text(encoding="utf-8").replace(
                "customer_id=customer-a", "customer_id=customer-b"
            ), encoding="utf-8"
        )
        with self.assertRaisesRegex(LicenseError, "invalid"):
            self.verify(date(2027, 1, 1))

    def test_tampered_expiry_is_rejected(self):
        self.path.write_text(
            self.path.read_text(encoding="utf-8").replace(
                "expires_on=2027-12-31", "expires_on=2028-12-31"
            ), encoding="utf-8"
        )
        with self.assertRaisesRegex(LicenseError, "invalid"):
            self.verify(date(2027, 1, 1))

    def test_duplicate_property_is_rejected(self):
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write("expires_on=2028-12-31\n")
        with self.assertRaisesRegex(LicenseError, "invalid"):
            self.verify(date(2027, 1, 1))

    def test_issuer_writes_verifiable_properties(self):
        key_path = Path(self.temp.name) / "signing-key.pem"
        key_path.write_bytes(self.private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ))
        issued_path = Path(self.temp.name) / "issued.properties"
        script = Path(__file__).resolve().parents[1] / "tools" / "issue_license.py"
        subprocess.run([
            sys.executable, str(script), "--key", str(key_path),
            "--customer-id", "customer-a", "--issued-on", "2026-01-01",
            "--expires-on", "2027-12-31", "--output", str(issued_path),
        ], check=True, capture_output=True, text=True)
        with patch.dict(os.environ, {"DATABUCK_SDK_CONFIG_FILE": str(issued_path)},
                        clear=True), patch("databuck.license.PUBLIC_KEY", self.public_key):
            self.assertEqual(require_license(today=date(2027, 1, 1))[
                "customer_id"], "customer-a")

    def test_import_requires_license_before_jar_download(self):
        environment = os.environ.copy()
        environment.pop("DATABUCK_SDK_CONFIG_FILE", None)
        environment["DATABUCK_SDK_AUTO_DOWNLOAD"] = "0"
        result = subprocess.run(
            [sys.executable, "-c", "import databuck"],
            cwd=Path(__file__).resolve().parents[1], env=environment,
            capture_output=True, text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DATABUCK_SDK_CONFIG_FILE", result.stderr)


if __name__ == "__main__":
    unittest.main()
