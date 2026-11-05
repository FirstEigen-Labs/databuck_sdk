"""Verify customer licenses using the public half of DataBuck's signing key."""

import base64
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


PUBLIC_KEY = "EtIbUm67XS0iwyvXVuiufQb525sEn7EJMpRprDFBQQc="
CONFIG_FILE_ENV = "DATABUCK_SDK_CONFIG_FILE"
CONFIG_FILE_URL_ENV = "DATABUCK_SDK_CONFIG_FILE_URL"
CONFIG_FILE_URL = "https://tmplog1-pub.s3.us-east-1.amazonaws.com/databucksdk.properties"
CONFIG_FILE_NAME = "databucksdk.properties"


class LicenseError(RuntimeError):
    """The installed DataBuck license is missing, invalid, or expired."""


def _signed_payload(document):
    return json.dumps(
        {key: document[key] for key in ("customer_id", "issued_on", "expires_on")},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _read_properties(path):
    properties = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        if "=" not in line:
            raise ValueError("each property must use key=value")
        key, value = (part.strip() for part in line.split("=", 1))
        if not key or key in properties:
            raise ValueError("property names must be unique and nonempty")
        properties[key] = value
    return properties


def _validate_license(config_path, today=None):
    try:
        config = _read_properties(config_path)
        if not config["customer_id"]:
            raise ValueError("customer_id must be a nonempty string")
        payload = _signed_payload(config)
        signature = base64.b64decode(config["license_key"], validate=True)
        Ed25519PublicKey.from_public_bytes(base64.b64decode(PUBLIC_KEY)).verify(
            signature, payload
        )
        issued_on = date.fromisoformat(config["issued_on"])
        expires_on = date.fromisoformat(config["expires_on"])
    except (OSError, ValueError, KeyError, TypeError, InvalidSignature) as exc:
        raise LicenseError(f"DataBuck config at {config_path} is missing or invalid") from exc
    if issued_on > expires_on:
        raise LicenseError(f"DataBuck config at {config_path} has invalid license dates")
    current = today or datetime.now(timezone.utc).date()
    if current < issued_on:
        raise LicenseError(f"DataBuck license is not valid before {issued_on}")
    if current > expires_on:
        raise LicenseError(
            f"DataBuck license expired on {expires_on}; request a renewed license"
        )
    return config


def require_license(*, today=None):
    """Verify the signed customer and expiry in one local config file."""
    config_location = os.environ.get(CONFIG_FILE_ENV, "").strip()
    if not config_location:
        raise LicenseError(f"Set {CONFIG_FILE_ENV} to your databucksdk.properties file")
    return _validate_license(Path(config_location).expanduser(), today=today)


def download_config_file(destination=None):
    """Download and validate the signed license before SDK import continues."""
    if destination is None:
        configured = os.environ.get(CONFIG_FILE_ENV, "").strip()
        destination = (Path(configured).expanduser() if configured else
                       Path(__file__).resolve().parent / "jars" / CONFIG_FILE_NAME)
    config_path = Path(destination).expanduser()
    if config_path.is_file():
        return str(config_path.resolve())

    url = os.environ.get(CONFIG_FILE_URL_ENV) or CONFIG_FILE_URL
    temporary = None
    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"databuck: downloading SDK properties to {config_path}", file=sys.stderr)
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{CONFIG_FILE_NAME}.", suffix=".tmp",
            dir=config_path.parent, delete=False,
        ) as output:
            temporary = Path(output.name)
            with urllib.request.urlopen(url, timeout=60) as response:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
        _validate_license(temporary)
        os.replace(temporary, config_path)
        print(f"databuck: SDK properties ready at {config_path}", file=sys.stderr)
    except (OSError, urllib.error.URLError) as exc:
        raise RuntimeError(
            f"Could not download the DataBuck SDK properties file from {url} "
            f"to {config_path}: {exc}. Set {CONFIG_FILE_ENV} to an existing "
            f"local properties file or {CONFIG_FILE_URL_ENV} to a reachable URL."
        ) from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return str(config_path.resolve())
