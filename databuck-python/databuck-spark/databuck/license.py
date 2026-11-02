"""Verify customer licenses using the public half of DataBuck's signing key."""

import base64
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


PUBLIC_KEY = "EtIbUm67XS0iwyvXVuiufQb525sEn7EJMpRprDFBQQc="
CONFIG_FILE_ENV = "DATABUCK_SDK_CONFIG_FILE"


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


def require_license(*, today=None):
    """Verify the signed customer and expiry in one local config file."""
    config_location = os.environ.get(CONFIG_FILE_ENV, "").strip()
    if not config_location:
        raise LicenseError(f"Set {CONFIG_FILE_ENV} to your databucksdk.properties file")
    config_path = Path(config_location).expanduser()
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
