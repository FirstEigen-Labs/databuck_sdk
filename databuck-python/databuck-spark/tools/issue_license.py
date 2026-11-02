"""Issue a signed DataBuck customer license. Keep the private key outside Git."""

import argparse
import base64
import json
from datetime import date
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key", required=True, type=Path, help="Private signing key PEM")
    parser.add_argument("--customer-id", required=True)
    parser.add_argument("--issued-on", default=date.today().isoformat())
    parser.add_argument("--expires-on", required=True)
    parser.add_argument("--output", required=True, type=Path,
                        help="Customer databucksdk.properties file")
    args = parser.parse_args()

    issued_on = date.fromisoformat(args.issued_on)
    expires_on = date.fromisoformat(args.expires_on)
    if issued_on > expires_on:
        parser.error("--expires-on must be on or after --issued-on")
    if not args.customer_id.strip():
        parser.error("--customer-id cannot be empty")
    if any(char in args.customer_id for char in "\r\n"):
        parser.error("--customer-id cannot contain line breaks")
    if args.output.suffix.lower() != ".properties":
        parser.error("--output must end in .properties")
    key = serialization.load_pem_private_key(args.key.read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        parser.error("--key must contain an Ed25519 private key")
    document = {
        "customer_id": args.customer_id.strip(),
        "issued_on": issued_on.isoformat(),
        "expires_on": expires_on.isoformat(),
    }
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    document["license_key"] = base64.b64encode(key.sign(payload)).decode("ascii")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        for name in ("customer_id", "issued_on", "expires_on", "license_key"):
            stream.write(f"{name}={document[name]}\n")
    print(f"Issued config for {document['customer_id']} through {expires_on}: {args.output}")


if __name__ == "__main__":
    main()
