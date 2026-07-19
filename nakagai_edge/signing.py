"""Ed25519-signed approval artifacts.

The platform signs {approval_id, agent_id, connector_id, tool, args_hash,
account, expires_at} when a human grants an edge agent's write intent. The
edge verifies the signature (and that args_hash matches the intent it holds)
before anything reaches a broker. An approval for order A can never
authorize order B, and a forged or expired artifact executes nothing.

Keys are raw Ed25519, urlsafe-base64. The private seed lives only in the
platform's NAKAGAI_APPROVAL_SIGNING_KEY; the public key ships in the bundle.
"""

import base64
import hashlib
import json
import time


def canonical_json(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      default=str).encode()


def args_hash(args: dict) -> str:
    return hashlib.sha256(canonical_json(args)).hexdigest()


def _private_key(private_b64: str):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    return Ed25519PrivateKey.from_private_bytes(
        base64.urlsafe_b64decode(private_b64.encode()))


def generate_keypair() -> tuple[str, str]:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.generate()
    priv = key.private_bytes(serialization.Encoding.Raw,
                             serialization.PrivateFormat.Raw,
                             serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.Raw,
                                        serialization.PublicFormat.Raw)
    return (base64.urlsafe_b64encode(priv).decode(),
            base64.urlsafe_b64encode(pub).decode())


def public_key_for(private_b64: str) -> str:
    from cryptography.hazmat.primitives import serialization

    pub = _private_key(private_b64).public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.urlsafe_b64encode(pub).decode()


def sign_artifact(private_b64: str, payload: dict) -> dict:
    sig = _private_key(private_b64).sign(canonical_json(payload))
    return {**payload, "sig": base64.urlsafe_b64encode(sig).decode()}


def verify_artifact(public_b64: str, artifact: dict) -> bool:
    """False on ANY failure: bad key, missing sig, tampered payload. The edge
    treats False as a hard deny; this function must never raise."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        sig = base64.urlsafe_b64decode(str(artifact["sig"]).encode())
        payload = {k: v for k, v in artifact.items() if k != "sig"}
        key = Ed25519PublicKey.from_public_bytes(
            base64.urlsafe_b64decode(public_b64.encode()))
        key.verify(sig, canonical_json(payload))
        return True
    except Exception:
        return False


def extract_account(args: dict, arg_names: list[str]) -> str:
    for name in arg_names:
        if args.get(name):
            return str(args[name])
    return ""


def build_payload(*, approval_id: str, agent_id: str, connector_id: str,
                  tool: str, args: dict, account_arg_names: list[str],
                  ttl_s: int, now: float | None = None) -> dict:
    now = time.time() if now is None else now
    return {"approval_id": approval_id, "agent_id": agent_id,
            "connector_id": connector_id, "tool": tool,
            "args_hash": args_hash(args),
            "account": extract_account(args, account_arg_names),
            "expires_at": now + ttl_s}
