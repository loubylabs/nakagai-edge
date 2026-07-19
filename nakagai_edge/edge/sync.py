"""Pull the platform bundle into the edge root.

The synced registry drops into the gateway's own expected location
(config/connectors.yaml under the edge root), so ConnectorHub, guardrails, and
FileTokenStorage run unmodified. Freshness is the fail-closed gate: past the
TTL the runtime refuses every connector call until a sync succeeds."""

import json
import time

import httpx
import yaml

from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.state import EdgeState

POLICY_TTL_S = 900          # deny everything on staler policy
SYNC_INTERVAL_S = 60        # etag check cadence


def apply_bundle(state: EdgeState, bundle: dict, etag: str) -> None:
    reg_path = state.root / "config" / "connectors.yaml"
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    connectors_doc = bundle.get("connectors") or {"connectors": []}
    reg_path.write_text(yaml.safe_dump(_edge_connectors_doc(state, connectors_doc),
                                       sort_keys=False))
    state.bundle_path.parent.mkdir(parents=True, exist_ok=True)
    state.bundle_path.write_text(json.dumps(bundle))
    _stamp(state, etag)


def _edge_connectors_doc(state: EdgeState, connectors_doc: dict) -> dict:
    """The platform's registry names `nakagai-mcp` with a dial-itself localhost
    URL and no auth, correct for the platform but useless on the edge. Rewrite
    that one entry, on a copy, to dial the real platform with the agent's own
    bearer token; leave every other entry (and a malformed/unpaired doc)
    untouched. Never raises: an unexpected bundle shape just skips the
    rewrite, same as leaving the registry verbatim."""
    try:
        agent = state.agent()
        if agent is None:
            return connectors_doc
        platform_url = (agent.get("platform_url") or "").rstrip("/")
        if not platform_url:
            return connectors_doc
        entries = connectors_doc.get("connectors")
        if not isinstance(entries, list):
            return connectors_doc
        new_entries = []
        changed = False
        for entry in entries:
            if isinstance(entry, dict) and entry.get("id") == "nakagai-mcp":
                entry = dict(entry)
                # Trailing slash matters: the platform mounts at /mcp/, and the
                # bare path 307s there with an http:// Location behind Fly's
                # proxy, which makes httpx drop the Authorization header
                # (insecure-origin redirect) and every call 401.
                entry["url"] = f"{platform_url}/mcp/"
                entry["auth"] = {"mode": "bearer", "token_env": "NAKAGAI_AGENT_TOKEN"}
                changed = True
            new_entries.append(entry)
        if not changed:
            return connectors_doc
        new_doc = dict(connectors_doc)
        new_doc["connectors"] = new_entries
        return new_doc
    except Exception:
        return connectors_doc


def _stamp(state: EdgeState, etag: str) -> None:
    """Record that the platform answered us, authoritatively, just now.

    Written on both answering paths (a 200 through apply_bundle, and a 304) and
    on neither failure path. That asymmetry is load-bearing: see fetched_at.
    """
    state.meta_path.parent.mkdir(parents=True, exist_ok=True)
    state.meta_path.write_text(json.dumps({"etag": etag, "fetched_at": time.time()}))


def meta(state: EdgeState) -> dict:
    if not state.meta_path.exists():
        return {}
    try:
        return json.loads(state.meta_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def cached_bundle(state: EdgeState) -> dict | None:
    if not state.bundle_path.exists():
        return None
    try:
        return json.loads(state.bundle_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def public_key(state: EdgeState) -> str:
    bundle = cached_bundle(state) or {}
    return bundle.get("signing_public_key", "")


def fetched_at(state: EdgeState) -> float:
    """When the platform last answered this edge, or 0.0 if it never has.

    The one honest signal of whether a pull reached the platform. sync_once
    returns False for two very different things, a 304 and a total failure, and
    a registry on disk proves nothing on an edge that synced before: it is
    there either way. This advances only when the platform answered, so a
    caller that reads it before and after a sync can tell the two apart.
    """
    try:
        return float(meta(state).get("fetched_at", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def policy_fresh(state: EdgeState, ttl_s: int = POLICY_TTL_S) -> bool:
    fetched = fetched_at(state)
    return bool(fetched) and (time.time() - fetched) < ttl_s


def sync_once(state: EdgeState, client: PlatformClient) -> bool:
    """One conditional fetch. Returns True when the bundle changed. No
    exception ever escapes: network trouble, a non-JSON body, a bad 304
    stamp, or a malformed bundle all leave the cache untouched and return
    False. The TTL will fail us closed soon enough. The background syncer loop
    depends on that silence: a network blip must not kill the edge.

    Silence is not the same as success, though, and False alone cannot say
    which happened. Both answering paths stamp fetched_at and neither failure
    path does, so a caller that wants to know (the CLI does, the loop does not)
    reads it around the call."""
    try:
        etag, bundle = client.get_bundle(etag=meta(state).get("etag", ""))
    except (EdgeClientError, httpx.HTTPError, ValueError):
        return False
    try:
        if bundle is None:        # 304: policy unchanged, still authoritative
            _stamp(state, etag)
            return False
        apply_bundle(state, bundle, etag)
        return True
    except Exception:
        return False
