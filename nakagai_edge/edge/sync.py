"""Pull the platform bundle into the edge root.

The synced registry drops into the gateway's own expected location
(config/connectors.yaml under the edge root), so ConnectorHub, guardrails, and
FileTokenStorage run unmodified. Freshness is the fail-closed gate: past the
TTL the runtime refuses every connector call until a sync succeeds. A bundle
this edge cannot fully read is refused outright, which is that same rule one
step earlier."""

import json
import logging
import time

import httpx
import yaml

from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.state import EdgeState

POLICY_TTL_S = 900          # deny everything on staler policy
SYNC_INTERVAL_S = 60        # etag check cadence
BUNDLE_SCHEMA = 2           # bump when the bundle's shape changes

log = logging.getLogger("nakagai.edge")


class BundleSchemaError(Exception):
    """The platform sent a bundle this edge cannot fully read."""


def apply_bundle(state: EdgeState, bundle: dict, etag: str) -> None:
    # The version gate goes first, before a single byte is written, so a bundle
    # this edge cannot fully read leaves the previous registry and cached
    # bundle exactly as they were.
    #
    # Why a gate at all: ConnectorSpec reaches the platform through PyPI, and
    # pydantic ignores unknown fields, so an edge one release ahead of the
    # platform would parse an older bundle cleanly and silently drop whatever
    # the new shape carries. Losing the capability map that builds a stop's
    # exit order would record every supervised position as unguarded while
    # every display still called it guarded: a live position keeping its stop
    # on screen and losing it in fact. Refusing beats half-understanding.
    version = bundle.get("schema_version")
    if version != BUNDLE_SCHEMA:
        # The missing-field case is the one that will actually happen, since a
        # platform older than the field simply does not send it, and "schema
        # None" is Python talking to itself at an owner who is reading this at
        # 2am wondering why their edge went quiet.
        sent = f"schema {version!r}" if version is not None else "no schema version"
        raise BundleSchemaError(
            f"platform sent {sent} on its policy bundle, this edge reads "
            f"schema {BUNDLE_SCHEMA}. Upgrade the platform, or pin an older "
            f"nakagai-edge. Refusing to act on a policy it cannot fully "
            f"read is the same rule the policy TTL enforces.")
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

    It rewrites meta whole, which is also what clears a schema_error: getting
    here at all means the platform answered with policy this edge can read, and
    that is exactly the condition an earlier refusal was waiting on.
    """
    state.meta_path.parent.mkdir(parents=True, exist_ok=True)
    state.meta_path.write_text(json.dumps({"etag": etag, "fetched_at": time.time()}))


def _record_schema_error(state: EdgeState, reason: str) -> None:
    """Note why the last bundle was refused, leaving fetched_at where it was.

    Not stamping is the whole mechanism: the TTL expires this edge within
    POLICY_TTL_S and every connector call refuses, rather than the edge acting
    on the older policy forever. The reason rides alongside so `edge status`
    and get_connector_status can say what actually happened, since "stale"
    alone reads as a network problem and sends the owner hunting the wrong
    fault.
    """
    doc = meta(state)
    doc["schema_error"] = reason
    state.meta_path.parent.mkdir(parents=True, exist_ok=True)
    state.meta_path.write_text(json.dumps(doc))


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


def schema_error(state: EdgeState) -> str:
    """Why the last bundle was refused, or "" when the last one was readable."""
    return str(meta(state).get("schema_error", "") or "")


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
    reads it around the call.

    The one exception to the silence is a bundle this edge cannot read. That is
    not a blip: retrying cannot fix it, only an owner can, so it is logged loud
    and recorded in meta."""
    try:
        etag, bundle = client.get_bundle(etag=meta(state).get("etag", ""))
    except (EdgeClientError, httpx.HTTPError, ValueError):
        return False
    try:
        if bundle is None:        # 304: policy unchanged, still authoritative
            _stamp(state, etag)   # carries no body to version-check, so no gate
            return False
        apply_bundle(state, bundle, etag)
        return True
    except BundleSchemaError as e:
        # Caught before the broad except below on purpose: everything else here
        # is noise the TTL handles quietly, and this one needs a human. Nothing
        # else would tell them; they would just watch every connector call start
        # refusing a quarter of an hour from now, with no reason given.
        log.error("policy sync refused: %s", e)
        _record_schema_error(state, str(e))
        return False
    except Exception:
        return False
