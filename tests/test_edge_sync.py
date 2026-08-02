"""Bundle sync: registry lands on disk, 304 refreshes freshness, staleness
fails closed, and a bundle this edge cannot fully read is refused outright."""

import logging
import time

import httpx
import pytest
import yaml

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
import nakagai_edge.edge.sync as sync
from nakagai_edge.edge.sync import (BUNDLE_SCHEMA, BundleSchemaError, apply_bundle,
                               cached_bundle, fetched_at, meta, policy_fresh,
                               public_key, schema_error, sync_once)

BUNDLE = {"bundle_version": "v1", "schema_version": 2,
          "connectors": {"connectors": [{"id": "demo", "kind": "mcp-http",
                                         "url": "https://d.test/mcp", "enabled": True}]},
          "mandate": {}, "strategy_configs": {},
          "signing_public_key": "PUBKEY"}


def test_apply_bundle_writes_registry_and_meta(tmp_path):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    reg = yaml.safe_load((tmp_path / "config" / "connectors.yaml").read_text())
    assert reg["connectors"][0]["id"] == "demo"
    assert cached_bundle(s)["bundle_version"] == "v1"
    assert meta(s)["etag"] == "v1"
    assert public_key(s) == "PUBKEY"


def test_policy_freshness_ttl(tmp_path, monkeypatch):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    assert policy_fresh(s, ttl_s=900)
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 901)
    assert not policy_fresh(s, ttl_s=900)


def test_sync_once_304_refreshes_freshness_only(tmp_path, monkeypatch):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    def handler(req):
        assert req.headers["if-none-match"] == "v1"
        return httpx.Response(304)
    c = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    before = meta(s)["fetched_at"]
    time.sleep(0.01)
    assert sync_once(s, c) is False
    assert meta(s)["fetched_at"] > before


def test_sync_once_error_leaves_cache(tmp_path):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    before = meta(s)["fetched_at"]
    def handler(req):
        raise httpx.ConnectError("down")
    c = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    assert sync_once(s, c) is False
    assert cached_bundle(s)["bundle_version"] == "v1"
    assert meta(s)["fetched_at"] == before


NEW_BUNDLE = {"bundle_version": "v2", "schema_version": 2,
              "connectors": {"connectors": [{"id": "other", "kind": "mcp-http",
                                             "url": "https://o.test/mcp", "enabled": True}]},
              "mandate": {}, "strategy_configs": {},
              "signing_public_key": "PUBKEY2"}


def test_sync_once_200_applies_new_bundle(tmp_path):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    def handler(req):
        assert req.headers["if-none-match"] == "v1"
        return httpx.Response(200, json=NEW_BUNDLE, headers={"etag": "v2"})
    c = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    assert sync_once(s, c) is True
    reg = yaml.safe_load((tmp_path / "config" / "connectors.yaml").read_text())
    assert reg["connectors"][0]["id"] == "other"
    assert cached_bundle(s)["bundle_version"] == "v2"
    assert meta(s)["etag"] == "v2"


def test_sync_once_non_json_200_leaves_cache(tmp_path):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    before = meta(s)["fetched_at"]
    def handler(req):
        return httpx.Response(200, text="<html>captive portal</html>")
    c = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    assert sync_once(s, c) is False
    assert cached_bundle(s)["bundle_version"] == "v1"
    assert meta(s)["etag"] == "v1"
    assert meta(s)["fetched_at"] == before


def test_sync_once_apply_failure_leaves_cache(tmp_path, monkeypatch):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    before = meta(s)["fetched_at"]
    def handler(req):
        return httpx.Response(200, json=NEW_BUNDLE, headers={"etag": "v2"})
    c = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    def boom(state, bundle, etag):
        raise OSError("disk full")
    monkeypatch.setattr(sync, "apply_bundle", boom)
    assert sync_once(s, c) is False
    assert cached_bundle(s)["bundle_version"] == "v1"
    assert meta(s)["etag"] == "v1"
    assert meta(s)["fetched_at"] == before


BUNDLE_WITH_NAKAGAI_MCP = {
    "bundle_version": "v1", "schema_version": 2,
    "connectors": {"connectors": [
        {"id": "nakagai-mcp", "name": "Nakagai MCP (this platform)",
         "kind": "mcp-http", "role": "signals", "url": "http://127.0.0.1:8321/mcp",
         "enabled": True},
        {"id": "demo", "kind": "mcp-http", "url": "https://d.test/mcp", "enabled": True},
    ]},
    "mandate": {}, "strategy_configs": {},
    "signing_public_key": "PUBKEY",
}


def test_apply_bundle_rewires_nakagai_mcp_edge_side_when_paired(tmp_path):
    s = EdgeState(tmp_path)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(s, BUNDLE_WITH_NAKAGAI_MCP, "v1")

    reg = yaml.safe_load((tmp_path / "config" / "connectors.yaml").read_text())
    by_id = {e["id"]: e for e in reg["connectors"]}
    assert by_id["nakagai-mcp"]["url"] == "https://api.test/mcp/"
    assert by_id["nakagai-mcp"]["auth"] == {"mode": "bearer",
                                            "token_env": "NAKAGAI_AGENT_TOKEN"}
    # other entries pass through untouched
    assert by_id["demo"]["url"] == "https://d.test/mcp"
    assert "auth" not in by_id["demo"]

    # the cached bundle.json keeps the platform's original (un-rewritten) shape
    cached = cached_bundle(s)
    cached_entries = {e["id"]: e for e in cached["connectors"]["connectors"]}
    assert cached_entries["nakagai-mcp"]["url"] == "http://127.0.0.1:8321/mcp"
    assert "auth" not in cached_entries["nakagai-mcp"]


def test_apply_bundle_unpaired_writes_registry_unchanged(tmp_path):
    s = EdgeState(tmp_path)  # no save_agent: unpaired, no agent.json
    apply_bundle(s, BUNDLE_WITH_NAKAGAI_MCP, "v1")  # must not raise

    reg = yaml.safe_load((tmp_path / "config" / "connectors.yaml").read_text())
    by_id = {e["id"]: e for e in reg["connectors"]}
    assert by_id["nakagai-mcp"]["url"] == "http://127.0.0.1:8321/mcp"
    assert "auth" not in by_id["nakagai-mcp"]


# ---- the schema version gate ---------------------------------------------
#
# ConnectorSpec reaches the platform through PyPI, and pydantic ignores
# unknown fields, so an edge one release ahead of the platform would read an
# older bundle cleanly and quietly lose whatever the new shape carries: the
# capability map that builds a stop's exit order, for one. Every supervised
# position would then be recorded as unguarded while every display still
# called it guarded. These tests pin the refusal, and pin that the refusal
# leaves fetched_at alone, because not stamping is the entire mechanism: the
# TTL then fails every connector call closed within POLICY_TTL_S.

UNVERSIONED_BUNDLE = {"bundle_version": "v2",   # what today's platform sends
                      "connectors": {"connectors": [{"id": "other", "kind": "mcp-http",
                                                     "url": "https://o.test/mcp",
                                                     "enabled": True}]},
                      "signing_public_key": "PUBKEY2"}

OLD_SCHEMA_BUNDLE = {**UNVERSIONED_BUNDLE, "schema_version": 1}


def _answering(*responses):
    """A platform that gives each response in turn, one per sync_once."""
    queue = list(responses)

    def handler(req):
        return queue.pop(0) if len(queue) > 1 else queue[0]
    return PlatformClient("https://api.test", "t",
                          transport=httpx.MockTransport(handler))


def _bundle_response(bundle, etag="v2"):
    return httpx.Response(200, json=bundle, headers={"etag": etag})


def test_apply_bundle_names_both_schemas_and_the_fix(tmp_path):
    """The message is what a confused owner reads at 2am, so it has to carry
    the whole diagnosis: what arrived, what this edge reads, what to do."""
    s = EdgeState(tmp_path)
    with pytest.raises(BundleSchemaError) as excinfo:
        apply_bundle(s, OLD_SCHEMA_BUNDLE, "v2")
    msg = str(excinfo.value)
    assert "1" in msg and str(BUNDLE_SCHEMA) in msg
    assert "upgrade the platform" in msg.lower()
    assert "nakagai-edge" in msg


def test_a_bundle_without_a_schema_version_is_refused(tmp_path):
    """A platform older than the field sends nothing at all, which is the case
    this whole gate exists for, so it must not read as "schema None"."""
    s = EdgeState(tmp_path)
    c = _answering(_bundle_response(UNVERSIONED_BUNDLE))
    assert sync_once(s, c) is False
    assert fetched_at(s) == 0.0
    assert "no schema version" in schema_error(s)
    assert "None" not in schema_error(s)
    assert not (tmp_path / "config" / "connectors.yaml").exists()
    assert cached_bundle(s) is None


def test_a_bundle_at_the_wrong_schema_version_is_refused(tmp_path):
    s = EdgeState(tmp_path)
    c = _answering(_bundle_response(OLD_SCHEMA_BUNDLE))
    assert sync_once(s, c) is False
    assert fetched_at(s) == 0.0
    assert "schema" in schema_error(s).lower()
    assert not (tmp_path / "config" / "connectors.yaml").exists()


def test_a_refused_bundle_leaves_the_previous_registry_untouched(tmp_path):
    """Half-applying is the one outcome worse than refusing: the registry on
    disk must still be the last policy this edge could actually read."""
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    before = fetched_at(s)
    assert sync_once(s, _answering(_bundle_response(OLD_SCHEMA_BUNDLE))) is False

    reg = yaml.safe_load((tmp_path / "config" / "connectors.yaml").read_text())
    assert reg["connectors"][0]["id"] == "demo"
    assert cached_bundle(s)["bundle_version"] == "v1"
    assert public_key(s) == "PUBKEY"
    assert meta(s)["etag"] == "v1"
    assert fetched_at(s) == before


def test_a_refused_bundle_is_logged_at_error(tmp_path, caplog):
    """sync_once swallows everything by design, so a network blip cannot kill
    the edge. This one case must not be swallowed: no amount of retrying fixes
    it, only an owner upgrading the platform or pinning an older edge does."""
    s = EdgeState(tmp_path)
    c = _answering(_bundle_response(OLD_SCHEMA_BUNDLE))
    with caplog.at_level(logging.ERROR, logger="nakagai.edge"):
        assert sync_once(s, c) is False
    assert [r for r in caplog.records
            if r.levelno >= logging.ERROR and "schema" in r.getMessage().lower()]


def test_a_successful_sync_clears_a_previous_schema_error(tmp_path):
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    c = _answering(_bundle_response(OLD_SCHEMA_BUNDLE),
                   _bundle_response(NEW_BUNDLE))
    assert sync_once(s, c) is False
    assert schema_error(s)
    assert sync_once(s, c) is True
    assert schema_error(s) == ""
    assert cached_bundle(s)["bundle_version"] == "v2"


def test_a_304_still_stamps_and_clears_a_schema_error(tmp_path):
    """A 304 carries no body to version-check and means the policy already on
    disk is still authoritative, so it keeps stamping. Breaking that would
    expire an edge whose platform simply had nothing new to say."""
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    before = fetched_at(s)
    c = _answering(_bundle_response(OLD_SCHEMA_BUNDLE), httpx.Response(304))
    assert sync_once(s, c) is False
    assert schema_error(s)
    assert fetched_at(s) == before

    time.sleep(0.01)
    assert sync_once(s, c) is False
    assert schema_error(s) == ""
    assert fetched_at(s) > before


def test_policy_goes_stale_after_a_refused_bundle(tmp_path, monkeypatch):
    """The payoff. A refused bundle never advances fetched_at, so the TTL that
    already fails a silent platform closed does the same here, and every
    connector call refuses within POLICY_TTL_S."""
    s = EdgeState(tmp_path)
    apply_bundle(s, BUNDLE, "v1")
    assert policy_fresh(s, ttl_s=900)
    c = _answering(_bundle_response(OLD_SCHEMA_BUNDLE))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 901)
    assert sync_once(s, c) is False
    assert not policy_fresh(s, ttl_s=900)
