"""Bundle sync: registry lands on disk, 304 refreshes freshness, staleness
fails closed."""

import time

import httpx
import yaml

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
import nakagai_edge.edge.sync as sync
from nakagai_edge.edge.sync import (apply_bundle, cached_bundle, meta, policy_fresh,
                               public_key, sync_once)

BUNDLE = {"bundle_version": "v1",
          "connectors": {"connectors": [{"id": "demo", "kind": "mcp-http",
                                         "url": "https://d.test/mcp", "enabled": True}]},
          "watchlist": ["SPY"], "mandate": {}, "strategy_configs": {},
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


NEW_BUNDLE = {"bundle_version": "v2",
              "connectors": {"connectors": [{"id": "other", "kind": "mcp-http",
                                             "url": "https://o.test/mcp", "enabled": True}]},
              "watchlist": ["QQQ"], "mandate": {}, "strategy_configs": {},
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
    "bundle_version": "v1",
    "connectors": {"connectors": [
        {"id": "nakagai-mcp", "name": "Nakagai MCP (this platform)",
         "kind": "mcp-http", "role": "signals", "url": "http://127.0.0.1:8321/mcp",
         "enabled": True},
        {"id": "demo", "kind": "mcp-http", "url": "https://d.test/mcp", "enabled": True},
    ]},
    "watchlist": ["SPY"], "mandate": {}, "strategy_configs": {},
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
