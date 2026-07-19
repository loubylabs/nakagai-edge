"""PlatformClient against httpx.MockTransport: no network, real wire shapes."""

import json

import httpx
import pytest

from nakagai_edge.edge.client import EdgeClientError, PlatformClient, pair
from nakagai_edge.edge.state import EdgeState


def _transport(handler):
    return httpx.MockTransport(handler)


def test_pair_stores_nothing_but_returns_token():
    def handler(req):
        assert req.url.path == "/api/agents/pair"
        assert json.loads(req.content) == {"code": "c123"}
        return httpx.Response(200, json={"ok": True, "agent_id": "ag1",
                                         "token": "nk_agent_t"})
    out = pair("https://api.test", "c123", transport=_transport(handler))
    assert out == {"ok": True, "agent_id": "ag1", "token": "nk_agent_t"}


def test_pair_error_raises_with_detail():
    def handler(req):
        return httpx.Response(403, json={"detail": "pairing failed"})
    with pytest.raises(EdgeClientError, match="pairing failed"):
        pair("https://api.test", "bad", transport=_transport(handler))


def test_bundle_etag_and_304():
    def handler(req):
        assert req.headers["authorization"] == "Bearer nk_agent_t"
        if req.headers.get("if-none-match") == "v1":
            return httpx.Response(304)
        return httpx.Response(200, json={"bundle_version": "v1", "connectors": {}},
                              headers={"ETag": "v1"})
    c = PlatformClient("https://api.test", "nk_agent_t", transport=_transport(handler))
    etag, bundle = c.get_bundle()
    assert etag == "v1" and bundle["bundle_version"] == "v1"
    etag2, none = c.get_bundle(etag="v1")
    assert etag2 == "v1" and none is None


def test_approval_round_trip_paths():
    seen = []
    def handler(req):
        seen.append((req.method, req.url.path))
        return httpx.Response(200, json={"ok": True, "approval_id": "a1",
                                         "status": "pending", "expires_at": 0})
    c = PlatformClient("https://api.test", "nk_agent_t", transport=_transport(handler))
    c.enqueue_approval("rh", "place_order", {"qty": 1})
    c.get_approval("a1")
    c.report_execution("a1", ok=True, result={"id": "42"})
    c.ship_audit([{"kind": "call"}])
    assert seen == [("POST", "/api/agent/approvals"),
                    ("GET", "/api/agent/approvals/a1"),
                    ("POST", "/api/agent/approvals/a1/execution"),
                    ("POST", "/api/agent/audit")]


def test_state_agent_json_round_trip(tmp_path):
    s = EdgeState(tmp_path)
    assert s.agent() is None
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    assert s.agent() == {"platform_url": "https://api.test",
                         "agent_id": "ag1", "token": "nk_agent_t"}
    import stat, os
    mode = stat.S_IMODE(os.stat(s.root / "agent.json").st_mode)
    assert mode == 0o600


def test_edge_root_dir_is_0700(tmp_path):
    root = tmp_path / "edge-root"
    s = EdgeState(root)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    import stat, os
    mode = stat.S_IMODE(os.stat(s.root).st_mode)
    assert mode == 0o700


def test_edge_root_dir_tightened_if_already_loose(tmp_path):
    root = tmp_path / "edge-root"
    root.mkdir(mode=0o755)
    import os
    os.chmod(root, 0o755)  # mkdir's mode arg is masked by umask; force it
    s = EdgeState(root)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    import stat
    mode = stat.S_IMODE(os.stat(s.root).st_mode)
    assert mode == 0o700
