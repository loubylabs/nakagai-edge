"""Edge runtime surface: freshness gate, tool passthrough, hub wiring."""

import json
import time

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp, freshness_error
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import apply_bundle

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _state(tmp_path):
    s = EdgeState(tmp_path)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(s, {"bundle_version": "v1",
                     "connectors": {"connectors": []},
                     "signing_public_key": "k"}, "v1")
    return s


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the freshness gate and
    tool passthrough, not the portfolio path, so a no-op stand-in keeps their
    intent unchanged."""

    async def snapshot_and_push(self):
        return {"connectors": []}


async def test_call_connector_denied_on_stale_policy(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter())
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL
    result = await mcp.call_tool("call_connector",
                                 {"connector_id": "x", "tool": "get_quote"})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    assert "policy stale" in text


async def test_get_approval_denied_on_stale_policy(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter())
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL
    result = await mcp.call_tool("get_approval", {"approval_id": "anything"})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    assert "policy stale" in text


async def test_status_tool_works_even_stale(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter())
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)
    result = await mcp.call_tool("get_connector_status", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    assert "connectors" in json.loads(text)


def test_build_hub_exports_agent_token_env(tmp_path, monkeypatch):
    monkeypatch.delenv("NAKAGAI_AGENT_TOKEN", raising=False)
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    import os
    assert os.environ["NAKAGAI_AGENT_TOKEN"] == "nk_agent_t"
    assert hub.root == state.root


def test_freshness_error_is_json_with_is_error(tmp_path):
    doc = json.loads(freshness_error())
    assert doc["is_error"] is True and "policy stale" in doc["error"]


async def test_agent_checkin_forwards_to_the_platform_and_is_not_gated_on_staleness(
        tmp_path, monkeypatch):
    """agent_checkin talks straight to the platform rather than reading cached
    policy, so it must keep working past the freshness TTL that blocks every
    connector tool - the check-in is exactly what would let a reconnecting
    edge prove itself alive."""
    seen = []

    def handler(req):
        seen.append(json.loads(req.content))
        assert req.url.path == "/api/agent/checkin"
        return httpx.Response(200, json={"ok": True, "mandate": {"preset": "advisor"}})

    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter())
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL

    result = await mcp.call_tool("agent_checkin", {
        "status": "scanning", "note": "watching NVDA",
        "account_equity": 100_000.0, "day_pnl": -1_500.0})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    doc = json.loads(text)

    assert doc == {"ok": True, "mandate": {"preset": "advisor"}}
    assert seen == [{"status": "scanning", "note": "watching NVDA",
                     "account_equity": 100_000.0, "day_pnl": -1_500.0}]


async def test_agent_checkin_platform_error_returns_is_error_json(tmp_path):
    """The platform being unreachable or rejecting the token comes back as an
    ordinary is_error payload, never a traceback."""
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(401)))
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter())

    result = await mcp.call_tool("agent_checkin", {"status": "scanning"})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    doc = json.loads(text)

    assert doc["is_error"] is True
    assert "revoked" in doc["error"]


async def test_write_tool_edge_client_error_returns_is_error_json(tmp_path):
    """A write reaching RemoteApprovalQueue.enqueue while the platform is
    down/429/404 raises EdgeClientError from PlatformClient._check. That must
    be caught by _guarded's contract like every other handled exception, not
    escape past call_connector."""
    from nakagai_edge.edge.client import EdgeClientError

    state = _state(tmp_path)

    class BoomHub:
        async def call(self, connector_id, tool, args, **kw):
            raise EdgeClientError("platform rejected the agent token. Was it revoked?")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, BoomHub(), client, audit, _Reporter())
    result = await mcp.call_tool("call_connector",
                                 {"connector_id": "demo", "tool": "place_order",
                                  "args_json": "{}"})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    doc = json.loads(text)
    assert doc["is_error"] is True
    assert "revoked" in doc["error"]
