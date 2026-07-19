"""An edge agent's live-channel loop reaches the platform: the edge shim's
await_events/send_message tools forward to /api/agent/events and
/api/agent/message through a real bridged ASGI stack, like test_edge_checkin."""

import json

import httpx
import pytest

pytest.importorskip("mcp")
fastapi = pytest.importorskip("fastapi")
pytest.importorskip("nakagai")
from fastapi.testclient import TestClient  # noqa: E402

from nakagai.api.app import create_app  # noqa: E402
from nakagai_edge.edge.audit import EdgeAudit  # noqa: E402
from nakagai_edge.edge.client import PlatformClient  # noqa: E402
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp  # noqa: E402
from nakagai_edge.edge.state import EdgeState  # noqa: E402
from nakagai_edge.edge.sync import apply_bundle  # noqa: E402

pytestmark = pytest.mark.anyio

APPROVER = {"X-User": "chris@nakag.ai", "X-Approver-Token": "approver-secret"}
AUTH = {"Authorization": "Bearer api-secret"}


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def platform(tmp_path, monkeypatch):
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", "chris@nakag.ai")
    app = create_app(tmp_path, with_mcp=False)
    client = TestClient(app)
    code = client.post("/api/agents", json={"name": "edge-claw"},
                       headers={**AUTH, **APPROVER}).json()["code"]
    token = client.post("/api/agents/pair", json={"code": code}).json()["token"]
    return app, client, token


def _bridged(app, token):
    bridge = TestClient(app)

    def handler(request: httpx.Request) -> httpx.Response:
        resp = bridge.request(request.method, str(request.url),
                              headers=dict(request.headers), content=request.content)
        return httpx.Response(resp.status_code, headers=resp.headers,
                              content=resp.content)

    return PlatformClient("https://platform.test", token,
                          transport=httpx.MockTransport(handler))


def test_platform_client_channel_round_trip(platform):
    app, web, token = platform
    client = _bridged(app, token)
    web.post("/api/channel/message", json={"text": "you there?"},
             headers={**AUTH, "X-User": "chris@nakag.ai"})
    got = client.await_events(after=0, timeout_s=0)
    assert got["events"][0]["body"]["text"] == "you there?"
    sent = client.send_message("here")
    assert sent["ok"] and sent["seq"] > got["cursor"]


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the live channel
    tools, not the portfolio path, so a no-op stand-in keeps their intent
    unchanged."""

    async def snapshot_and_push(self):
        return {"connectors": []}


def _edge_mcp(edge_root, platform_client: PlatformClient):
    state = EdgeState(edge_root)
    state.save_agent("https://platform.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    hub = build_hub(state, platform_client)
    return create_edge_mcp(state, hub, platform_client, EdgeAudit(state), _Reporter())


def _tool_json(result) -> dict:
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    return json.loads(text)


async def test_await_events_tool_returns_the_owner_message_and_a_cursor(
        tmp_path, platform):
    """The wrapper layer itself: cursor->after mapping and the json.dumps
    output shape, not just the PlatformClient method underneath it."""
    app, web, token = platform
    client = _bridged(app, token)
    mcp = _edge_mcp(tmp_path / "edge", client)

    web.post("/api/channel/message", json={"text": "owner says hi"},
             headers={**AUTH, "X-User": "chris@nakag.ai"})

    result = await mcp.call_tool("await_events", {"timeout_s": 0, "cursor": 0})
    out = _tool_json(result)
    assert out["events"][0]["body"]["text"] == "owner says hi"
    assert "cursor" in out


async def test_send_message_tool_returns_ok_and_seq(tmp_path, platform):
    app, web, token = platform
    client = _bridged(app, token)
    mcp = _edge_mcp(tmp_path / "edge", client)

    result = await mcp.call_tool("send_message", {"text": "edge agent here"})
    out = _tool_json(result)
    assert out == {"ok": True, "seq": out["seq"]}
    assert isinstance(out["seq"], int)


async def test_send_message_tool_reports_transport_failure_as_json(
        tmp_path, platform):
    """The try/except in the wrapper, not the underlying method: a broken
    platform must come back as {"is_error": true, ...}, never as a raised
    exception through call_tool."""
    app, web, token = platform

    def handler_500(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"platform is down")

    broken = PlatformClient("https://platform.test", token,
                            transport=httpx.MockTransport(handler_500))
    mcp = _edge_mcp(tmp_path / "edge", broken)

    result = await mcp.call_tool("send_message", {"text": "hello?"})
    out = _tool_json(result)
    assert out["is_error"] is True
