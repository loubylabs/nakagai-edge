"""An edge agent's live-channel loop reaches the platform: `nakagai-edge listen`
reads /api/agent/events and the send_message tool forwards to
/api/agent/message, through a real bridged ASGI stack, like test_edge_checkin."""

import json

import httpx
import pytest

pytest.importorskip("mcp")
fastapi = pytest.importorskip("fastapi")
pytest.importorskip("nakagai_platform")
from fastapi.testclient import TestClient  # noqa: E402

from nakagai_platform.api.app import create_app  # noqa: E402
from nakagai_edge.edge.audit import EdgeAudit  # noqa: E402
from nakagai_edge.edge.brake import Brake  # noqa: E402
from nakagai_edge.edge.client import PlatformClient  # noqa: E402
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp  # noqa: E402
from nakagai_edge.edge.state import EdgeState  # noqa: E402
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle  # noqa: E402

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
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    hub = build_hub(state, platform_client)
    audit = EdgeAudit(state)
    return create_edge_mcp(state, hub, platform_client, audit, _Reporter(),
                           Brake(state, hub, platform_client, audit))


def _tool_json(result) -> dict:
    text = result.content[0].text
    return json.loads(text)


def test_listener_emits_an_owner_message_through_the_real_stack(tmp_path, platform):
    """The listener against a real platform, not a scripted client: a message
    posted to the owner's pane comes back out as one emitted line.

    This replaces the old await_events MCP tool test. The tool is gone: once
    `nakagai-edge listen` owns reading the channel, a second read path over the
    same cursor-less endpoint would hand an agent every message twice.
    """
    from nakagai_edge.edge.listen import ChannelListener, CursorStore

    app, web, token = platform
    client = _bridged(app, token)
    root = tmp_path / "edge"
    # Resume from the beginning so the drain sees the message posted below;
    # a fresh listener would anchor to now and skip it, which is the point.
    CursorStore(root).save(0)

    web.post("/api/channel/message", json={"text": "owner says hi"},
             headers={**AUTH, "X-User": "chris@nakag.ai"})

    # Two passes, not one: a resumed cursor means the listener is catching up,
    # and a gap is buffered until a poll comes back empty so the trim can keep
    # its NEWEST messages. The second pass is that empty poll, which flushes.
    emitted = []
    passes = iter([True, True, False])
    ChannelListener(client, root, emit=emitted.append, sleep=lambda _s: None,
                    timeout_s=0).run(should_continue=lambda: next(passes, False))

    # Read the kind, the way the listener's own contract tells an agent to.
    # The platform publishes on its own account into the same stream: an edge
    # running a version the index has moved past gets an `edge_update` event,
    # which is correct behaviour and carries no `text`. Asserting over
    # everything that arrived made this test fail for as long as the platform's
    # lock lagged an edge release, which is a window that exists by design.
    messages = [e for e in emitted if e["kind"] == "owner_msg"]
    assert [e["text"] for e in messages] == ["owner says hi"]
    assert messages[0]["from"] == "chris@nakag.ai"
    assert CursorStore(root).load() == emitted[-1]["cursor"]


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
