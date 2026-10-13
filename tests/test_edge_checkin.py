"""End-to-end proof that an edge agent's heartbeat reaches the platform.

This is the test that was missing and let the edge check-in gap through: it
drives a check-in through the REAL edge MCP shim (create_edge_mcp's
agent_checkin tool) over a PlatformClient whose HTTP transport is bridged, in
process, into a real platform FastAPI app - the same custody split described
in docs/internal/EDGE.md, minus sockets. It proves an edge owner can report
equity through their edge agent, exactly like a direct MCP agent can.
"""

import json
import uuid

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


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _bridged_platform_client(app, token: str) -> PlatformClient:
    """A PlatformClient whose sync httpx transport routes straight into the
    given platform app's real ASGI stack, via the platform's own TestClient -
    no socket, but a real request through BearerAuth, require_agent, and the
    route handler itself. This is what makes the test prove the wiring, not
    just the shared function in isolation."""
    bridge = TestClient(app)

    def handler(request: httpx.Request) -> httpx.Response:
        resp = bridge.request(request.method, str(request.url),
                              headers=dict(request.headers), content=request.content)
        return httpx.Response(resp.status_code, headers=resp.headers, content=resp.content)

    return PlatformClient("https://platform.test", token,
                          transport=httpx.MockTransport(handler))


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the check-in tool,
    not the portfolio path, so a no-op stand-in keeps their intent unchanged."""

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


@pytest.fixture
def platform_root(tmp_path):
    root = tmp_path / "platform"
    (root / "config").mkdir(parents=True)
    (root / "config" / "scan.yaml").write_text("{}\n")
    return root


@pytest.fixture
def platform_app(platform_root, monkeypatch):
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", "chris@nakag.ai")
    return create_app(platform_root, with_mcp=False)


def _history(user: str):
    from nakagai_platform.activity_history import ActivityHistory
    from nakagai_platform.api.db import require_installed_database
    from nakagai_platform.api.tenancy import resolve_workspace_for_email

    database = require_installed_database()
    context = resolve_workspace_for_email(database, user)
    return ActivityHistory(database, context.wid)


def _enroll(app, *, name="edge-shim", user=APPROVER["X-User"]) -> str:
    """Mint an agent token the way the web proxy would: approver-guarded
    POST /api/agents in mode=direct, so this test does not also have to drive
    the pairing-code exchange just to get a usable token."""
    from nakagai_platform.api.db import Database

    Database.from_env().workspace_id(user.split("@", 1)[0], user)
    r = TestClient(app).post(
        "/api/agents", json={"name": name, "mode": "direct"},
        headers={"Authorization": "Bearer api-secret",
                 "X-User": user,
                 "X-Approver-Token": APPROVER["X-Approver-Token"]})
    assert r.status_code == 200
    return r.json()["token"]


async def test_edge_checkin_lands_where_the_platform_can_read_it(
        tmp_path, platform_root, platform_app):
    """An equity report relayed by an EDGE agent, through its shim's own
    agent_checkin tool, lands in the owner's activity history as the same
    check-in event a direct MCP agent's report produces."""
    owner = f"equity-owner-{uuid.uuid4().hex}@nakag.ai"
    token = _enroll(platform_app, user=owner)
    platform_client = _bridged_platform_client(platform_app, token)
    mcp = _edge_mcp(tmp_path / "edge", platform_client)

    result = await mcp.call_tool("agent_checkin", {
        "status": "scanning", "note": "watching NVDA",
        "account_equity": 100_000.0, "day_pnl": -1_500.0})
    text = result.content[0].text
    out = json.loads(text)
    assert out["ok"] is True

    # It landed under the edge agent's own name/id, not a synthetic one,
    # carrying the equity pair.
    event = _history(owner).read(limit=1, category="agents")[0]
    assert event.actor_label == "edge-shim"
    assert event.kind == "checkin"
    assert event.summary == "watching NVDA"
    assert event.details["status"] == "scanning"
    assert event.details["account_equity"] == 100_000.0
    assert event.details["day_pnl"] == -1_500.0


async def test_edge_checkin_discards_half_an_equity_report(
        tmp_path, platform_root, platform_app):
    """Both fields or nothing, over the edge path too: equity alone has no
    baseline and would read as a flat day. The beat itself is still recorded."""
    owner = f"half-report-owner-{uuid.uuid4().hex}@nakag.ai"
    token = _enroll(platform_app, user=owner)
    platform_client = _bridged_platform_client(platform_app, token)
    mcp = _edge_mcp(tmp_path / "edge", platform_client)

    await mcp.call_tool("agent_checkin", {
        "status": "scanning", "account_equity": 100_000.0})   # no day_pnl

    event = _history(owner).read(limit=1, category="agents")[0]
    assert event.kind == "checkin"
    assert event.details["status"] == "scanning"
    assert "account_equity" not in event.details
    assert "day_pnl" not in event.details
