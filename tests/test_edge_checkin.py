"""End-to-end proof that an edge agent's heartbeat reaches the platform.

This is the test that was missing and let the edge check-in gap through: it
drives a check-in through the REAL edge MCP shim (create_edge_mcp's
agent_checkin tool) over a PlatformClient whose HTTP transport is bridged, in
process, into a real platform FastAPI app - the same custody split described
in docs/internal/EDGE.md, minus sockets. It proves the branch's whole premise:
an edge owner on autopilot's default 3.0 loss dial can report equity through
their edge agent and then arm, exactly like a direct MCP agent can.
"""

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

APPROVER = {"X-User": "chris@x.com", "X-Approver-Token": "approver-secret"}


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
    """Stub for PortfolioReporter: these tests exercise the check-in tool and
    the autopilot loss-dial gate, not the portfolio path, so a no-op
    stand-in keeps their intent unchanged."""

    async def snapshot_and_push(self):
        return {"connectors": []}


def _edge_mcp(edge_root, platform_client: PlatformClient):
    state = EdgeState(edge_root)
    state.save_agent("https://platform.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    hub = build_hub(state, platform_client)
    return create_edge_mcp(state, hub, platform_client, EdgeAudit(state), _Reporter())


@pytest.fixture
def platform_root(tmp_path):
    root = tmp_path / "platform"
    (root / "config").mkdir(parents=True)
    (root / "config" / "scan.yaml").write_text("{}\n")
    (root / "config" / "watchlist.yaml").write_text("symbols: [NVDA]\n")
    return root


@pytest.fixture
def platform_app(platform_root, monkeypatch):
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", "chris@x.com")
    return create_app(platform_root, with_mcp=False)


def _enroll(app, *, name="edge-shim") -> str:
    """Mint an agent token the way the web proxy would: approver-guarded
    POST /api/agents in mode=direct, so this test does not also have to drive
    the pairing-code exchange just to get a usable token."""
    r = TestClient(app).post(
        "/api/agents", json={"name": name, "mode": "direct"},
        headers={"Authorization": "Bearer api-secret", **APPROVER})
    assert r.status_code == 200
    return r.json()["token"]


async def test_edge_checkin_lands_where_the_platform_can_read_it(
        tmp_path, platform_root, platform_app):
    """The whole point of this branch: an equity report relayed by an EDGE
    agent, through its shim's own agent_checkin tool, must be readable by
    mandate.latest_equity_report() on the platform - exactly like a direct
    MCP agent's report already was before this fix, and was NOT for an edge
    agent (the defect this branch closes)."""
    from nakagai import mandate as m

    token = _enroll(platform_app)
    platform_client = _bridged_platform_client(platform_app, token)
    mcp = _edge_mcp(tmp_path / "edge", platform_client)

    result = await mcp.call_tool("agent_checkin", {
        "status": "scanning", "note": "watching NVDA",
        "account_equity": 100_000.0, "day_pnl": -1_500.0})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)
    assert out["ok"] is True

    report = m.latest_equity_report(platform_root)
    assert report is not None
    assert report["account_equity"] == 100_000.0
    assert report["day_pnl"] == -1_500.0

    # And it landed under the edge agent's own name/id, not a synthetic one.
    line = json.loads(
        (platform_root / "results" / "agent-activity.jsonl").read_text().splitlines()[-1])
    assert line["agent"] == "edge-shim"
    assert line["kind"] == "checkin"


async def test_edge_checkin_discards_half_an_equity_report(
        tmp_path, platform_root, platform_app):
    """Both fields or nothing, over the edge path too: equity alone has no
    baseline and would read as a flat day."""
    from nakagai import mandate as m

    token = _enroll(platform_app)
    platform_client = _bridged_platform_client(platform_app, token)
    mcp = _edge_mcp(tmp_path / "edge", platform_client)

    await mcp.call_tool("agent_checkin", {
        "status": "scanning", "account_equity": 100_000.0})   # no day_pnl

    assert m.latest_equity_report(platform_root) is None


async def test_edge_agent_can_arm_autopilot_after_checking_in_with_equity(
        tmp_path, platform_root, platform_app):
    """The acceptance test for the whole branch. With the loss dial at its
    3.0 default and no equity ever reported, arming autopilot 422s: the breaker
    would be on and unenforceable. Once an EDGE agent relays equity through
    its own shim's agent_checkin tool, the exact same arm call succeeds. This
    is what 'the breaker works in edge topology' actually means: before the
    fix, an edge owner had no way to ever clear the 422 short of zeroing the
    dial - precisely the failure this branch exists to eliminate.
    """
    from nakagai import mandate as m

    owner = TestClient(platform_app)
    owner_headers = {"Authorization": "Bearer api-secret"}

    doc = m.load_doc(platform_root)
    doc["preset"] = "autopilot"
    m.save_doc(platform_root, doc)

    # No equity report yet: the default 3.0% dial is on and unenforceable.
    before = owner.post("/api/mandate/arm", json={"armed": True}, headers=owner_headers)
    assert before.status_code == 422
    assert "no agent has reported account equity" in before.json()["detail"]

    token = _enroll(platform_app, name="edge-shim")
    platform_client = _bridged_platform_client(platform_app, token)
    mcp = _edge_mcp(tmp_path / "edge", platform_client)
    result = await mcp.call_tool("agent_checkin", {
        "status": "scanning", "account_equity": 50_000.0, "day_pnl": -200.0})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    assert json.loads(text)["ok"] is True

    after = owner.post("/api/mandate/arm", json={"armed": True}, headers=owner_headers)
    assert after.status_code == 200
    assert after.json()["autopilot_state"]["armed"] is True
