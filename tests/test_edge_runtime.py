"""Edge runtime surface: freshness gate, tool passthrough, hub wiring."""

import json
import time

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
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
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
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
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
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
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
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
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
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
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

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
    boom_hub = BoomHub()
    mcp = create_edge_mcp(state, boom_hub, client, audit, _Reporter(),
                          Brake(state, boom_hub, client, audit))
    result = await mcp.call_tool("call_connector",
                                 {"connector_id": "demo", "tool": "place_order",
                                  "args_json": "{}"})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    doc = json.loads(text)
    assert doc["is_error"] is True
    assert "revoked" in doc["error"]


def _supervised_position(**over) -> dict:
    # expires_at is an epoch float, the way warrant.build_warrant_payload
    # writes it, because `guarded` reads it now: an ISO string is not a clock
    # anything in the edge can compare against.
    rec = {"position_id": "ap_1", "symbol": "AAPL", "connector_id": "demo",
           "account": "123", "direction": "long", "entry_price": 100.0,
           "stop": 95.0, "entry_qty": 10.0, "confirmed_qty": 10.0,
           "state": "armed",
           "warrant": {"trigger": {"type": "price_below", "level": 95.0},
                      "expires_at": 4_102_444_800.0}}
    rec.update(over)
    return rec


async def test_get_open_risk_reports_positions_with_live_prices(tmp_path):
    """The tool must convert _quotes()'s {symbol: full quote} shape to the
    bare {symbol: price} that open_risk() still expects - the boundary
    correction this task exists to land."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())

    class QuoteHub:
        async def call(self, connector_id, tool, args, **kw):
            assert tool == "get_quotes"
            return {"data": {"quotes": [{"symbol": "AAPL", "price": 92.0}]}}

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = QuoteHub()
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert out["armed"] is True
    assert out["disarmed_positions"] == []
    row = out["positions"][0]
    assert row["position_id"] == "ap_1"
    assert row["price"] == 92.0
    assert row["guarded"] is True
    assert out["portfolio_heat"] == row["open_risk"]


async def test_the_quote_read_goes_through_the_named_constant(tmp_path, monkeypatch):
    """The one tool name the brake's whole existence depends on lives in a
    single named place, because it has to be classified read-only in the
    guardrail config or the read is denied and the brake goes blind in
    silence."""
    import nakagai_edge.edge.runtime as runtime
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    monkeypatch.setattr(runtime, "QUOTE_TOOL", "get_market_quotes")

    asked = []

    class QuoteHub:
        async def call(self, connector_id, tool, args, **kw):
            asked.append(tool)
            return {"data": {"quotes": []}}

    await runtime._quotes(QuoteHub(), state, ["AAPL"])
    assert asked == ["get_market_quotes"]


async def test_get_open_risk_keeps_terminal_records_out_of_the_heat(tmp_path):
    """portfolio_heat answers "what if every stop hit at once", so a position
    that already closed must not sit in it forever. reconcile() skips TERMINAL,
    so nothing downstream ever zeroes such a record. The row itself stays
    visible: only the figure changes."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    record(state, _supervised_position(position_id="ap_2",
                                       state="outcome_unknown"))

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert {r["position_id"] for r in out["positions"]} == {"ap_1", "ap_2"}
    live = next(r for r in out["positions"] if r["position_id"] == "ap_1")
    assert live["open_risk"] > 0
    assert out["portfolio_heat"] == round(live["open_risk"], 2)


async def test_get_open_risk_survives_a_dead_quote_feed(tmp_path, monkeypatch):
    """A quote feed that fails outright (not just one connector inside it)
    must not hide the book: an agent needs to see its own open risk most
    urgently when things are degraded."""
    import nakagai_edge.edge.runtime as runtime
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())

    async def boom(hub, state, symbols):
        raise RuntimeError("no route to broker")

    monkeypatch.setattr(runtime, "_quotes", boom)

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert out["positions"][0]["position_id"] == "ap_1"
    assert out["positions"][0]["price"] is None


async def test_get_open_risk_reports_unguarded_under_a_global_disarm(tmp_path):
    """Fix round 1: guarded was computed from the ledger record alone (warrant
    + state == armed) and never consulted brake.armed()/disarmed_positions(),
    the two mechanisms `brake off` actually drives. After a global disarm
    nothing fires on a confirmed breach, so a still-True `guarded` here would
    tell the owner the opposite of the truth."""
    from nakagai_edge.edge.brake import set_local_disarm
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    set_local_disarm(state, all_positions=True)

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert out["armed"] is False
    assert out["positions"][0]["guarded"] is False


async def test_get_open_risk_is_self_consistent_after_a_per_position_disarm(tmp_path):
    """A position released with `brake off --position` must never show up
    both in disarmed_positions AND as guarded: true in the same payload -
    that combination is the exact self-contradiction the fix round closes."""
    from nakagai_edge.edge.brake import set_local_disarm
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    set_local_disarm(state, position_id="ap_1")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert out["disarmed_positions"] == ["ap_1"]
    guarded_ids = {r["position_id"] for r in out["positions"] if r["guarded"]}
    assert not (guarded_ids & set(out["disarmed_positions"]))
    assert out["positions"][0]["guarded"] is False
