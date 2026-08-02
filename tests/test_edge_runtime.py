"""Edge runtime surface: freshness gate, tool passthrough, hub wiring."""

import asyncio
import contextlib
import json
import logging
import time

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.capability import Capability
from nakagai_edge.config import ConnectorSpec, load_specs
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp, freshness_error
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle, sync_once
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _state(tmp_path):
    s = EdgeState(tmp_path)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(s, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
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
    doc = json.loads(text)
    assert "connectors" in doc
    assert doc["schema_error"] == ""


async def test_status_tool_carries_the_schema_error(tmp_path):
    """The one tool that answers on stale policy, so it is the one place an
    agent can learn WHY the rest is refusing. `policy_fresh: false` alone reads
    as a network problem; a refused bundle needs the owner, not a retry."""
    state = _state(tmp_path)
    client = PlatformClient(
        "https://api.test", "t",
        transport=httpx.MockTransport(lambda r: httpx.Response(
            200, json={"bundle_version": "v2", "connectors": {"connectors": []}},
            headers={"etag": "v2"})))
    assert sync_once(state, client) is False

    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    result = await mcp.call_tool("get_connector_status", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    doc = json.loads(text)
    assert "upgrade the platform" in doc["schema_error"].lower()
    assert str(BUNDLE_SCHEMA) in doc["schema_error"]


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


DEMO_SPEC = ConnectorSpec(
    id="demo", kind="mcp-http", role="broker",
    capabilities={"get_quote": Capability(
        tool="get_quotes", args={"symbols": "symbols"}, items=["quotes"],
        fields={"symbol": ["symbol"], "price": ["price"]})})


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
        def spec(self, connector_id):
            return DEMO_SPEC

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


# ---- the quote feed goes through each connector's own map -----------------
#
# The read the brake's whole existence depends on. Whatever tool a connector's
# `get_quote` entry names must still be classified read-only, or the edge's own
# guardrails deny it and the brake goes blind in silence; brake.tick's famine
# signal is what makes that audible.

QUOTE_SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})

QUOTE_PAYLOADS = {
    # The alien broker wraps nothing and spells every field its own way.
    "ticker": {"ticks": [{"tkr": "aapl", "last": "92.00"}]},
    # Robinhood nests its own {"data": ..., "guide": ...} envelope, which the
    # map roots at rather than anything in runtime.py peeling it.
    "get_quotes": {
        "data": {"quotes": [{"symbol": "MSFT",
                             "last_trade_price": "310.50"}]},
        "guide": "ignore me"},
}


class MapQuoteHub:
    """Answers through the real connector maps, keyed by the tool asked for."""

    def __init__(self, payloads=None, fail=frozenset()):
        self.calls = []
        self.payloads = QUOTE_PAYLOADS if payloads is None else payloads
        self.fail = fail

    def spec(self, connector_id):
        return QUOTE_SPECS[connector_id]

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        if connector_id in self.fail:
            raise RuntimeError(f"{connector_id} is not answering")
        return {"data": self.payloads[tool]}


def _two_brokers(tmp_path):
    """One supervised position on each connector, distinct symbols."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position(connector_id="alien-broker",
                                       symbol="AAPL", account="AL-1"))
    record(state, _supervised_position(position_id="ap_2",
                                       connector_id="robinhood-trading",
                                       symbol="MSFT", account="463605220"))
    return state


async def test_quotes_dial_each_connectors_own_quote_tool(tmp_path):
    """`ticker` with `tickers` for the alien broker, `get_quotes` with
    `symbols` for Robinhood, and the same canonical quote shape out of both.

    The tool name and the argument key both come from the map, and both are
    asserted: a broker handed the right tool under the wrong argument key
    answers about nothing at all, and the brake then goes quiet in exactly the
    way a working brake looks from outside.
    """
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub()

    quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    by_tool = {tool: args for _, tool, args in hub.calls}
    assert by_tool == {"ticker": {"tickers": ["AAPL"]},
                       "get_quotes": {"symbols": ["MSFT"]}}
    assert set(quotes) == {"AAPL", "MSFT"}
    assert quotes["AAPL"]["price"] == 92.00
    assert quotes["MSFT"]["price"] == 310.50
    for quote in quotes.values():
        # The full normalized shape, not a bare price: usable() needs the
        # receipt time and the book to judge freshness and spread.
        assert set(quote) == {"price", "bid", "ask", "ts"}
        assert quote["ts"] > 0


async def test_a_connector_that_will_not_answer_costs_only_its_own_symbols(
        tmp_path):
    """No quote means no tick for that symbol, which means no fire. That is
    the safe direction but not a quiet one: the missing symbol is what
    brake.tick counts toward its famine signal. The other connector's
    positions must still be priced."""
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub(fail={"alien-broker"})

    quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    assert set(quotes) == {"MSFT"}, "the sweep survives one dead connector"


async def test_a_connector_declaring_no_get_quote_is_skipped_with_a_reason(
        tmp_path, caplog):
    """A connector with no `get_quote` entry cannot be asked to guess one. It
    is skipped by name in the log, not dialed on a hopeful tool name, and it
    does not take the other connectors' quotes down with it."""
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub()
    hub.spec = lambda cid: (
        ConnectorSpec(id=cid, kind="mcp-http", role="broker")
        if cid == "alien-broker" else QUOTE_SPECS[cid])

    with caplog.at_level(logging.WARNING, logger="nakagai.edge"):
        quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    assert set(quotes) == {"MSFT"}
    assert [c[0] for c in hub.calls] == ["robinhood-trading"], (
        "an unmapped connector must not be dialed on a guessed tool name")
    assert "alien-broker" in caplog.text and "get_quote" in caplog.text


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


async def test_get_open_risk_says_so_when_the_ledger_was_lost(tmp_path):
    """An empty positions list reads as "you have no open risk". After a
    corrupt ledger it means "the brake forgot every position it was watching",
    which is the opposite, so the tool has to say which one it is."""
    state = _state(tmp_path)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text("{not json")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
    out = json.loads(text)

    assert out["positions"] == []
    assert "supervised.corrupt.json" in out["ledger_fault"]


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


# ---- the tool schemas the platform cannot fetch for itself ----------------
#
# The platform never dials a broker, so a connector's tool list and JSON
# schemas reach it only if this edge ships them. They ride the connector
# report, not the agent-facing status tools: `list_connectors` is read by an
# agent that pays tokens for every byte, and `list_connector_tools` already
# serves the schemas per connector, on demand.

SPEC = {"id": "demo", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp/", "enabled": True}

LISTED_TOOL = {
    "name": "get_account",
    "title": "Get account",
    "description": "Balances for one account",
    "inputSchema": {"type": "object",
                    "properties": {"account": {"type": "string"}}},
    "outputSchema": {"type": "object"},
    "annotations": {"readOnlyHint": True},
    "_meta": {"noise": True},
}


def test_to_dict_carries_each_tools_name_description_and_schema():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[LISTED_TOOL]).to_dict(with_tools=True)
    assert row["tool_count"] == 1
    # Exactly three keys: outputSchema, annotations and _meta are the
    # downstream's business, and shipping them would put bytes on the wire
    # every cycle that nothing upstream reads.
    assert row["tools"] == [{
        "name": "get_account",
        "description": "Balances for one account",
        "inputSchema": {"type": "object",
                        "properties": {"account": {"type": "string"}}}}]


def test_to_dict_leaves_the_schemas_out_unless_they_are_asked_for():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[LISTED_TOOL]).to_dict()
    assert "tools" not in row and row["tool_count"] == 1


def test_to_dict_of_a_connector_that_never_connected_still_has_a_tool_list():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC)).to_dict(with_tools=True)
    assert row["status"] == "disconnected"
    assert row["tools"] == [] and row["tool_count"] == 0


def test_a_tool_that_publishes_no_description_or_schema_still_reports_both_keys():
    # A predictable payload: the platform reads three keys off every entry
    # rather than three keys off some of them.
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[{"name": "ping"}]).to_dict(with_tools=True)
    assert row["tools"] == [{"name": "ping", "description": "", "inputSchema": {}}]


async def test_the_status_tools_do_not_hand_the_agent_every_schema(tmp_path):
    """`list_connectors` answers "which broker can be asked what", and an
    agent pays tokens for every byte of it. Thirty JSON schemas belong in the
    connector report, and in `list_connector_tools` when actually asked for."""
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    for tool in ("list_connectors", "get_connector_status"):
        result = await mcp.call_tool(tool, {})
        text = result[0][0].text if isinstance(result, tuple) else result.content[0].text
        assert "inputSchema" not in text, f"{tool} shipped downstream schemas"


async def test_the_syncer_survives_a_report_that_raises(tmp_path, monkeypatch, caplog):
    """The connector report is best-effort and the syncer runs forever: one
    uncaught exception out of it would kill every later sync too, not just
    this cycle's report. The schema guard runs inside that call, so it must
    not become a new way for the loop to die."""
    import nakagai_edge.edge.runtime as runtime
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    calls = {"bundle": 0, "report": 0}

    def handler(req):
        if req.url.path == "/api/agent/bundle":
            calls["bundle"] += 1
            return httpx.Response(200, json={"bundle_version": "v1",
                                             "schema_version": BUNDLE_SCHEMA,
                                             "connectors": {"connectors": []},
                                             "signing_public_key": "k"},
                                  headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))

    def boom(connectors):
        calls["report"] += 1
        raise RuntimeError("the schema guard blew up")

    client.report_connectors = boom
    hub = build_hub(state, client)
    audit = EdgeAudit(state)

    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    tasks = await runtime._loops(state, hub, client, audit, _Reporter(),
                                 Brake(state, hub, client, audit))
    for _ in range(200):
        if calls["report"] >= 2:
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t

    assert calls["report"] >= 2, "the loop died on the first raising report"
    assert calls["bundle"] >= 2, "sync_once stopped running too, not just the report"
    assert any("connector" in r.message.lower() for r in caplog.records)
    await hub.aclose()
