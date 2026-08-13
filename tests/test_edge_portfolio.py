"""The snapshot loop's assembly and push. Fixture-driven: a fake hub, no
network, no broker. The numbers only ever originate from the edge's own
broker calls; an agent poke carries no data.

Every assembly test runs against two brokers that agree on nothing: Robinhood,
which wraps every response in its own envelope, and the alien fixture, which
wraps nothing and spells account, symbol and quantity differently. A sweep that
still had a Robinhood field name baked into it would pass the first and fail
the second."""

import asyncio

import httpx
import pytest
import yaml

from nakagai_edge.config import ConnectorSpec, load_specs
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.portfolio import (
    PORTFOLIO_INTERVAL_S, REFRESH_MIN_INTERVAL_S, PortfolioReporter,
    broker_specs, connector_snapshot, mark_guarded, tiered_accounts)
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import record
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio

SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _spec(**over):
    base = dict(id="robinhood-trading", kind="mcp-http", role="broker",
                url="https://x.test/mcp", enabled=True,
                capabilities=ROBINHOOD_CONNECTOR["capabilities"],
                guardrails={"tools": {"allow": ["get_*"]},
                            "read_only_tools": ["get_*"],
                            "accounts": {"allow": ["463605220"],
                                         "read": ["5QU41901"]}})
    base.update(over)
    return ConnectorSpec(**base)


ACCOUNTS = {"accounts": [
    {"account_number": "5QU41901", "type": "margin", "is_default": True},
    {"account_number": "463605220", "type": "cash", "nickname": "Agentic"},
]}
# What list_accounts extracts from ACCOUNTS. tiered_accounts is handed the
# normalized rows, not the broker's, so it keys off `account`.
LISTED = [{"account": "5QU41901", "type": "margin"},
          {"account": "463605220", "type": "cash", "nickname": "Agentic"}]
PORTFOLIO = {"total_value": "1000", "cash": "1000", "currency": "USD",
             "buying_power": {"buying_power": "1000.0000"}}
POSITIONS = {"positions": [{"symbol": "SPY", "quantity": "10",
                            "average_buy_price": "500.00", "type": "equity"}]}


class FakeHub:
    """hub.call's contract: {"data": <downstream>}, raising on refusal.
    Robinhood nests its own {"data": ..., "guide": ...} envelope, so the
    canned responses here nest one too. Nothing in portfolio.py peels it any
    more: the map roots every Robinhood capability at `data`, and these
    responses prove the map is actually being followed."""

    def __init__(self, responses):
        self.account_key = "ag1"
        self.responses = responses
        self.calls = []

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        key = (tool, str(args.get("account_number", "")))
        out = self.responses[key]
        if isinstance(out, Exception):
            raise out
        return {"connector": connector_id, "tool": tool, "is_write": False,
                "is_error": False, "data": {"data": out, "guide": "ignore me"}}


class FlatHub:
    """The same contract, keyed by tool alone: one canned response per tool,
    handed back as hub.call's `data` and nothing more. What a test puts in is
    exactly what the map has to read, which is what makes it useful both for
    the alien broker (no envelope at all) and for pinning what happens when a
    Robinhood-shaped payload is missing the node its map names."""

    def __init__(self, responses):
        self.account_key = "ag1"
        self.responses, self.calls = responses, []

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        return {"data": self.responses[tool]}


def _hub_ok():
    return FakeHub({
        ("get_accounts", ""): ACCOUNTS,
        ("get_portfolio", "463605220"): PORTFOLIO,
        ("get_equity_positions", "463605220"): POSITIONS,
        ("get_portfolio", "5QU41901"): {"total_value": "2500", "cash": "100"},
        ("get_equity_positions", "5QU41901"): {"positions": []},
    })


# ---- tiering -------------------------------------------------------------

def test_tiered_accounts_unions_both_tiers_with_their_labels():
    got = tiered_accounts(_spec(), LISTED)
    by_num = {a["account"]: tier for a, tier in got}
    assert by_num == {"463605220": "full", "5QU41901": "read"}


def test_no_account_lists_means_every_listed_account_at_full_tier():
    s = _spec(guardrails={"tools": {"allow": ["get_*"]},
                          "read_only_tools": ["get_*"]})
    got = tiered_accounts(s, LISTED)
    assert [(a["account"], t) for a, t in got] == [
        ("5QU41901", "full"), ("463605220", "full")]


def test_a_configured_account_the_broker_did_not_list_is_still_fetched():
    got = tiered_accounts(_spec(), [])
    assert {a["account"] for a, _ in got} == {"463605220", "5QU41901"}


# ---- assembly ------------------------------------------------------------

async def test_connector_snapshot_carries_totals_positions_and_tiers():
    entry = await connector_snapshot(_hub_ok(), _spec())
    assert entry["id"] == "robinhood-trading" and entry["error"] == ""
    by_num = {a["account_number"]: a for a in entry["accounts"]}
    agentic = by_num["463605220"]
    assert agentic["tier"] == "full" and agentic["nickname"] == "Agentic"
    assert agentic["portfolio"]["total_value"] == "1000"
    assert agentic["positions"][0]["symbol"] == "SPY"
    margin = by_num["5QU41901"]
    assert margin["tier"] == "read"
    assert margin["portfolio"]["total_value"] == "2500"
    assert margin["positions"] == []


async def test_one_accounts_failure_does_not_blank_its_siblings():
    hub = _hub_ok()
    hub.responses[("get_portfolio", "5QU41901")] = RuntimeError("broker hiccup")
    entry = await connector_snapshot(hub, _spec())
    by_num = {a["account_number"]: a for a in entry["accounts"]}
    assert "broker hiccup" in by_num["5QU41901"]["error"]
    assert by_num["5QU41901"]["portfolio"] == {}
    assert by_num["463605220"]["portfolio"]["total_value"] == "1000"
    assert by_num["463605220"]["error"] == ""


async def test_a_dead_account_list_degrades_to_a_connector_level_error():
    hub = FakeHub({("get_accounts", ""): RuntimeError("token expired")})
    entry = await connector_snapshot(hub, _spec())
    assert "token expired" in entry["error"]
    assert entry["accounts"] == []


def _alien_hub(**over):
    responses = {
        "accounts_list": {"accounts": [{"acct": "AL-1", "label": "Main",
                                        "kind": "margin"}]},
        "balances": {"net_liq": "104238.55"},
        "holdings": {"holdings": [{"ticker": "aapl", "qty": "25",
                                   "cost": "187.20"}]}}
    responses.update(over)
    return FlatHub(responses)


async def test_snapshot_dials_the_alien_brokers_own_tool_names():
    hub = _alien_hub()
    entry = await connector_snapshot(hub, SPECS["alien-broker"])
    assert [c[1] for c in hub.calls] == ["accounts_list", "balances", "holdings"]
    assert entry["accounts"][0]["account_number"] == "AL-1"
    assert entry["accounts"][0]["nickname"] == "Main"


async def test_snapshot_normalizes_symbol_and_quantity_onto_raw_rows():
    entry = await connector_snapshot(_alien_hub(), SPECS["alien-broker"])
    row = entry["accounts"][0]["positions"][0]
    assert row["symbol"] == "AAPL"       # normalized, overwrites nothing here
    assert row["quantity"] == 25.0
    assert row["cost"] == "187.20"       # raw fields survive untouched


async def test_snapshot_keeps_broker_figures_verbatim():
    hub = _alien_hub(balances={"net_liq": "104,238.55", "denom": "USD"},
                     holdings={"holdings": []})
    entry = await connector_snapshot(hub, SPECS["alien-broker"])
    assert entry["accounts"][0]["portfolio"] == {"net_liq": "104,238.55",
                                                 "denom": "USD"}


async def test_a_connector_missing_a_capability_reports_the_error():
    spec = SPECS["alien-broker"].model_copy(deep=True)
    del spec.capabilities["list_accounts"]
    entry = await connector_snapshot(FlatHub({}), spec)
    assert "list_accounts" in entry["error"]
    assert entry["accounts"] == []


async def test_a_position_whose_quantity_is_unreadable_keeps_its_symbol():
    """The row stays, named, with no `quantity`. Dropping it would read as a
    position that closed, and the brake releases what it cannot see: a live
    position would silently lose its stop. supervision.unreadable() exists to
    tell those two apart, and this is the shape it reads."""
    hub = _alien_hub(holdings={"holdings": [
        {"ticker": "spy", "qty": "many"},
        {"ticker": "aapl", "qty": "25"}]})
    entry = await connector_snapshot(hub, SPECS["alien-broker"])
    rows = entry["accounts"][0]["positions"]
    assert [r["symbol"] for r in rows] == ["SPY", "AAPL"]
    assert "quantity" not in rows[0]
    assert rows[1]["quantity"] == 25.0


async def test_an_unreadable_row_does_not_shift_its_neighbours_quantity():
    """The zip trap. `extract` drops what it cannot read, so pairing its output
    against the raw list positionally would hand row 2's quantity to row 1 and
    put a real stop on the wrong symbol. Rows are read one at a time."""
    hub = _alien_hub(holdings={"holdings": [
        {"ticker": "spy"},                      # no qty at all
        {"ticker": "aapl", "qty": "25"},
        {"ticker": "msft", "qty": "7"}]})
    entry = await connector_snapshot(hub, SPECS["alien-broker"])
    rows = entry["accounts"][0]["positions"]
    assert [(r["symbol"], r.get("quantity")) for r in rows] == [
        ("SPY", None), ("AAPL", 25.0), ("MSFT", 7.0)]


def _untiered_alien():
    """The alien spec with its account lists cleared, so tiered_accounts takes
    the no-restriction path and reports whatever the broker listed. That is the
    only path on which an account the broker lists reaches the document by
    itself, so it is the path an unreadable one can vanish from."""
    spec = SPECS["alien-broker"].model_copy(deep=True)
    spec.guardrails.accounts.allow = []
    spec.guardrails.accounts.read = []
    return spec


async def test_an_account_whose_identifier_will_not_read_stays_visible():
    """`account` is a required field, so `extract` would drop this row and the
    account would simply be gone, taking its balances and every position under
    it with it, with nothing on any display saying so. Same rule as an
    unreadable position: present-but-unparseable is not absent, and the owner
    has to be able to see the difference."""
    hub = _alien_hub(accounts_list={"accounts": [
        {"label": "Ghost"},                     # no `acct` at all
        {"acct": "AL-1", "label": "Main"}]})
    entry = await connector_snapshot(hub, _untiered_alien())
    ghost, main = entry["accounts"]
    assert ghost["account_number"] == ""        # nothing invented
    assert ghost["nickname"] == "Ghost"         # what did read, survives
    assert ghost["error"]                       # and the owner is told
    assert ghost["portfolio"] == {} and ghost["positions"] == []
    assert main["account_number"] == "AL-1" and main["error"] == ""
    # The ghost is never dialed. There is nothing to ask about, and a made-up
    # identifier would be asking about somebody else's account.
    assert [c[1] for c in hub.calls] == ["accounts_list", "balances", "holdings"]


async def test_the_tiered_path_is_unaffected_by_an_unreadable_account():
    """With account lists configured the guardrails enumerate the accounts and
    the broker's list only enriches them, so an account the owner never listed
    is out of scope whether or not its identifier reads. It must not appear,
    and it must not displace the configured one."""
    hub = _alien_hub(accounts_list={"accounts": [
        {"label": "Ghost"},
        {"acct": "AL-1", "label": "Main"}]})
    entry = await connector_snapshot(hub, SPECS["alien-broker"])   # allows AL-1
    assert [a["account_number"] for a in entry["accounts"]] == ["AL-1"]
    assert entry["accounts"][0]["nickname"] == "Main"
    assert entry["accounts"][0]["error"] == ""


async def test_a_balance_payload_missing_the_mapped_root_is_no_figures():
    """The map says Robinhood's figures live under `data`. A payload without
    that node is not the shape the map describes, so the account reports no
    figures rather than handing the web whatever else the envelope held. Blank
    money tiles read as broken; a `guide` string sitting where the figures
    belong reads as figures. The account itself still reports."""
    hub = FlatHub({
        "get_accounts": {"data": {"accounts": [{"account_number": "463605220"}]}},
        "get_portfolio": {"guide": "the figures are elsewhere today"},
        "get_equity_positions": {"data": {"positions": []}}})
    spec = _spec(guardrails={"tools": {"allow": ["get_*"]},
                             "read_only_tools": ["get_*"]})
    row = (await connector_snapshot(hub, spec))["accounts"][0]
    assert row["account_number"] == "463605220"
    assert row["portfolio"] == {}
    assert row["error"] == ""


# ---- the guarded marker ---------------------------------------------------

EXPIRY = 4_102_444_800.0        # 2100-01-01, an epoch float like a real warrant


def _supervised(edge_state, **ledger_fields):
    rec = {"position_id": "ap_1", "symbol": "SPY", "connector_id": "robinhood-trading",
           "account": "463605220", "direction": "long", "entry_price": 480.0,
           "stop": 460.0, "entry_qty": 10.0, "confirmed_qty": 10.0,
           "state": "armed", "warrant": {"trigger": {"type": "price_below",
                                                      "level": 460.0},
                                         "expires_at": EXPIRY}}
    rec.update(ledger_fields)
    record(edge_state, rec)


def _connectors_with_one_position():
    return [{"id": "robinhood-trading", "error": "", "accounts": [
        {"account_number": "463605220", "positions": [{"symbol": "SPY"}]}]}]


def test_mark_guarded_tags_an_armed_warranted_position(tmp_path):
    state = EdgeState(tmp_path)
    _supervised(state)
    out = mark_guarded(state, _connectors_with_one_position())
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is True


def test_mark_guarded_is_false_with_no_ledger_entry(tmp_path):
    state = EdgeState(tmp_path)
    out = mark_guarded(state, _connectors_with_one_position())
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is False


def test_mark_guarded_is_false_for_an_unguarded_ledger_entry(tmp_path):
    """A record without a warrant must not read as guarded: this marker's
    whole purpose is to make that gap visible, not hide it."""
    state = EdgeState(tmp_path)
    _supervised(state, warrant=None, state="unguarded")
    out = mark_guarded(state, _connectors_with_one_position())
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is False


def test_mark_guarded_is_a_no_op_on_an_empty_ledger(tmp_path):
    state = EdgeState(tmp_path)
    connectors = [{"id": "robinhood-trading", "error": "", "accounts": []}]
    assert mark_guarded(state, connectors) == connectors


def test_mark_guarded_tags_false_under_a_global_disarm(tmp_path):
    """Fix round 1: an armed, warranted record must still read unguarded once
    the owner has disarmed the brake, or this display field lies about the
    one thing it exists to tell the owner."""
    state = EdgeState(tmp_path)
    _supervised(state)
    out = mark_guarded(state, _connectors_with_one_position(), brake_armed=False)
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is False


def test_mark_guarded_tags_false_once_the_warrant_has_expired(tmp_path):
    """A warrant nobody renewed is a brake that will not fire, so the marker
    on the owner's Portfolio page has to go dark with it."""
    state = EdgeState(tmp_path)
    _supervised(state)
    out = mark_guarded(state, _connectors_with_one_position(), now=EXPIRY + 1)
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is False


def test_mark_guarded_checks_expiry_against_the_real_clock_by_default(tmp_path):
    """No `now=`, exactly as snapshot_and_push calls it. The test above passes
    one explicitly and so cannot notice if mark_guarded stops reading the
    clock: is_guarded(now=None) is permissive by design, so deleting that one
    default line puts the expired-warrant marker back on the owner's Portfolio
    page with the suite still green."""
    state = EdgeState(tmp_path)
    _supervised(state, warrant={"trigger": {"type": "price_below",
                                            "level": 460.0},
                                "expires_at": 1_000_000_000.0})   # 2001
    out = mark_guarded(state, _connectors_with_one_position())
    assert out[0]["accounts"][0]["positions"][0]["guarded"] is False


# ---- spec discovery ------------------------------------------------------

def test_broker_specs_reads_only_enabled_mcp_brokers(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "connectors.yaml").write_text(yaml.safe_dump(
        {"connectors": [
            {"id": "robinhood-trading", "kind": "mcp-http", "role": "broker",
             "url": "https://x.test/mcp", "enabled": True},
            {"id": "demo-broker", "kind": "mcp-stdio", "role": "broker",
             "command": "python", "enabled": True},
            {"id": "disabled-broker", "kind": "mcp-http", "role": "broker",
             "url": "https://y.test/mcp", "enabled": False},
            {"id": "alpaca-data", "kind": "data", "role": "data", "enabled": True},
        ]}))
    assert [s.id for s in broker_specs(tmp_path)] == [
        "robinhood-trading", "demo-broker"]


def test_broker_specs_with_no_registry_is_empty(tmp_path):
    assert broker_specs(tmp_path) == []


# ---- the reporter: one path, rate-limited, pushes to the platform ---------

def _reporter(tmp_path, hub, handler):
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(yaml.safe_dump(
        {"connectors": [{
            "id": "robinhood-trading", "kind": "mcp-http", "role": "broker",
            "url": "https://x.test/mcp", "enabled": True,
            # Through the file and back, so the reporter proves the sweep works
            # off a parsed registry and not off a spec a test hand-built.
            "capabilities": ROBINHOOD_CONNECTOR["capabilities"],
            "guardrails": {"tools": {"allow": ["get_*"]},
                           "read_only_tools": ["get_*"],
                           "accounts": {"allow": ["463605220"],
                                        "read": ["5QU41901"]}}}]}))
    client = PlatformClient("http://platform.test", "nk_agent_x",
                            transport=httpx.MockTransport(handler))
    return PortfolioReporter(EdgeState(tmp_path), hub, client)


async def test_snapshot_and_push_posts_the_document(tmp_path):
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["body"] = request.read().decode()
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    reporter = _reporter(tmp_path, _hub_ok(), handler)
    doc = await reporter.snapshot_and_push()
    assert seen["path"] == "/api/agent/portfolio"
    assert "463605220" in seen["body"] and "5QU41901" in seen["body"]
    assert doc["connectors"][0]["id"] == "robinhood-trading"


async def test_two_pokes_inside_the_window_are_one_broker_sweep(tmp_path):
    hub = _hub_ok()
    reporter = _reporter(tmp_path, hub,
                         lambda r: httpx.Response(200, json={"ok": True}))
    first = await reporter.snapshot_and_push()
    calls_after_first = len(hub.calls)
    second = await reporter.snapshot_and_push()
    assert len(hub.calls) == calls_after_first   # no second sweep
    assert second == first                        # the fresh-enough snapshot back


async def test_a_down_platform_does_not_lose_the_snapshot(tmp_path):
    """Best-effort push: the sweep result is still returned (the refresh tool
    hands it to the agent) even when the POST fails."""
    def handler(request):
        return httpx.Response(503, json={"detail": "down"})

    reporter = _reporter(tmp_path, _hub_ok(), handler)
    doc = await reporter.snapshot_and_push()
    assert doc["connectors"][0]["accounts"]      # figures survived the 503


def test_the_report_client_method_hits_the_agent_portfolio_route():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    c = PlatformClient("http://platform.test", "nk_agent_x",
                       transport=httpx.MockTransport(handler))
    out = c.report_portfolio([{"id": "robinhood-trading", "error": "",
                               "accounts": []}])
    assert out["ok"] is True
    assert seen["path"] == "/api/agent/portfolio"
    assert seen["auth"] == "Bearer nk_agent_x"
