"""The seven semantic tools: one vocabulary an agent learns once, any broker.

Each tool picks the connector, resolves the call through that connector's own
map, and then goes down the SAME `_guarded` path `call_connector` uses. The
capability layer decides WHAT to call; it never decides WHETHER it is allowed.

Both halves are pinned below: the identical canonical shape out of two brokers
that agree on nothing, and the fact that every refusal still comes from the
guardrails rather than from this layer. The account-inference tests are the
sharp end of the second half. Inference FILLS an argument so `check_accounts`
can then evaluate it; a version that inferred a little more freely would be
choosing an account the owner never authorized, which is exactly what
`check_accounts` exists to stop.
"""

import json
import time

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.config import load_specs
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.candidate import CandidateWakeScope
from nakagai_edge.edge.remote import RemoteApprovalQueue
from nakagai_edge.edge.runtime import create_edge_mcp
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle
from nakagai_edge.hub import ConnectorError, ConnectorHub
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio

ORDER = {"limit_price": 190.0, "stop_price": 180.0,
         "time_in_force": "day"}

ALIEN, ROBINHOOD = "alien-broker", "robinhood-trading"
ACCOUNTS = {ALIEN: "AL-1", ROBINHOOD: "463605220"}
BOTH = [ALIEN, ROBINHOOD]


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---- the two brokers, and what they answer --------------------------------
#
# Every payload here is the broker's OWN shape, keyed by the broker's OWN tool
# name. Nothing in these dicts is canonical, which is the point: a test that
# passes against one connector and fails against the other has found something
# upstream that is still hardcoded.

PAYLOADS = {
    # The alien broker wraps nothing and spells every field its own way.
    "accounts_list": {"accounts": [{"acct": "AL-1", "label": "Alien Main",
                                    "kind": "margin"}]},
    "balances": {"net_liq": "104238.55", "settled_cash": "50.00",
                 "power": "12038.10", "denom": "USD"},
    "holdings": {"holdings": [{"ticker": "aapl", "qty": "25",
                               "cost": "187.20"}]},
    "ticker": {"ticks": [{"tkr": "aapl", "last": "190.00"}]},
    "orders": {"working": [{"ref": "AL-ORD-1", "tkr": "aapl",
                            "action": "BUY_TO_OPEN", "qty": "25",
                            "stage": "working"}]},
    "submit": {"order_ref": "AL-ORD-1", "state": "accepted"},
    "scrub": {"ref": "AL-ORD-1", "state": "cancelled"},
    # Robinhood nests its own {"data": ...} envelope, which each map roots at.
    "get_accounts": {"data": {"accounts": [{"account_number": "463605220",
                                            "nickname": "Main",
                                            "type": "margin"}]}},
    "get_portfolio": {"data": {"total_value": "104238.55", "cash": "50.00",
                               "buying_power": {"buying_power": "12038.10"},
                               "currency": "USD"}},
    "get_equity_positions": {"data": {"positions": [
        {"symbol": "AAPL", "quantity": "25", "average_buy_price": "187.20"}]}},
    "get_quotes": {"data": {"quotes": [{"symbol": "AAPL",
                                        "last_trade_price": "190.00"}]}},
    "get_equity_orders": {"data": {"orders": [{"id": "RH-ORD-9", "symbol": "AAPL",
                                        "side": "buy", "quantity": "25",
                                        "state": "confirmed"}]}},
    "place_equity_order": {"id": "RH-ORD-9", "state": "confirmed"},
    "cancel_order": {"id": "RH-ORD-9", "state": "cancelled"},
}


def _specs(*entries, over: dict | None = None):
    """Registry specs, with per-connector overrides merged in.

    `over` is {connector_id: {...}}, merged one level deep for `guardrails` so
    a test can retier the accounts or drop a capability without restating the
    whole map.
    """
    built = []
    for entry in entries or (ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR):
        patch = (over or {}).get(entry["id"]) or {}
        merged = {**entry, **patch}
        if "guardrails" in patch:
            merged["guardrails"] = {**entry["guardrails"], **patch["guardrails"]}
        built.append(merged)
    return load_specs({"connectors": built})


def _tiered(allow, read, entry=ALIEN_CONNECTOR):
    """One connector with its account tiers rewritten."""
    accounts = {**entry["guardrails"]["accounts"],
                "allow": list(allow), "read": list(read)}
    return _specs(entry, over={entry["id"]: {"guardrails": {"accounts": accounts}}})


def _without(entry, capability):
    """The same connector, minus one capability from its map."""
    caps = {k: v for k, v in entry["capabilities"].items() if k != capability}
    return _specs(entry, over={entry["id"]: {"capabilities": caps}})


class MapHub:
    """A hub that answers through the REAL connector maps, keyed by tool name.

    Task 6's FakeHub shape: every call lands on `self.calls`, so a test can pin
    the tool name AND the argument keys the map produced. A broker handed the
    right tool under the wrong key answers about nothing at all.
    """

    def __init__(self, specs=None, fail=None, payloads=None):
        self.account_key = "ag1"
        self.calls: list[tuple] = []
        self.kwargs: list[dict] = []
        self.specs = _specs() if specs is None else specs
        self.fail = fail
        self.payloads = {**PAYLOADS, **(payloads or {})}
        self.approvals = None

    def load_specs(self):
        return self.specs

    def spec(self, connector_id):
        if connector_id not in self.specs:
            raise ConnectorError(f"no connector {connector_id!r}")
        return self.specs[connector_id]

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        self.kwargs.append(kw)
        if self.fail is not None:
            raise self.fail
        return {"connector": connector_id, "tool": tool, "is_write": False,
                "is_error": False, "data": self.payloads[tool]}

    @property
    def tools(self) -> list[str]:
        return [tool for _, tool, _ in self.calls]

    @property
    def args(self) -> list[dict]:
        return [args for _, _, args in self.calls]


class _Reporter:
    async def snapshot_and_push(self):
        return {"connectors": []}


def _state(tmp_path, *entries):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": list(entries)},
                         "signing_public_key": "k"}, "v1")
    return state


def _dead_platform():
    return PlatformClient("https://api.test", "t",
                          transport=httpx.MockTransport(
                              lambda r: httpx.Response(500)))


def _server(state, hub, client=None):
    client = client or _dead_platform()
    audit = EdgeAudit(state)
    return create_edge_mcp(state, hub, client, audit, _Reporter(),
                           Brake(state, hub, client, audit))


async def _call(mcp, name, **args):
    result = await mcp.call_tool(name, args)
    text = result.content[0].text
    return json.loads(text)


# ---- raw calls cannot bypass semantic order intent -----------------------


@pytest.mark.parametrize("connector,raw_tool", [
    (ALIEN, "submit"),
    (ROBINHOOD, "place_equity_order"),
])
async def test_raw_declared_order_tool_requires_canonical_order_intent(
        tmp_path, connector, raw_tool):
    """Removing the refusal would let a caller bypass the canonical order
    vocabulary and reach the hub with the broker's private order shape."""
    hub = MapHub()
    doc = await _call(
        _server(_state(tmp_path), hub), "call_connector",
        connector_id=connector, tool=raw_tool,
        args_json=json.dumps({"account": ACCOUNTS[connector], "quantity": 1}),
    )

    assert doc == {
        "is_error": True,
        "code": "canonical_order_required",
        "error": "raw order calls are retired; use place_order",
    }
    assert hub.calls == [], "the refusal must land before hub call or enqueue"


async def test_semantic_place_order_still_resolves_the_declared_order_tool(
        tmp_path):
    """Guarding the raw name must not guard the semantic route after it has
    translated canonical fields through the selected connector map."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    doc = await _call(
        _server(_state(tmp_path), hub), "place_order",
        connector_id=ALIEN, symbol="AAPL", side="buy", quantity=1,
        account="AL-1", **ORDER,
    )

    assert doc["tool"] == "submit"
    assert hub.calls == [(ALIEN, "submit", {
        "ticker": "AAPL", "action": "BUY", "kind": "LIMIT",
        "qty": "1", "limit": "190.0", "trigger": "180.0",
        "tif": "day", "acct": "AL-1",
    })]


async def test_semantic_place_order_exposes_only_limit_entries(tmp_path):
    tools = {
        tool.name: tool
        for tool in await _server(_state(tmp_path), MapHub()).list_tools()
    }

    schema = tools["place_order"].input_schema
    assert "order_type" not in schema["properties"]
    assert "order_type" not in schema["required"]
    assert {"limit_price", "stop_price"} <= set(schema["required"])


async def test_candidate_wake_denies_every_model_order_door_and_keeps_reads(
        tmp_path):
    posted = []
    state, client = _state(tmp_path, ALIEN_CONNECTOR), _platform(posted)
    CandidateWakeScope(state).begin({
        "seq": 1, "kind": "execution_candidate", "response_required": True,
        "candidate_id": "candidate-1", "expires_at": time.time() + 60,
    })
    hub = _live_hub(state, client)
    mcp = _server(state, hub, client)
    try:
        place = await _call(
            mcp, "place_order", symbol="AAPL", side="buy", quantity=1,
            account="AL-1", **ORDER)
        cancel = await _call(
            mcp, "cancel_order", order_id="AL-ORD-1", account="AL-1")
        raw = await _call(
            mcp, "call_connector", connector_id=ALIEN, tool="scrub",
            args_json=json.dumps({"ref": "AL-ORD-1", "acct": "AL-1"}))
        positions = await _call(
            mcp, "list_positions", connector_id=ALIEN, account="AL-1")
    finally:
        await hub.aclose()

    for result in (place, cancel, raw):
        assert result["is_error"] is True
        assert "candidate wake" in result["error"]
    assert positions["is_error"] is False
    assert posted == []


async def test_candidate_wake_rejects_model_attempts_to_forge_internal_authority(
        tmp_path):
    posted = []
    state, client = _state(tmp_path, ALIEN_CONNECTOR), _platform(posted)
    CandidateWakeScope(state).begin({
        "seq": 2, "kind": "execution_candidate", "response_required": True,
        "candidate_id": "candidate-1", "expires_at": time.time() + 60,
    })
    hub = _live_hub(state, client)
    try:
        result = await _call(
            _server(state, hub, client), "call_connector",
            connector_id=ALIEN, tool="scrub",
            args_json=json.dumps({
                "acct": "AL-1", "ref": "AL-ORD-1", "approved": True,
                "candidate_id": "candidate-1",
            }),
        )
    finally:
        await hub.aclose()

    assert result["is_error"] is True
    assert "candidate wake" in result["error"]
    assert posted == []


async def test_raw_operation_outside_the_order_capability_still_works(tmp_path):
    """A broad write-name or connector-role check would retire operations the
    canonical vocabulary does not cover."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    doc = await _call(
        _server(_state(tmp_path), hub), "call_connector",
        connector_id=ALIEN, tool="ticker",
        args_json=json.dumps({"tickers": ["AAPL"]}),
    )

    assert doc["data"] == PAYLOADS["ticker"]
    assert hub.calls == [(ALIEN, "ticker", {"tickers": ["AAPL"]})]


async def test_raw_tool_is_unaffected_without_a_place_order_capability(tmp_path):
    """Matching a guessed broker name would refuse connectors that never
    declared the canonical capability and therefore have no semantic path."""
    hub = MapHub(specs=_without(ALIEN_CONNECTOR, "place_order"))
    doc = await _call(
        _server(_state(tmp_path), hub), "call_connector",
        connector_id=ALIEN, tool="submit",
        args_json=json.dumps({"acct": "AL-1", "qty": 1}),
    )

    assert doc["data"] == PAYLOADS["submit"]
    assert hub.calls == [(ALIEN, "submit", {"acct": "AL-1", "qty": 1})]


async def test_raw_registry_error_still_returns_an_error_document(tmp_path):
    """Adding the capability lookup must not turn a malformed registry from
    the existing guarded error document into an escaped MCP execution error."""
    class BrokenRegistryHub(MapHub):
        def spec(self, connector_id):
            raise ValueError("broken connector registry")

        async def call(self, connector_id, tool, args, **kw):
            raise ValueError("broken connector registry")

    hub = BrokenRegistryHub()
    doc = await _call(
        _server(_state(tmp_path), hub), "call_connector",
        connector_id=ALIEN, tool="ticker", args_json="{}",
    )

    assert doc == {"is_error": True, "error": "broken connector registry"}


# ---- the reads: the same canonical fields out of two alien shapes ---------


@pytest.mark.parametrize("connector", BOTH)
async def test_list_accounts_reads_both_brokers_into_one_shape(tmp_path, connector):
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "list_accounts",
                      connector_id=connector)

    assert doc["data"] == [{"account": ACCOUNTS[connector],
                            "nickname": {ALIEN: "Alien Main",
                                         ROBINHOOD: "Main"}[connector],
                            "type": "margin"}]
    assert doc["capability"] == "list_accounts"
    assert doc["connector"] == connector
    assert doc["tool"] == {ALIEN: "accounts_list",
                           ROBINHOOD: "get_accounts"}[connector]


@pytest.mark.parametrize("connector", BOTH)
async def test_get_balance_reads_both_brokers_into_one_shape(tmp_path, connector):
    """The worked example: `104238.55` sits under `net_liq` on one broker and
    under `data.total_value` on the other, and the agent sees `equity` either
    way. Verbatim, never re-typed: the edge relays this figure and does no
    arithmetic on it."""
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "get_balance",
                      connector_id=connector, account=ACCOUNTS[connector])

    assert doc["data"] == {"equity": "104238.55", "cash": "50.00",
                           "buying_power": "12038.10", "currency": "USD"}
    assert (doc["capability"], doc["connector"]) == ("get_balance", connector)
    assert doc["tool"] == {ALIEN: "balances", ROBINHOOD: "get_portfolio"}[connector]
    # The tool name AND the account key both came out of the map.
    assert hub.calls == [(connector, doc["tool"],
                          {{ALIEN: "acct", ROBINHOOD: "account_number"}[connector]:
                           ACCOUNTS[connector]})]


@pytest.mark.parametrize("connector", BOTH)
async def test_list_positions_reads_both_brokers_into_one_shape(tmp_path, connector):
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "list_positions",
                      connector_id=connector, account=ACCOUNTS[connector])

    assert doc["data"] == [{"symbol": "AAPL", "quantity": 25.0,
                            "avg_price": 187.20}]
    assert doc["tool"] == {ALIEN: "holdings",
                           ROBINHOOD: "get_equity_positions"}[connector]


async def test_a_broker_holding_nothing_is_an_empty_list(tmp_path):
    """The one shape that means "nothing held": a list, with nothing in it."""
    hub = MapHub(payloads={"holdings": {"holdings": []}})
    doc = await _call(_server(_state(tmp_path), hub), "list_positions",
                      connector_id=ALIEN, account=ACCOUNTS[ALIEN])

    assert doc["data"] == [] and not doc.get("is_error")


async def test_an_unreadable_list_answer_is_an_error_not_an_empty_account(
        tmp_path):
    """A payload holding no list where the map says one lives is a FAILURE to
    read, and the agent has to hear it as one.

    This is the surface an agent consults before adding to a position. An
    unreadable answer returned as `[]` reads as "the account is flat", which is
    how a stale map or a changed broker shape turns one intended position into
    two real ones. The brake is not the thing at risk here (its own position
    re-read refuses to use `extract` for exactly this reason); the agent is.
    """
    hub = MapHub(payloads={"holdings": {"holdings": {"ticker": "aapl"}}})
    doc = await _call(_server(_state(tmp_path), hub), "list_positions",
                      connector_id=ALIEN, account=ACCOUNTS[ALIEN])

    assert doc["is_error"] is True
    assert doc["data"] is None
    assert "NOT" in doc["error"] and ALIEN in doc["error"]
    # Provenance survives the refusal: the call really was made.
    assert (doc["capability"], doc["tool"]) == ("list_positions", "holdings")


@pytest.mark.parametrize("connector", BOTH)
async def test_get_quote_reads_both_brokers_into_one_shape(tmp_path, connector):
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "get_quote",
                      connector_id=connector, symbols=["AAPL"])

    assert doc["data"] == [{"symbol": "AAPL", "price": 190.0}]
    assert hub.args == [{{ALIEN: "tickers", ROBINHOOD: "symbols"}[connector]:
                         ["AAPL"]}]


@pytest.mark.parametrize("connector", BOTH)
async def test_list_orders_reads_both_brokers_into_one_shape(tmp_path, connector):
    """The status axis tests divergence only because the fixtures disagree
    about it: `stage` on the alien broker, `state` on Robinhood, and neither is
    the argument key the status filter goes under."""
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "list_orders",
                      connector_id=connector, account=ACCOUNTS[connector],
                      status="open")

    assert doc["data"] == [{"order_id": {ALIEN: "AL-ORD-1",
                                         ROBINHOOD: "RH-ORD-9"}[connector],
                            "symbol": "AAPL", "side": "buy", "quantity": 25.0,
                            "status": {ALIEN: "working",
                                       ROBINHOOD: "confirmed"}[connector]}]
    # Both brokers happen to call the filter `state`, and they reach it from
    # opposite directions: the alien map renames `status` to `state`, and
    # Robinhood's own parameter simply IS `state`. The map declared `status`
    # here until 2026-08-06, which sent Robinhood a key it ignores.
    assert hub.args == [{ALIEN: {"acct": "AL-1", "state": "open"},
                         ROBINHOOD: {"account_number": "463605220",
                                     "state": "open"}}[connector]]


# ---- arguments the caller must actually supply ---------------------------


@pytest.mark.parametrize("tool,args,named", [
    ("cancel_order", {"order_id": ""}, "order_id"),
    ("place_order", {"symbol": "", "side": "buy", "quantity": 1, **ORDER}, "symbol"),
    ("place_order", {"symbol": "AAPL", "side": "", "quantity": 1, **ORDER}, "side"),
    ("get_quote", {"symbols": []}, "symbols"),
])
async def test_an_empty_mandatory_argument_is_refused_not_dropped(
        tmp_path, tool, args, named):
    """Dropping an empty one would not ask the broker a smaller question, it
    would ask a different one: a cancel with no order id, or an order with no
    symbol, is a materially different request that the broker (or a human
    reading the approval) has to make sense of. Refuse it by name instead, and
    dial nothing."""
    hub = MapHub(specs=_tiered(["AL-1"], []))
    doc = await _call(_server(_state(tmp_path), hub), tool, **args)

    assert doc["is_error"] is True and named in doc["error"]
    assert hub.calls == [], "nothing may be dialed or enqueued on a blank"


async def test_an_omitted_optional_argument_is_simply_omitted(tmp_path):
    """The other half of the same rule: an optional the agent left alone is
    absent from the broker call rather than sent as an empty string, which a
    broker would read as a filter matching nothing."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    await _call(_server(_state(tmp_path), hub), "list_orders",
                account="AL-1", status="")

    assert hub.args == [{"acct": "AL-1"}]


# ---- connector inference --------------------------------------------------


async def test_two_brokers_declaring_a_capability_is_an_error_naming_both(tmp_path):
    """Never a pick. Which broker received an order must not depend on
    registry ordering."""
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "get_balance")

    assert doc["is_error"] is True
    assert ALIEN in doc["error"] and ROBINHOOD in doc["error"]
    assert "connector_id" in doc["error"]
    assert hub.calls == [], "an ambiguous connector must never be dialed"


async def test_one_broker_declaring_a_capability_is_inferred(tmp_path):
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    doc = await _call(_server(_state(tmp_path), hub), "get_balance")

    assert doc["connector"] == ALIEN
    assert doc["data"]["equity"] == "104238.55"


@pytest.mark.parametrize("disqualify", [{"enabled": False}, {"role": "data"}])
async def test_only_an_enabled_broker_is_a_candidate(tmp_path, disqualify):
    """A disabled connector, and one that is not a broker at all, are both out
    of the running: inference must not resolve to something the owner turned
    off, or to a data feed that happens to declare a map."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR,
                              over={ROBINHOOD: disqualify}))
    doc = await _call(_server(_state(tmp_path), hub), "get_balance")

    assert doc["connector"] == ALIEN


async def test_no_broker_declaring_a_capability_is_an_error_naming_it(tmp_path):
    hub = MapHub(specs=_without(ALIEN_CONNECTOR, "get_quote"))
    doc = await _call(_server(_state(tmp_path), hub), "get_quote",
                      symbols=["AAPL"])

    assert doc["is_error"] is True and "get_quote" in doc["error"]
    assert hub.calls == []


async def test_an_unknown_connector_id_is_an_error_not_a_fallback(tmp_path):
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "get_balance",
                      connector_id="nope")

    assert doc["is_error"] is True and "nope" in doc["error"]
    assert hub.calls == []


# ---- an unmapped capability ----------------------------------------------


async def test_a_capability_the_connector_never_declared_names_both(tmp_path):
    """`ConnectorSpec.capability()` refuses rather than guessing a tool name,
    and the agent is told which connector is missing which entry."""
    hub = MapHub(specs=_without(ALIEN_CONNECTOR, "list_positions"))
    doc = await _call(_server(_state(tmp_path), hub), "list_positions",
                      connector_id=ALIEN, account="AL-1")

    assert doc["is_error"] is True
    assert ALIEN in doc["error"] and "list_positions" in doc["error"]
    assert hub.calls == [], "an unmapped capability must not be dialed on a guess"


@pytest.mark.parametrize("specs,tool,args,connector", [
    # Unmapped: refused by `spec.capability`, after the connector was chosen.
    (_without(ALIEN_CONNECTOR, "list_positions"), "list_positions",
     {"connector_id": ALIEN, "account": "AL-1"}, ALIEN),
    # The same refusal on `list_accounts`, which the account-inference probe
    # also runs into. An agent that asked for it ITSELF is being turned away
    # and the trail has to say so; the probe below is not, and does not.
    (_without(ALIEN_CONNECTOR, "list_accounts"), "list_accounts",
     {"connector_id": ALIEN}, ALIEN),
    # Ambiguous: refused by `_pick_connector`, before one was chosen, so the
    # event names no broker rather than one nothing was sent to.
    (None, "get_balance", {}, ""),
    # A mandatory argument left empty: refused by `required`.
    (_specs(ALIEN_CONNECTOR), "cancel_order", {"order_id": ""}, ALIEN),
])
async def test_every_pre_guard_refusal_is_journalled(tmp_path, specs, tool,
                                                     args, connector):
    """An owner reading the audit trail sees every refusal, not a subset.

    These three land before `_guarded`, which is where every other denial is
    recorded, so each has to be journalled where it happens. Without that, an
    agent being turned away over and over is indistinguishable from an idle
    one, and the misconfiguration doing the turning away leaves no trace of
    itself anywhere the owner looks.
    """
    state = _state(tmp_path)
    hub = MapHub(specs=specs) if specs is not None else MapHub()
    doc = await _call(_server(state, hub), tool, **args)

    assert doc["is_error"] is True
    events = EdgeAudit(state).pending()
    assert [e["kind"] for e in events] == ["denial"]
    assert events[0]["connector_id"] == connector
    assert events[0]["detail"]["capability"] == tool
    # The reason is the sentence the agent was given, not a category.
    assert events[0]["detail"]["reason"] == doc["error"]


async def test_a_failed_inference_probe_journals_no_denial(tmp_path):
    """A denial that did not happen is worse than one that went unrecorded.

    `_infer_account` asks the connector for its accounts when the owner
    configured no tiers. That probe is a step INSIDE the agent's call, not
    something the agent asked for, so a connector with no `list_accounts` map
    refuses nothing: the outer call carries on and is journalled on its own
    terms. Left unsuppressed, a successful order would sit in the trail behind
    a `denial` naming a capability the agent never called, and an owner reading
    it cannot tell that from a real refusal.
    """
    caps = {k: v for k, v in ALIEN_CONNECTOR["capabilities"].items()
            if k != "list_accounts"}
    accounts = {**ALIEN_CONNECTOR["guardrails"]["accounts"],
                "allow": [], "read": []}
    # No tiers AND no list_accounts map: the only shape that reaches the probe
    # and then fails inside it.
    entry = {**ALIEN_CONNECTOR, "capabilities": caps,
             "guardrails": {**ALIEN_CONNECTOR["guardrails"], "accounts": accounts}}
    state = _state(tmp_path)
    hub = MapHub(specs=_specs(entry))

    doc = await _call(_server(state, hub), "get_balance")

    assert not doc.get("is_error"), "the outer call is unaffected by the probe"
    assert [e["kind"] for e in EdgeAudit(state).pending()] == ["call"]


# ---- account inference ----------------------------------------------------
#
# Inference FILLS the argument; `check_accounts` then evaluates it exactly as
# it evaluates one the agent named. These tests pin what reaches the broker,
# and the guardrail tests further down pin that an argument inference declined
# to fill is refused by the guardrails, not by this layer.


async def test_a_write_infers_the_single_allowed_account(tmp_path):
    hub = MapHub(specs=_tiered(["AL-1"], []))
    await _call(_server(_state(tmp_path), hub), "place_order",
                symbol="AAPL", side="buy", quantity=1, **ORDER)

    assert hub.args == [{"ticker": "AAPL", "action": "BUY", "kind": "LIMIT",
                         "qty": "1", "limit": "190.0", "trigger": "180.0",
                         "tif": "day", "acct": "AL-1"}]


async def test_a_write_without_an_unambiguous_account_is_refused_before_the_broker(tmp_path):
    """An absent account must not reach a broker default selection."""
    hub = MapHub(specs=_tiered(["AL-1", "AL-2"], []))
    doc = await _call(_server(_state(tmp_path), hub), "place_order",
                      symbol="AAPL", side="buy", quantity=1, **ORDER)

    assert doc["is_error"] is True and "account" in doc["error"]
    assert hub.args == []


async def test_a_write_never_infers_the_read_tiers_account(tmp_path):
    """`read` accounts may be viewed, never acted on. A write infers from
    `allow` alone, so the account it fills is one that could have been named."""
    hub = MapHub(specs=_tiered(["AL-2"], ["AL-1"]))
    await _call(_server(_state(tmp_path), hub), "place_order",
                symbol="AAPL", side="buy", quantity=1, **ORDER)

    assert hub.args[0]["acct"] == "AL-2"


async def test_a_read_infers_across_the_allow_and_read_tiers(tmp_path):
    """One account between the two tiers is unambiguous for a read, even when
    it is the tier a write may never touch."""
    hub = MapHub(specs=_tiered([], ["AL-1"]))
    await _call(_server(_state(tmp_path), hub), "get_balance")

    assert hub.args == [{"acct": "AL-1"}]


async def test_a_read_does_not_infer_when_the_tiers_hold_two_accounts(tmp_path):
    hub = MapHub(specs=_tiered(["AL-2"], ["AL-1"]))
    await _call(_server(_state(tmp_path), hub), "get_balance")

    assert hub.args == [{}]


async def test_with_no_tiers_the_account_comes_from_the_brokers_own_list(tmp_path):
    """No tiers configured means the owner stated no preference, so a broker
    holding exactly one account has answered the question itself."""
    hub = MapHub(specs=_tiered([], []))
    doc = await _call(_server(_state(tmp_path), hub), "get_balance")

    assert hub.tools == ["accounts_list", "balances"]
    assert hub.args[1] == {"acct": "AL-1"}
    assert doc["data"]["equity"] == "104238.55"


async def test_with_no_tiers_and_two_listed_accounts_nothing_is_inferred(tmp_path):
    hub = MapHub(specs=_tiered([], []),
                 payloads={"accounts_list": {"accounts": [{"acct": "AL-1"},
                                                          {"acct": "AL-2"}]}})
    await _call(_server(_state(tmp_path), hub), "get_balance")

    assert hub.args[1] == {}, "an ambiguous list is not an answer"


async def test_a_missing_write_account_never_consults_the_brokers_account_list(tmp_path):
    """The one rule that would reintroduce the default-account hole.

    Tiers are the OWNER's statement of authority; the broker's list is not.
    Here the broker holds exactly one account and it is the read-tier one, so a
    layer that fell back to the broker's list would fill in the very account
    the owner walled off from writes. It must not even ask.
    """
    hub = MapHub(specs=_tiered(["AL-2", "AL-3"], ["AL-1"]))
    await _call(_server(_state(tmp_path), hub), "place_order",
                symbol="AAPL", side="buy", quantity=1, **ORDER)

    assert hub.tools == [], "the broker's account list was consulted"
    assert hub.args == []


async def test_an_account_the_agent_named_is_never_overridden(tmp_path):
    """Inference only ever FILLS. What the agent said reaches `check_accounts`
    unchanged, including an account it is about to be refused for."""
    hub = MapHub(specs=_tiered(["AL-1"], []))
    await _call(_server(_state(tmp_path), hub), "get_balance", account="AL-9")

    assert hub.args == [{"acct": "AL-9"}]


async def test_a_capability_with_no_account_argument_infers_nothing(tmp_path):
    """`get_quote` takes symbols and nothing else, so there is no account to
    fill and the broker's account list is never dialed for one."""
    hub = MapHub(specs=_tiered([], []))
    await _call(_server(_state(tmp_path), hub), "get_quote",
                connector_id=ALIEN, symbols=["AAPL"])

    assert hub.tools == ["ticker"]


# ---- the writes go through the one authorization path ---------------------


def _platform(posted):
    def handler(req):
        if req.url.path == "/api/agent/approvals" and req.method == "POST":
            posted.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True, "approval_id": "ap_live",
                                             "status": "pending",
                                             "expires_at": time.time() + 900})
        return httpx.Response(404, json={"detail": "?"})

    return PlatformClient("https://api.test", "nk_agent_t",
                          transport=httpx.MockTransport(handler))


def _live_hub(state, client):
    """A REAL ConnectorHub over the real alien broker, in memory.

    Nothing is stubbed between the tool and the guardrails: this is the path
    that classifies the write, enqueues the approval, and hands back the
    envelope the agent has to poll on.
    """
    from tests.fixtures.alien_broker_mcp import mcp as alien_server
    from tests.fixtures.inproc import connect_to

    queue = RemoteApprovalQueue(client, state, "ag1")
    hub = ConnectorHub(state.root, connect=connect_to(alien_server), approvals=queue)
    hub.account_key = "ag1"
    return hub


async def test_place_order_returns_the_approval_envelope_intact(tmp_path):
    """The whole envelope, verbatim. Running a write through `extract` would
    return `{}` (place_order maps no readable fields by construction) and throw
    away the approval id the agent must poll."""
    posted = []
    state, client = _state(tmp_path, ALIEN_CONNECTOR), _platform(posted)
    hub = _live_hub(state, client)
    try:
        doc = await _call(_server(state, hub, client), "place_order",
                          symbol="AAPL", side="buy", quantity=1,
                          account="AL-1", **ORDER)
    finally:
        await hub.aclose()

    assert doc["approval_required"] is True
    assert doc["approval_id"] == "ap_live"
    assert doc["status"] == "pending"
    assert doc["expires_at"] > time.time()
    assert doc["is_write"] is True
    assert "get_approval" in doc["message"]
    assert (doc["capability"], doc["connector"], doc["tool"]) == (
        "place_order", ALIEN, "submit")
    # Translated on the way down: the broker's own words are what the human
    # sees on the approval screen and what the edge later executes.
    assert posted[0]["args"] == {"acct": "AL-1", "ticker": "AAPL",
                                 "action": "BUY", "kind": "LIMIT", "qty": "1",
                                 "limit": "190.0", "trigger": "180.0", "tif": "day"}


async def test_place_order_forwards_signal_id_to_the_platform(tmp_path):
    """autopilot's envelope checks the cited signal: an order citing nothing
    never auto-executes. Dropping it here would disable that check silently,
    from the platform's side, with everything still looking normal."""
    posted = []
    state, client = _state(tmp_path, ALIEN_CONNECTOR), _platform(posted)
    hub = _live_hub(state, client)
    try:
        await _call(_server(state, hub, client), "place_order",
                    symbol="AAPL", side="buy", quantity=1, account="AL-1",
                    signal_id="sig-42", **ORDER)
    finally:
        await hub.aclose()

    assert posted[0]["signal_id"] == "sig-42"


async def test_cancel_order_returns_the_approval_envelope_intact(tmp_path):
    posted = []
    state, client = _state(tmp_path, ALIEN_CONNECTOR), _platform(posted)
    hub = _live_hub(state, client)
    try:
        doc = await _call(_server(state, hub, client), "cancel_order",
                          order_id="AL-ORD-1", account="AL-1")
    finally:
        await hub.aclose()

    assert doc["approval_required"] is True and doc["approval_id"] == "ap_live"
    assert doc["tool"] == "scrub"
    assert posted[0]["args"] == {"acct": "AL-1", "ref": "AL-ORD-1"}


async def test_an_executed_write_returns_the_brokers_own_answer(tmp_path):
    """A write that needs no approval still comes back verbatim: the broker's
    order reference is what the agent has to hold on to, and `extract` would
    hand back `{}` in its place."""
    hub = MapHub()
    doc = await _call(_server(_state(tmp_path), hub), "place_order",
                      connector_id=ALIEN, symbol="AAPL", side="buy",
                      quantity=1, account="AL-1", **ORDER)

    assert doc["data"] == {"order_ref": "AL-ORD-1", "state": "accepted"}
    assert doc["capability"] == "place_order"


async def test_a_side_the_connector_cannot_spell_is_refused(tmp_path):
    """The vocabulary is closed at the edge of this layer too. `resolve`
    refuses a side it cannot translate rather than passing the agent's own word
    down, because a broker that read `short` as something else would open the
    opposite position."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    doc = await _call(_server(_state(tmp_path), hub), "place_order",
                      symbol="AAPL", side="short", quantity=1, account="AL-1", **ORDER)

    assert doc["is_error"] is True and "side" in doc["error"]
    assert hub.calls == []


async def test_an_uninferable_write_is_refused_by_the_order_resolver(tmp_path):
    """The proof that this layer holds no authority of its own.

    Two allowed accounts leave no account to infer. The resolver refuses before
    the broker can apply a default account.
    """
    entry = {**ALIEN_CONNECTOR,
             "guardrails": {**ALIEN_CONNECTOR["guardrails"],
                            "accounts": {**ALIEN_CONNECTOR["guardrails"]["accounts"],
                                         "allow": ["AL-1", "AL-2"]}}}
    state, client = _state(tmp_path, entry), _platform([])
    hub = _live_hub(state, client)
    try:
        doc = await _call(_server(state, hub, client), "place_order",
                          symbol="AAPL", side="buy", quantity=1, **ORDER)
    finally:
        await hub.aclose()

    assert doc["is_error"] is True
    assert "account" in doc["error"]


async def test_a_write_to_a_read_only_connector_is_refused_by_the_guardrails(
        tmp_path):
    """`allow_writes: false` is the owner's word, and the capability layer
    cannot spend it. Same denial, same sentence, through the semantic tool."""
    entry = {**ALIEN_CONNECTOR,
             "guardrails": {**ALIEN_CONNECTOR["guardrails"], "allow_writes": False}}
    state, client = _state(tmp_path, entry), _platform([])
    hub = _live_hub(state, client)
    try:
        doc = await _call(_server(state, hub, client), "place_order",
                          symbol="AAPL", side="buy", quantity=1, account="AL-1", **ORDER)
    finally:
        await hub.aclose()

    assert doc["is_error"] is True and "read-only" in doc["error"]


# ---- the stale-policy gate ------------------------------------------------


SEVEN = [
    ("list_accounts", {}),
    ("get_balance", {}),
    ("list_positions", {}),
    ("get_quote", {"symbols": ["AAPL"]}),
    ("list_orders", {}),
    ("place_order", {"symbol": "AAPL", "side": "buy", "quantity": 1, **ORDER}),
    ("cancel_order", {"order_id": "AL-ORD-1"}),
]


@pytest.mark.parametrize("tool,args", SEVEN)
async def test_every_semantic_tool_refuses_on_stale_policy(tmp_path, monkeypatch,
                                                           tool, args):
    """Fail closed exactly like `call_connector`: policy past its TTL refuses
    every connector call, and the refusal lands before anything is dialed."""
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    mcp = _server(_state(tmp_path), hub)
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past the 900s TTL

    doc = await _call(mcp, tool, **args)

    assert doc["is_error"] is True and "policy stale" in doc["error"]
    assert hub.calls == []


async def test_stale_policy_is_the_answer_before_any_other_complaint(
        tmp_path, monkeypatch):
    """Staleness is the outermost rule, so it is what the agent hears.

    Two brokers are configured here, which on a fresh edge would come back as
    "name one with connector_id". While policy is stale that answer would be a
    lie of emphasis: the agent would go and name a connector, and be refused
    again for the real reason. `_guarded` refuses either way, so nothing is
    dialed; this is about which sentence the agent is sent away with.
    """
    hub = MapHub()
    mcp = _server(_state(tmp_path), hub)
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)

    doc = await _call(mcp, "get_balance")

    assert "policy stale" in doc["error"]
    assert "connector_id" not in doc["error"]
    assert hub.calls == []


async def test_a_stale_write_attempt_is_still_journalled(tmp_path, monkeypatch):
    """An agent trying to trade while the edge is cut off from the platform is
    exactly the event an owner needs to find afterwards.

    The gate that refuses it sits ahead of `_guarded`, which is where every
    other denial is recorded, so this one has to be recorded where it happens
    or the attempt leaves no trace at all: the owner's audit trail would show
    an outage and, during it, an agent that looks idle.
    """
    hub = MapHub(specs=_specs(ALIEN_CONNECTOR))
    state = _state(tmp_path)
    mcp = _server(state, hub)
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)

    await _call(mcp, "place_order", symbol="AAPL", side="buy", quantity=1, **ORDER)

    events = EdgeAudit(state).pending()
    assert len(events) == 1
    assert events[0]["kind"] == "denial"
    assert events[0]["detail"] == {"reason": "policy stale",
                                   "capability": "place_order"}


async def test_a_refusal_before_the_connector_was_chosen_names_no_broker(tmp_path,
                                                                        monkeypatch):
    """Provenance on the stale result too, and honest: empty strings say the
    refusal landed before a connector was picked, rather than naming a broker
    nothing was ever sent to."""
    mcp = _server(_state(tmp_path), MapHub(specs=_specs(ALIEN_CONNECTOR)))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)

    doc = await _call(mcp, "get_balance")

    assert (doc["capability"], doc["connector"], doc["tool"]) == (
        "get_balance", "", "")


# ---- a broker that will not answer ---------------------------------------


async def test_a_broker_error_comes_back_as_an_error_with_its_provenance(tmp_path):
    """The connector and tool that failed are named: an agent holding five
    connectors needs to know which one went dark."""
    hub = MapHub(fail=ConnectorError("connection reset mid-call"))
    doc = await _call(_server(_state(tmp_path), hub), "get_balance",
                      connector_id=ALIEN, account="AL-1")

    assert doc["is_error"] is True and "connection reset" in doc["error"]
    assert (doc["capability"], doc["connector"], doc["tool"]) == (
        "get_balance", ALIEN, "balances")


# ---- what an agent can see before it calls -------------------------------


async def test_list_connectors_says_what_each_connector_can_do(tmp_path):
    state = _state(tmp_path, ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR)
    doc = await _call(_server(state, ConnectorHub(state.root)), "list_connectors")

    by_id = {c["id"]: c for c in doc["connectors"]}
    assert by_id[ALIEN]["capabilities"] == sorted(ALIEN_CONNECTOR["capabilities"])
    assert by_id[ROBINHOOD]["capabilities"] == sorted(
        ROBINHOOD_CONNECTOR["capabilities"])


async def test_get_connector_status_says_so_too_even_on_stale_policy(
        tmp_path, monkeypatch):
    """The one tool that works on stale policy, because an agent needs to see
    WHY everything else is refusing. It must still say what exists."""
    state = _state(tmp_path, ALIEN_CONNECTOR)
    mcp = _server(state, ConnectorHub(state.root))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)

    doc = await _call(mcp, "get_connector_status")

    assert doc["policy_fresh"] is False
    assert "get_balance" in doc["connectors"][0]["capabilities"]
