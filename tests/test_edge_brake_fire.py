"""Firing: the one path where the brake touches the broker.

The bounds pinned here are the whole reason a standing warrant is safe. Note
two inversions of the edge's usual posture, both deliberate: the brake fires on
STALE POLICY and with the KILL SWITCH engaged, because its authority came from
a signed artifact rather than from cached policy, and because reducing exposure
is the safe direction.
"""

import asyncio
import threading

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.capability import Capability
from nakagai_edge.config import ConnectorSpec, load_specs
from nakagai_edge.edge import brake as brake_module
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import load, record
from nakagai_edge.edge.sync import apply_bundle
from nakagai_edge.hub import GuardrailDenied
from nakagai_edge.signing import generate_keypair, sign_artifact
from nakagai_edge.warrant import TRIGGER_BELOW, build_warrant_payload
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio

PRIV, PUB = generate_keypair()

SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})

PLACE_ORDER = Capability(
    tool="place_equity_order",
    args={"symbol": "symbol", "side": "side", "quantity": "quantity",
          "price": "limit_price", "stop": "stop_price"},
    values={"side": {"buy": ["buy", "buy_to_open", "buy_to_cover"],
                     "sell": ["sell", "sell_to_open", "sell_short"]}},
    market_args={"order_type": "market"})

# The pre-fire position re-read now goes through the map like everything else.
# Rooted at `positions` rather than Robinhood's real `data.positions` because
# the FakeHub below answers without Robinhood's inner envelope; the two real
# maps are exercised against their real shapes further down.
LIST_POSITIONS = Capability(
    tool="get_equity_positions",
    args={"account": "account_number"},
    items=["positions"],
    fields={"symbol": ["symbol"], "quantity": ["quantity"]})

ENTRY_ARGS = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
              "quantity": 100, "limit_price": 47.55, "stop_price": 46.20}


def _demo_spec(**caps) -> ConnectorSpec:
    """The demo connector's map. `list_positions` is always present, because a
    connector that cannot be asked what it holds can never fire at all."""
    return ConnectorSpec(id="demo", kind="mcp-http", role="broker",
                         capabilities={"list_positions": LIST_POSITIONS, **caps})


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeHub:
    def __init__(self, held=100.0, fail=None):
        self.calls = []
        self.held = held
        self.fail = fail
        self._spec = _demo_spec(place_order=PLACE_ORDER)

    def spec(self, connector_id):
        return self._spec

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, args, kw))
        if tool == "get_equity_positions":
            return {"data": {"positions": [
                {"symbol": "AAPL", "quantity": str(self.held)}]}}
        if self.fail is not None:
            raise self.fail
        return {"is_error": False, "data": {"order_id": "42"}}


class MalformedPositionsHub(FakeHub):
    """get_equity_positions answers, but not with a list the brake can walk."""

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, args, kw))
        if tool == "get_equity_positions":
            return {"data": "the broker sent prose, not a position list"}
        if self.fail is not None:
            raise self.fail
        return {"is_error": False, "data": {"order_id": "42"}}


class NoQtyKeyHub(FakeHub):
    """The symbol's row is there, but its size is not where the map says.

    The row reads far enough to name the position and no further, which is "I
    found it and cannot size it", never "it is gone"."""

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, args, kw))
        if tool == "get_equity_positions":
            return {"data": {"positions": [
                {"symbol": "AAPL", "lot_size": "100"}]}}
        if self.fail is not None:
            raise self.fail
        return {"is_error": False, "data": {"order_id": "42"}}


# No market_args declared, so this connector cannot express an exit at all.
NO_MARKET_CAP = PLACE_ORDER.model_copy(update={"market_args": {}})


class NoMarketExitHub(FakeHub):
    """A connector that never declared how to say "market order"."""

    def __init__(self, held=100.0, fail=None):
        super().__init__(held=held, fail=fail)
        self._spec = _demo_spec(place_order=NO_MARKET_CAP)


class NoPlaceOrderHub(FakeHub):
    """A connector whose map lost `place_order` between entry and the stop.

    A registry the owner edited, or a bundle that arrived with the entry
    dropped. `spec.capability` raises here, and an uncaught raise inside
    fire() is a brake that does not fire and says nothing.
    """

    def __init__(self, held=100.0, fail=None):
        super().__init__(held=held, fail=fail)
        self._spec = _demo_spec()


class NoListPositionsHub(FakeHub):
    """A connector whose map lost `list_positions` before the stop was touched.

    There is no position read left to make, and inventing one is exactly what
    this layer exists to stop. The answer has to be None ("do not fire blind"),
    never 0.0, which fire() would mark `released` and never look at again.
    """

    def __init__(self, held=100.0, fail=None):
        super().__init__(held=held, fail=fail)
        self._spec = ConnectorSpec(id="demo", kind="mcp-http", role="broker",
                                   capabilities={"place_order": PLACE_ORDER})


class MappedHub:
    """A hub that answers through the REAL connector maps, not a canned shape.

    `spec` hands back the actual registry entry, so the tool name and the
    argument key both have to come out of the map rather than out of this
    module's memory of what Robinhood calls things.
    """

    def __init__(self, payloads: dict):
        self.calls = []
        self.payloads = payloads

    def spec(self, connector_id):
        return SPECS[connector_id]

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, dict(args)))
        return {"data": self.payloads[tool]}


class OtherSymbolsHub(FakeHub):
    """A perfectly readable position list that simply does not name our symbol.

    This is what a broker plausibly answers when asked about account "": the
    real position is in a real account, so `_held_now` reads 0.0 and the record
    is marked `released`, which is TERMINAL and unrecoverable.
    """

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, args, kw))
        if tool == "get_equity_positions":
            return {"data": {"positions": [
                {"symbol": "MSFT", "quantity": "5"}]}}
        if self.fail is not None:
            raise self.fail
        return {"is_error": False, "data": {"order_id": "42"}}


class RacingHub(FakeHub):
    """FakeHub's call() never actually suspends, so two gathered coroutines
    calling it run fully sequentially: no interleaving, no race, whether or
    not the one-shot guard is atomic. A real await point here is what forces
    genuine interleaving between two concurrent fire() calls, which is what
    the one-shot claim must survive."""

    async def call(self, connector_id, tool, args, **kw):
        await asyncio.sleep(0)
        return await super().call(connector_id, tool, args, **kw)


class FakeClient:
    def __init__(self):
        self.messages = []
        self.executions = []

    def send_message(self, text):
        self.messages.append(text)
        return {"ok": True}

    def report_execution(self, approval_id, **kw):
        self.executions.append((approval_id, kw))
        return {"ok": True}


def _warrant(*, max_qty=100.0, agent_id="ag1", ttl_s=86400):
    return sign_artifact(PRIV, build_warrant_payload(
        grant_id="wr_1", agent_id=agent_id, connector_id="demo",
        account="463605220", symbol="AAPL", tool="place_equity_order",
        side="sell", max_qty=max_qty, trigger_kind=TRIGGER_BELOW,
        level=46.20, ttl_s=ttl_s))


def _rec(warrant=None, **over):
    rec = {"position_id": "ap_1", "connector_id": "demo",
           "account": "463605220", "symbol": "AAPL", "direction": "long",
           "signal_id": "sig_1", "entry_args": dict(ENTRY_ARGS),
           "entry_qty": 100.0, "entry_price": 47.55, "stop": 46.20,
           "confirmed_qty": 100.0, "last_confirmed_at": 0.0,
           "unguarded_qty": 0.0,
           "warrant": _warrant() if warrant is None else warrant,
           "state": "armed", "opened_at": 1.0}
    rec.update(over)
    return rec


class ThreadRecordingClient(FakeClient):
    """Records which thread each platform call ran on."""

    def __init__(self):
        super().__init__()
        self.threads = []

    def send_message(self, text):
        self.threads.append(threading.get_ident())
        return super().send_message(text)

    def report_execution(self, approval_id, **kw):
        self.threads.append(threading.get_ident())
        return super().report_execution(approval_id, **kw)


def _brake(tmp_path, hub, *, stale=False, client=None):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1",
                         "connectors": {"connectors": []},
                         "mandate": {}, "strategy_configs": {},
                         "signing_public_key": PUB}, "v1")
    if stale:
        state.meta_path.write_text('{"etag": "v1", "fetched_at": 1.0}')
    client = FakeClient() if client is None else client
    return state, client, Brake(state, hub, client, EdgeAudit(state))


async def test_a_breach_places_a_market_exit(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    assert await brake.fire(load(state)["ap_1"]) == ""
    tool, args, kw = hub.calls[-1]
    assert tool == "place_equity_order"
    assert args["side"] == "sell"
    assert args["quantity"] == 100.0
    assert args["order_type"] == "market"
    assert "limit_price" not in args and "stop_price" not in args
    assert kw["approved"] is True


async def test_the_exit_is_clamped_to_what_the_broker_says_is_held_right_now(tmp_path):
    # The ledger's figure is up to 300s old. Overselling a cash account is a
    # real violation, so the size comes from a fresh read, not from the ledger.
    hub = FakeHub(held=40.0)
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    assert await brake.fire(load(state)["ap_1"]) == ""
    assert hub.calls[-1][1]["quantity"] == 40.0


async def test_a_position_the_broker_no_longer_holds_is_released_not_sold(tmp_path):
    hub = FakeHub(held=0.0)
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    assert "no longer held" in await brake.fire(load(state)["ap_1"])
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "released"


async def test_a_forged_warrant_never_reaches_the_broker(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    forged = _warrant()
    forged["max_qty"] = 10_000.0
    record(state, _rec(warrant=forged))
    assert "signature" in await brake.fire(load(state)["ap_1"])
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]


async def test_firing_is_one_shot(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.fire(load(state)["ap_1"])
    placed = [c for c in hub.calls if c[0] == "place_equity_order"]
    assert "already" in await brake.fire(load(state)["ap_1"])
    assert len([c for c in hub.calls if c[0] == "place_equity_order"]) == len(placed)


async def test_the_brake_fires_on_stale_policy(tmp_path):
    # The inversion. Every other path in the edge refuses here.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub, stale=True)
    record(state, _rec())
    assert await brake.fire(load(state)["ap_1"]) == ""
    assert hub.calls[-1][0] == "place_equity_order"


async def test_a_guardrail_denial_does_not_spend_the_warrant(tmp_path):
    # Nothing left the machine, so this is safe to retry once the owner fixes
    # the configuration. The position stays armed.
    hub = FakeHub(fail=GuardrailDenied("account not in the allow-list"))
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    assert "allow-list" in await brake.fire(load(state)["ap_1"])
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear that a position is unguarded"


async def test_an_ambiguous_failure_spends_the_warrant_and_never_retries(tmp_path):
    # Anything past the guardrails may have reached the broker. Retrying is
    # how you sell a position twice.
    hub = FakeHub(fail=RuntimeError("connection reset mid-call"))
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.fire(load(state)["ap_1"])
    assert load(state)["ap_1"]["state"] == "outcome_unknown"


async def test_a_successful_fire_tells_the_owner_and_the_platform(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.fire(load(state)["ap_1"])
    assert load(state)["ap_1"]["state"] == "fired"
    assert client.executions and client.executions[0][0] == "ap_1"
    assert any("AAPL" in m for m in client.messages)


# ---- the three corrections ------------------------------------------------


async def test_a_forged_symbol_in_the_sent_payload_is_caught(tmp_path, monkeypatch):
    # exit_order_args is what actually decides the bytes reaching the broker.
    # If IT names the wrong symbol, verification must catch that. Checking
    # against rec["symbol"] instead would let a construction bug sail through,
    # because the warrant would be checked against our own assumption rather
    # than the payload about to be sent.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())

    def wrong_symbol(cap, entry_args, qty):
        return {"account_number": "463605220", "symbol": "MSFT",
                "side": "sell", "quantity": float(qty), "order_type": "market"}

    monkeypatch.setattr(brake_module, "exit_order_args", wrong_symbol)
    reason = await brake.fire(load(state)["ap_1"])
    assert "symbol" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"


async def test_an_unparseable_held_now_response_does_not_release_the_position(tmp_path):
    # "I could not read the answer" must never be read as "the position is
    # gone": that would silently disarm the brake on a live holding.
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    reason = await brake.fire(load(state)["ap_1"])
    assert "not firing blind" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"


async def test_a_symbol_row_with_no_recognizable_quantity_key_does_not_release(tmp_path):
    hub = NoQtyKeyHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    reason = await brake.fire(load(state)["ap_1"])
    assert "not firing blind" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"


# ---- the position re-read goes through the connector's own map ------------
#
# Two brokers that agree on nothing. A re-read with a Robinhood tool name or a
# Robinhood argument key still baked into it passes the second of these and
# fails the first, which is the whole point of running both.


@pytest.mark.parametrize(
    "connector_id, account, tool, key, payload",
    [("alien-broker", "AL-1", "holdings", "acct",
      {"holdings": [{"ticker": "aapl", "qty": "25"}]}),
     ("robinhood-trading", "463605220", "get_equity_positions", "account_number",
      {"data": {"positions": [{"symbol": "AAPL", "quantity": "25"}]}})],
    ids=["alien", "robinhood"])
async def test_held_now_dials_each_connectors_own_position_tool(
        tmp_path, connector_id, account, tool, key, payload):
    # BOTH halves are asserted on purpose. Getting the tool right and the
    # argument key wrong is a real and silent failure: the broker is asked
    # about an account nobody named, and a perfectly readable answer that does
    # not contain our symbol reads as 0.0 held, which marks a live position
    # `released`. That is TERMINAL.
    hub = MappedHub({tool: payload})
    state, client, brake = _brake(tmp_path, hub)
    held = await brake._held_now(_rec(connector_id=connector_id,
                                      account=account))
    assert held == 25.0
    assert hub.calls == [(tool, {key: account})]


async def test_an_unreadable_position_answer_is_none_and_never_zero(tmp_path):
    # The distinction the whole function exists for, asserted on the function
    # itself rather than only through fire(). 0.0 means "the broker answered
    # and the symbol is not in the list", and fire() releases on it. None means
    # "there was no answer to read", and fire() refuses on it. A broker that
    # sent prose is the second one.
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    assert await brake._held_now(_rec()) is None


async def test_a_readable_list_without_our_symbol_is_zero_and_never_none(tmp_path):
    # The other side of the same coin: a genuinely readable list that simply
    # does not name the symbol IS evidence the position closed, and refusing
    # here would strand a closed position `armed` forever.
    hub = OtherSymbolsHub()
    state, client, brake = _brake(tmp_path, hub)
    assert await brake._held_now(_rec()) == 0.0


async def test_a_connector_that_declares_no_list_positions_asks_nothing(tmp_path):
    hub = NoListPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    assert await brake._held_now(_rec()) is None
    assert hub.calls == [], "with no map there is no question to ask"


async def test_a_connector_that_declares_no_list_positions_does_not_fire_blind(
        tmp_path):
    # The map is read at firing time, so it can be gone by then. An unmapped
    # position read must come out as a named refusal the owner hears, never as
    # a release and never as an uncaught raise.
    hub = NoListPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    reason = await brake.fire(load(state)["ap_1"])
    assert "not firing blind" in reason
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear the position is unguarded"
    assert "denial" in [e["kind"] for e in brake.audit.pending()]


# ---- fix round 1: one-shot under concurrency, and loud refusals ----------


async def test_two_concurrent_fires_place_only_one_order(tmp_path):
    # Two coroutines, each holding its OWN independently-loaded snapshot of
    # the same armed record, exactly the shape a future runtime loop and a
    # manually-triggered pass could produce together. Only an atomic claim on
    # the disk state (not the in-memory `armed` each coroutine already read)
    # can stop the second one from also reaching the broker.
    hub = RacingHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    rec_a, rec_b = load(state)["ap_1"], load(state)["ap_1"]
    results = await asyncio.gather(brake.fire(rec_a), brake.fire(rec_b))
    placed = [c for c in hub.calls if c[0] == "place_equity_order"]
    assert len(placed) == 1, f"expected exactly one live order, placed {len(placed)}"
    assert "" in results, "one of the two calls must still have succeeded"
    losers = [r for r in results if r]
    assert losers and "already claimed" in losers[0]
    assert load(state)["ap_1"]["state"] == "fired"


async def test_the_owner_hears_when_the_broker_will_not_confirm_the_position(tmp_path):
    # held is None is the worst case in the module: the stop may have just
    # been touched and the brake is choosing not to act, so silence here is
    # not caution, it is a position nobody is told about.
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.fire(load(state)["ap_1"])
    assert client.messages, "the owner must hear the position is unguarded"
    kinds = [e["kind"] for e in brake.audit.pending()]
    assert "denial" in kinds


# ---- fix round 2: a non-finite warrant ceiling must never widen a sale ---
#
# `min(held, nan)` returns `held` (every NaN comparison is False), so a NaN
# max_qty would otherwise remove the ceiling entirely for that sale. This is
# the earliest of three layers now guarding against it (fire()'s clamp,
# warrant.authorizes()'s comparison, supervision.apply_renewals's intake);
# see tests/test_warrant.py and tests/test_edge_warrant_renewal.py for the
# other two.

async def test_fire_refuses_a_nan_warrant_ceiling(tmp_path):
    hub = FakeHub(held=500.0)
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec(warrant=_warrant(max_qty=float("nan"))))
    reason = await brake.fire(load(state)["ap_1"])
    assert "not a usable number" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear the position is unguarded"


async def test_fire_refuses_a_negative_infinite_warrant_ceiling(tmp_path):
    hub = FakeHub(held=500.0)
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec(warrant=_warrant(max_qty=float("-inf"))))
    reason = await brake.fire(load(state)["ap_1"])
    assert "not a usable number" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"


async def test_a_connector_that_declares_no_place_order_refuses_by_name(tmp_path):
    # The map is read at firing time, so it can be gone by then. An uncaught
    # CapabilityError here would leave the position `armed` with its stop
    # already touched, no order placed and nothing said: the one failure of
    # the brake nobody would see. It has to come out as a named refusal.
    hub = NoPlaceOrderHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    reason = await brake.fire(load(state)["ap_1"])
    assert "place_order" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear the position is unguarded"
    assert "denial" in [e["kind"] for e in brake.audit.pending()]


async def test_the_owner_hears_when_the_connector_cannot_express_an_exit(tmp_path):
    hub = NoMarketExitHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    reason = await brake.fire(load(state)["ap_1"])
    assert "cannot express a market exit" in reason
    assert [c[0] for c in hub.calls] == ["get_equity_positions"]
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear the position is unguarded"
    kinds = [e["kind"] for e in brake.audit.pending()]
    assert "denial" in kinds


# ---- final round: an account-less record is never a broker question -------
#
# Task 4's rule: a record whose account cannot be named must never be acted
# on. Asking a broker about account "" is not a degraded read, it is a
# different question, and a broker that answers it with a list not containing
# the symbol turns a fully-held position into `released`, which is TERMINAL.


async def test_fire_refuses_an_account_less_record_without_asking_the_broker(tmp_path):
    hub = OtherSymbolsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec(account=""))
    reason = await brake.fire(load(state)["ap_1"])
    assert "account" in reason
    assert hub.calls == [], "the broker must never be asked about account ''"
    assert load(state)["ap_1"]["state"] == "armed"
    assert client.messages, "the owner must hear the position is unguarded"
    assert "denial" in [e["kind"] for e in brake.audit.pending()]


# ---- final round: the owner is told, not buried ---------------------------
#
# One unfireable position is retried every 15 seconds forever, so an
# unthrottled message per refusal is roughly 118 owner messages an hour about
# a single name. Spec section 9 asks for backoff and an alert after a few
# failures. The journal must stay complete either way: it is the record of
# what happened, and it is not the thing an owner learns to ignore.


async def test_a_refusal_repeated_every_tick_is_told_once(tmp_path):
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    for i in range(6):
        await brake.fire(load(state)["ap_1"], now=1000.0 + i * 15.0)
    assert len(client.messages) == 1
    denials = [e for e in brake.audit.pending() if e["kind"] == "denial"]
    assert len(denials) == 6, "the journal keeps every refusal"


async def test_the_owner_hears_again_once_the_backoff_has_run_out(tmp_path):
    # Still unguarded an hour later is news again, not the same news.
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.fire(load(state)["ap_1"], now=1000.0)
    await brake.fire(load(state)["ap_1"], now=1000.0 + 15.0)
    await brake.fire(load(state)["ap_1"],
                     now=1000.0 + brake_module.NOTIFY_BACKOFF_S + 1)
    assert len(client.messages) == 2


async def test_a_second_distinct_reason_on_one_position_still_reaches_the_owner(
        tmp_path, monkeypatch):
    # Suppression is per reason as well as per position: the next thing the
    # owner needs to hear must never be swallowed by the last thing they were
    # told.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec(warrant=_warrant(max_qty=float("nan"))))
    await brake.fire(load(state)["ap_1"], now=1000.0)
    record(state, _rec())                       # a usable ceiling now
    monkeypatch.setattr(brake_module, "exit_order_args", lambda *a: None)
    await brake.fire(load(state)["ap_1"], now=1015.0)
    assert len(client.messages) == 2


async def test_the_platform_is_never_called_on_the_event_loop(tmp_path):
    # PlatformClient is synchronous on purpose (see client.py), and every other
    # async caller wraps it in asyncio.to_thread. Not wrapping it here stalls
    # all five background loops AND the MCP server for up to the client
    # timeout, in precisely the scenario the brake exists for: a dark platform.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub, client=ThreadRecordingClient())
    record(state, _rec())
    assert await brake.fire(load(state)["ap_1"]) == ""
    assert len(client.threads) == 2, "report_execution and send_message both ran"
    assert threading.get_ident() not in client.threads


async def test_a_refusal_message_is_also_kept_off_the_event_loop(tmp_path):
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub, client=ThreadRecordingClient())
    record(state, _rec())
    await brake.fire(load(state)["ap_1"])
    assert client.threads and threading.get_ident() not in client.threads


async def test_two_positions_failing_the_same_way_are_both_reported(tmp_path):
    hub = MalformedPositionsHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    record(state, _rec(position_id="ap_2"))
    await brake.fire(load(state)["ap_1"], now=1000.0)
    await brake.fire(load(state)["ap_2"], now=1000.0)
    assert len(client.messages) == 2


# ---- the tick: observation to firing --------------------------------------
#
# tick() takes the FULL normalized quote per symbol (ts, bid, ask included),
# never a bare price: usable() needs the real receipt time and the book to do
# its job, and an observation restated by its consumer is an observation
# whose provenance has been erased.


def _q(price, ts=1.0, bid=None, ask=None):
    return {"price": price, "bid": bid, "ask": ask, "ts": ts}


async def test_a_tick_needs_two_confirmed_breaches_before_it_fires(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    assert await brake.tick({"AAPL": _q(46.10, ts=1.0)}, 1.0) == []
    assert await brake.tick({"AAPL": _q(46.10, ts=16.0)}, 16.0) == ["ap_1"]


async def test_a_tick_carries_a_real_book_through_to_usable(tmp_path):
    # Proves bid/ask actually survive the trip into usable(): a spread this
    # tight must not block a legitimate breach.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    q1 = _q(46.10, ts=1.0, bid=46.09, ask=46.11)
    q2 = _q(46.10, ts=16.0, bid=46.09, ask=46.11)
    assert await brake.tick({"AAPL": q1}, 1.0) == []
    assert await brake.tick({"AAPL": q2}, 16.0) == ["ap_1"]


async def test_a_price_above_the_stop_resets_the_run(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    await brake.tick({"AAPL": _q(46.10, ts=1.0)}, 1.0)
    await brake.tick({"AAPL": _q(46.90, ts=16.0)}, 16.0)
    assert await brake.tick({"AAPL": _q(46.10, ts=31.0)}, 31.0) == []


async def test_a_disarmed_position_never_fires(tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    set_local_disarm(state, position_id="ap_1")
    await brake.tick({"AAPL": _q(46.10, ts=1.0)}, 1.0)
    assert await brake.tick({"AAPL": _q(46.10, ts=16.0)}, 16.0) == []


async def test_an_unguarded_position_is_never_ticked(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec(warrant=None, state="unguarded"))
    await brake.tick({"AAPL": _q(46.10, ts=1.0)}, 1.0)
    assert await brake.tick({"AAPL": _q(46.10, ts=16.0)}, 16.0) == []


async def test_a_quote_famine_reaches_the_owner_once(tmp_path):
    # A quote read the edge's own guardrails deny is indistinguishable from a
    # working brake from outside: no price, no breach, no fire, and a display
    # that still says guarded. Only the elapsed silence tells them apart, so
    # the silence itself has to reach the owner. Then it must go quiet: 118
    # messages an hour is how an owner learns to ignore the channel.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    ticks = brake_module.QUOTE_FAMINE_TICKS
    for i in range(ticks - 1):
        assert await brake.tick({}, 1.0 + i * 15.0) == []
    assert client.messages == [], "not yet: a single dry tick is routine"

    assert await brake.tick({}, 1.0 + (ticks - 1) * 15.0) == []
    assert len(client.messages) == 1
    assert "AAPL" in client.messages[0]
    assert "error" in [e["kind"] for e in brake.audit.pending()]

    for i in range(ticks, ticks + 6):
        await brake.tick({}, 1.0 + i * 15.0)
    assert len(client.messages) == 1, "the famine is reported once, not per tick"


async def test_quotes_resuming_re_arms_the_famine_signal(tmp_path):
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    ticks = brake_module.QUOTE_FAMINE_TICKS
    for i in range(ticks):
        await brake.tick({}, 1.0 + i * 15.0)
    assert len(client.messages) == 1

    resumed = 1.0 + ticks * 15.0
    await brake.tick({"AAPL": _q(46.90, ts=resumed)}, resumed)
    for i in range(ticks):
        await brake.tick({}, resumed + 15.0 + i * 15.0)
    assert len(client.messages) == 2, "a second famine is a second event"


async def test_a_quote_discarded_every_tick_counts_as_a_famine(tmp_path):
    # usable() discarding every observation leaves the brake exactly as blind
    # as no quote at all, and just as quiet toward the owner.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    ticks = brake_module.QUOTE_FAMINE_TICKS
    for i in range(ticks):
        now = 1.0 + i * 15.0
        # Unstamped: refused by usable() on every pass, never counted.
        await brake.tick({"AAPL": _q(46.10, ts=0.0)}, now)
    assert len(client.messages) == 1
    assert "AAPL" in client.messages[0]


async def test_a_stale_quote_never_fires_the_brake_through_tick(tmp_path):
    # Proves the freshness check in usable() is actually reachable FROM
    # tick(), not just exercised by unit tests of usable() in isolation. If
    # tick() restamped every quote with its own `now` (the discarded first
    # draft), this same stale observation would look fresh on every pass and
    # would confirm-and-fire on the second tick below.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())
    stale = _q(46.10, ts=1.0)
    past_max_age = 1.0 + brake_module.QUOTE_MAX_AGE_S + 1
    assert await brake.tick({"AAPL": stale}, past_max_age) == []
    assert await brake.tick({"AAPL": stale}, past_max_age + 15.0) == []
