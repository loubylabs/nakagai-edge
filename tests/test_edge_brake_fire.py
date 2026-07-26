"""Firing: the one path where the brake touches the broker.

The bounds pinned here are the whole reason a standing warrant is safe. Note
two inversions of the edge's usual posture, both deliberate: the brake fires on
STALE POLICY and with the KILL SWITCH engaged, because its authority came from
a signed artifact rather than from cached policy, and because reducing exposure
is the safe direction.
"""

import asyncio

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.config import ConnectorSpec, GuardrailsConfig, OrderShape
from nakagai_edge.edge import brake as brake_module
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import load, record
from nakagai_edge.edge.sync import apply_bundle
from nakagai_edge.hub import GuardrailDenied
from nakagai_edge.signing import generate_keypair, sign_artifact
from nakagai_edge.warrant import TRIGGER_BELOW, build_warrant_payload

pytestmark = pytest.mark.anyio

PRIV, PUB = generate_keypair()

SHAPE = OrderShape(
    symbol_keys=["symbol"], side_keys=["side"], quantity_keys=["quantity"],
    price_keys=["limit_price"], stop_keys=["stop_price"],
    stock_tools=["place_equity_order"],
    market_order_args={"order_type": "market"})

ENTRY_ARGS = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
              "quantity": 100, "limit_price": 47.55, "stop_price": 46.20}


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeHub:
    def __init__(self, held=100.0, fail=None):
        self.calls = []
        self.held = held
        self.fail = fail
        self._spec = ConnectorSpec(
            id="demo", kind="mcp-http", role="broker",
            guardrails=GuardrailsConfig(order_shape=SHAPE))

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
    """The symbol's row is there, but under a quantity key the brake does not
    recognize."""

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, args, kw))
        if tool == "get_equity_positions":
            return {"data": {"positions": [
                {"symbol": "AAPL", "lot_size": "100"}]}}
        if self.fail is not None:
            raise self.fail
        return {"is_error": False, "data": {"order_id": "42"}}


NO_MARKET_SHAPE = OrderShape(
    symbol_keys=["symbol"], side_keys=["side"], quantity_keys=["quantity"],
    price_keys=["limit_price"], stop_keys=["stop_price"],
    stock_tools=["place_equity_order"])  # no market_order_args declared


class NoMarketExitHub(FakeHub):
    """A connector that never declared how to say "market order"."""

    def __init__(self, held=100.0, fail=None):
        super().__init__(held=held, fail=fail)
        self._spec = ConnectorSpec(
            id="demo", kind="mcp-http", role="broker",
            guardrails=GuardrailsConfig(order_shape=NO_MARKET_SHAPE))


class RacingHub(FakeHub):
    """FakeHub's call() never actually suspends, so two gathered coroutines
    calling it run fully sequentially -- no interleaving, no race, whether or
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


def _brake(tmp_path, hub, *, stale=False):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1",
                         "connectors": {"connectors": []}, "watchlist": [],
                         "mandate": {}, "strategy_configs": {},
                         "signing_public_key": PUB}, "v1")
    if stale:
        state.meta_path.write_text('{"etag": "v1", "fetched_at": 1.0}')
    client = FakeClient()
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
    # If IT names the wrong symbol, verification must catch that -- checking
    # against rec["symbol"] instead would let a construction bug sail through,
    # because the warrant would be checked against our own assumption rather
    # than the payload about to be sent.
    hub = FakeHub()
    state, client, brake = _brake(tmp_path, hub)
    record(state, _rec())

    def wrong_symbol(shape, entry_args, qty):
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


# ---- fix round 1: one-shot under concurrency, and loud refusals ----------


async def test_two_concurrent_fires_place_only_one_order(tmp_path):
    # Two coroutines, each holding its OWN independently-loaded snapshot of
    # the same armed record -- exactly the shape a future runtime loop and a
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
