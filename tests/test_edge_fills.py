"""The fill journal: the one path carrying trades the AGENT did not place.

Run against two brokers that agree on nothing, the same way the portfolio
sweep's tests are. Robinhood wraps every response in its own envelope, declares
both enrichment fields and a word for `filled`; the alien fixture wraps nothing,
spells everything differently, and declares neither. A sweep with any broker's
vocabulary baked into it passes one and fails the other.
"""

import json

import pytest

from nakagai_edge.config import load_specs
from nakagai_edge.edge.fills import (Anchor, FillsReporter, account_rows,
                                     filled_status, key, open_order_rows)
from nakagai_edge.edge.state import EdgeState
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio

SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})


@pytest.fixture
def anyio_backend():
    return "asyncio"


class EnvelopeHub:
    """hub.call's contract with Robinhood's own {"data", "guide"} envelope
    nested inside it, exactly as the real connector answers."""

    def __init__(self, responses):
        self.account_key = "ag1"
        self.responses, self.calls = responses, []

    def spec(self, connector_id):
        return SPECS[connector_id]

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        out = self.responses[tool]
        if isinstance(out, Exception):
            raise out
        return {"data": {"data": out, "guide": "ignore me"}}


class FlatHub:
    """The same contract with no envelope, for the alien broker."""

    def __init__(self, responses):
        self.account_key = "ag1"
        self.responses, self.calls = responses, []

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        out = self.responses[tool]
        if isinstance(out, Exception):
            raise out
        return {"data": out}


def _rh_hub(orders):
    return EnvelopeHub({
        "get_accounts": {"accounts": [{"account_number": "463605220"}]},
        "get_equity_orders": {"orders": orders},
    })


ORDER = {"id": "ord-1", "symbol": "nvda", "side": "buy", "quantity": "60",
         "state": "filled", "average_price": "112.40",
         "last_transaction_at": "2026-08-05T14:31:00Z"}


# ---- reading a row -------------------------------------------------------

async def test_a_row_is_read_into_canonical_fields_through_the_map():
    spec = SPECS["robinhood-trading"]
    rows = await account_rows(_rh_hub([ORDER]), spec, "463605220")
    assert rows == [{"order_id": "ord-1", "symbol": "NVDA", "side": "buy",
                     "quantity": 60.0, "status": "filled",
                     "fill_price": 112.40,
                     "filled_at": "2026-08-05T14:31:00Z"}]


async def test_the_alien_broker_reads_the_same_shape_from_its_own_spelling():
    spec = SPECS["alien-broker"]
    hub = FlatHub({"accounts": [{"acct": "AL-1"}],
                   "orders": {"working": [
                       {"ref": "x-9", "tkr": "spy", "action": "SELL",
                        "qty": "5", "stage": "FILLED"}]}})
    rows = await account_rows(hub, spec, "AL-1")
    assert rows == [{"order_id": "x-9", "symbol": "SPY", "side": "sell",
                     "quantity": 5.0, "status": "FILLED"}]


async def test_a_row_with_no_order_id_is_dropped():
    # The inverse of portfolio._positions, which KEEPS an unreadable row so a
    # live position cannot look closed. A fill has no such safety role: with no
    # order_id it can never be deduped and never joined, so keeping it would
    # re-ship it on every sweep forever.
    spec = SPECS["robinhood-trading"]
    rows = await account_rows(_rh_hub([{**ORDER, "id": None}, ORDER]), spec, "1")
    assert [r["order_id"] for r in rows] == ["ord-1"]


async def test_status_is_relayed_verbatim_not_canonicalized():
    # Brokers disagree about what their states mean. Flattening them here would
    # lose a distinction the platform may need.
    spec = SPECS["alien-broker"]
    hub = FlatHub({"orders": {"working": [
        {"ref": "x-9", "tkr": "spy", "stage": "partially_filled"}]}})
    rows = await account_rows(hub, spec, "AL-1")
    assert rows[0]["status"] == "partially_filled"


async def test_open_orders_share_the_canonical_list_orders_reader():
    """Changing the shared parser to skip side, quantity, or status must fail
    this test and the fill-journal test above together.

    The portfolio report asks for the broker's default open set, while the
    journal asks for its declared filled spelling. Both must still normalize
    one broker payload through one parser.
    """
    spec = SPECS["robinhood-trading"]
    queued = {**ORDER, "state": "queued"}
    hub = _rh_hub([queued])

    rows = await open_order_rows(hub, spec, "463605220")

    assert rows == [{"order_id": "ord-1", "symbol": "NVDA", "side": "buy",
                     "quantity": 60.0, "status": "queued",
                     "fill_price": 112.40,
                     "filled_at": "2026-08-05T14:31:00Z"}]
    assert "state" not in hub.calls[0][2]


async def test_a_malformed_open_order_is_explicit_unknown_evidence():
    """Dropping this row would turn an unreadable broker answer into an empty
    working-order book, which is the unsafe direction for a new entry."""
    spec = SPECS["robinhood-trading"]

    rows = await open_order_rows(
        _rh_hub([{"symbol": "aapl", "side": "buy", "state": "queued"}]),
        spec, "463605220")

    assert rows == [{"symbol": "AAPL", "side": "buy", "status": "queued",
                     "unknown": True}]


# ---- asking for filled orders --------------------------------------------

async def test_a_declared_filled_status_is_sent_in_the_brokers_own_spelling():
    """Both halves of "its own spelling": the VALUE (`filled`) and the KEY it
    travels under. Robinhood's filter parameter is `state`, so a map declaring
    `status` sent a key the broker ignores, which reads as an unfiltered sweep
    rather than an error."""
    spec = SPECS["robinhood-trading"]
    hub = _rh_hub([])
    await account_rows(hub, spec, "463605220")
    assert hub.calls[0][2]["state"] == "filled"
    assert "status" not in hub.calls[0][2]


async def test_a_broker_declaring_no_filled_status_is_asked_for_its_default():
    # No status key at all, rather than a guess. The (order_id, status) keying
    # is what makes journaling the default set correct anyway.
    spec = SPECS["alien-broker"]
    hub = FlatHub({"orders": {"working": []}})
    await account_rows(hub, spec, "AL-1")
    assert "state" not in hub.calls[0][2]


def test_filled_status_reads_the_map_and_is_empty_when_undeclared():
    assert filled_status(SPECS["robinhood-trading"].capability("list_orders")) == "filled"
    assert filled_status(SPECS["alien-broker"].capability("list_orders")) == ""


# ---- the anchor ----------------------------------------------------------

def test_an_unanchored_account_is_none_and_an_empty_one_is_a_set(tmp_path):
    # Different facts: never swept, against swept and the broker held nothing.
    # Only the first suppresses shipping, so they must not collapse.
    a = Anchor(tmp_path)
    assert a.known("c\x1f1") is None
    a.anchor("c\x1f1", [])
    assert a.known("c\x1f1") == set()


def test_first_sight_happens_exactly_once(tmp_path):
    a = Anchor(tmp_path)
    a.anchor("c\x1f1", ["a", "b"])
    a.anchor("c\x1f1", ["z"])
    assert a.known("c\x1f1") == {"a", "b"}


def test_an_unreadable_anchor_file_does_not_raise(tmp_path):
    a = Anchor(tmp_path)
    a.path.parent.mkdir(parents=True, exist_ok=True)
    a.path.write_text("{not json")
    assert a.known("c\x1f1") is None


# ---- the sweep -----------------------------------------------------------

class Client:
    def __init__(self, fail=False):
        self.batches, self.outcomes, self.alerts, self.fail = [], [], [], fail

    def report_fills(self, fills):
        if self.fail:
            raise RuntimeError("platform down")
        self.batches.append(fills)
        return {"ok": True}

    def report_candidate_outcome(self, candidate_id, **payload):
        self.outcomes.append((candidate_id, payload))
        return {
            "candidate_id": candidate_id,
            "mechanical_status": payload["mechanical_status"],
            "approval_id": payload["approval_id"],
        }

    def agent_checkin(self, status, note, **payload):
        self.alerts.append((status, note, payload))
        return {"ok": True}


def _state(tmp_path, connectors):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(
        json.dumps({"connectors": connectors}))
    return EdgeState(tmp_path)


def _rh_only(tmp_path):
    return _state(tmp_path, [ROBINHOOD_CONNECTOR])


async def test_the_first_sweep_anchors_and_ships_nothing(tmp_path):
    # The whole privacy promise: an account already full of the owner's trading
    # uploads none of it.
    client = Client()
    r = FillsReporter(_rh_only(tmp_path), _rh_hub([ORDER]), client)
    assert await r.sweep() == []
    assert client.batches == []


async def test_fill_sweep_completes_exact_submitted_candidate_before_anchoring(
        tmp_path):
    state = _rh_only(tmp_path)
    state._write_private(state.intents_path, {
        "approval-1": {
            "connector_id": "robinhood-trading",
            "tool": "place_equity_order",
            "args": {
                "account_number": "463605220", "symbol": "NVDA",
                "side": "buy", "type": "limit", "quantity": "60",
                "limit_price": "112.35", "stop_price": "109.50",
                "time_in_force": "day",
            },
            "args_hash": "frozen", "candidate_id": "candidate-1",
            "account": "463605220", "phase": "submitted",
            "broker_order_id": "ord-1",
            "approval": {
                "signal_id": "signal-1",
                "exit_warrant": {
                    "expires_at": 4_102_444_800.0, "signature": "warrant"},
            },
            "broker_result": {"data": {"order_id": "ord-1"}},
        },
    })
    hub = EnvelopeHub({
        "get_accounts": {"accounts": [{"account_number": "463605220"}]},
        "get_equity_orders": {"orders": [ORDER]},
        "get_equity_positions": {"positions": [
            {"symbol": "NVDA", "quantity": "60"}]},
    })
    client = Client()

    await FillsReporter(state, hub, client).sweep()

    from nakagai_edge.edge.remote import intents
    from nakagai_edge.edge.supervision import load
    assert intents(state) == {}
    assert load(state)["approval-1"]["state"] == "armed"
    assert client.outcomes == []


async def test_fill_sweep_queries_declared_terminal_states_for_submitted_order(
        tmp_path):
    state = _state(tmp_path, [ROBINHOOD_CONNECTOR])
    state._write_private(state.intents_path, {
        "approval-1": {
            "connector_id": "robinhood-trading",
            "tool": "place_equity_order",
            "args": {
                "account_number": "463605220", "symbol": "NVDA",
                "side": "buy", "type": "limit", "quantity": "60",
                "limit_price": "112.35", "stop_price": "109.50",
                "time_in_force": "day",
            },
            "args_hash": "frozen", "candidate_id": "candidate-1",
            "account": "463605220", "phase": "submitted",
            "broker_order_id": "ord-1", "approval": {},
            "broker_result": {"data": {"order_id": "ord-1"}},
        },
    })

    class TerminalHub(EnvelopeHub):
        async def call(self, connector_id, tool, args, **kw):
            self.calls.append((connector_id, tool, dict(args)))
            if tool == "get_accounts":
                out = {"accounts": [{"account_number": "463605220"}]}
            elif args.get("state") == "cancelled":
                out = {"orders": [{
                    "id": "ord-1", "symbol": "NVDA", "side": "buy",
                    "quantity": "60", "state": "cancelled",
                }]}
            else:
                out = {"orders": []}
            return {"data": {"data": out, "guide": "ignore me"}}

    hub = TerminalHub({})
    client = Client()

    await FillsReporter(state, hub, client).sweep()

    from nakagai_edge.edge.remote import intents
    assert intents(state) == {}
    assert client.outcomes == []
    queried = [args.get("state") for _connector, tool, args in hub.calls
               if tool == "get_equity_orders"]
    assert queried == ["filled", "cancelled"]


async def test_only_what_appears_after_the_anchor_is_journaled(tmp_path):
    client = Client()
    state = _rh_only(tmp_path)
    later = {**ORDER, "id": "ord-2", "symbol": "aapl"}
    r = FillsReporter(state, _rh_hub([ORDER]), client)
    await r.sweep()
    r._hub = _rh_hub([ORDER, later])
    journaled = await r.sweep()
    assert [row["order_id"] for row in journaled] == ["ord-2"]
    assert [row["order_id"] for row in client.batches[0]] == ["ord-2"]


async def test_a_status_change_ships_a_second_time(tmp_path):
    # Keyed on (order_id, status), not order_id. Otherwise an order first seen
    # working is recorded queued forever and the journal never learns it filled.
    client = Client()
    state = _rh_only(tmp_path)
    working = {**ORDER, "id": "ord-3", "state": "queued"}
    r = FillsReporter(state, _rh_hub([]), client)
    await r.sweep()                                   # anchor on an empty account
    r._hub = _rh_hub([working])
    await r.sweep()
    r._hub = _rh_hub([{**working, "state": "filled"}])
    await r.sweep()
    shipped = [row["status"] for batch in client.batches for row in batch]
    assert shipped == ["queued", "filled"]


async def test_the_same_row_twice_ships_once(tmp_path):
    client = Client()
    state = _rh_only(tmp_path)
    r = FillsReporter(state, _rh_hub([]), client)
    await r.sweep()
    r._hub = _rh_hub([ORDER])
    await r.sweep()
    await r.sweep()
    assert sum(len(b) for b in client.batches) == 1


async def test_the_seen_set_survives_a_restart(tmp_path):
    # Rebuilt from the journal, ignoring the watermark: an order the platform
    # has already confirmed must not be journaled again.
    client = Client()
    state = _rh_only(tmp_path)
    r = FillsReporter(state, _rh_hub([]), client)
    await r.sweep()
    r._hub = _rh_hub([ORDER])
    await r.sweep()
    again = FillsReporter(state, _rh_hub([ORDER]), client)
    assert await again.sweep() == []


async def test_a_broker_that_will_not_answer_neither_anchors_nor_journals(tmp_path):
    # A failed read proves nothing about what the broker holds. Anchoring on it
    # would anchor an empty set, and the whole history would ship next cycle.
    client = Client()
    state = _rh_only(tmp_path)
    hub = EnvelopeHub({"get_accounts": RuntimeError("broker down")})
    r = FillsReporter(state, hub, client)
    assert await r.sweep() == []
    assert Anchor(tmp_path).known("robinhood-trading\x1f463605220") is None


async def test_an_empty_account_still_anchors_so_its_first_trade_is_journaled(tmp_path):
    # The regression this cost an hour to find. A brand-new brokerage account
    # holds no orders at first sight. If "no rows" meant "no account", it would
    # never anchor, and the owner's very first fill would arrive at an
    # unanchored account, be recorded as history, and never ship.
    client = Client()
    state = _rh_only(tmp_path)
    r = FillsReporter(state, _rh_hub([]), client)
    await r.sweep()
    assert Anchor(tmp_path).known("robinhood-trading\x1f463605220") == set()
    r._hub = _rh_hub([ORDER])
    assert [row["order_id"] for row in await r.sweep()] == ["ord-1"]


async def test_a_connector_declaring_no_list_orders_is_skipped(tmp_path):
    # Not misconfigured: it simply does not offer order history.
    no_orders = {**ROBINHOOD_CONNECTOR,
                 "capabilities": {k: v for k, v
                                  in ROBINHOOD_CONNECTOR["capabilities"].items()
                                  if k != "list_orders"}}
    r = FillsReporter(_state(tmp_path, [no_orders]), _rh_hub([ORDER]), Client())
    assert await r.sweep() == []


async def test_a_down_platform_keeps_the_journal_for_the_next_cycle(tmp_path):
    state = _rh_only(tmp_path)
    r = FillsReporter(state, _rh_hub([]), Client(fail=True))
    await r.sweep()
    r._hub = _rh_hub([ORDER])
    await r.sweep()                                   # journals, fails to ship
    good = Client()
    r._client = good
    await r.sweep()
    assert [row["order_id"] for row in good.batches[0]] == ["ord-1"]


def test_the_seen_key_separates_id_from_status():
    # A separator no broker id or status realistically contains, so "a" + "bc"
    # and "ab" + "c" cannot collide into one key.
    assert key({"order_id": "a", "status": "bc"}) != key({"order_id": "ab",
                                                          "status": "c"})
