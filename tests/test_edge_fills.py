"""The fill journal: the one path carrying trades the AGENT did not place.

Run against two brokers that agree on nothing, the same way the portfolio
sweep's tests are. Robinhood wraps every response in its own envelope, declares
both enrichment fields and a word for `filled`; the alien fixture wraps nothing,
spells everything differently, and declares neither. A sweep with any broker's
vocabulary baked into it passes one and fails the other.
"""

import json
import time

import pytest

from nakagai_edge.config import load_specs
from nakagai_edge.edge.fills import (Anchor, FillsReporter, HistoryMarker,
                                     account_rows, filled_status, key,
                                     open_order_rows)
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
        self.batches, self.alerts, self.fail = [], [], fail

    def report_fills(self, fills):
        if self.fail:
            raise RuntimeError("platform down")
        self.batches.append(fills)
        return {"ok": True}

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


async def test_fill_sweep_completes_exact_submitted_proposal_before_anchoring(
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
            "args_hash": "frozen", "proposal_id": "proposal-1",
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
    assert client.alerts == []


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
            "args_hash": "frozen", "proposal_id": "proposal-1",
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
    assert client.alerts == []
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


# ---- the history window --------------------------------------------------
#
# The one named exception to first-sight anchoring. Off unless the owner turns
# it on for a broker; then that window, and nothing older, ships once.

DAY = 86400


def _iso(seconds_ago):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(time.time() - seconds_ago,
                                  tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _order(oid, days_ago, **extra):
    stamp = _iso(days_ago * DAY)
    return {**ORDER, "id": oid, "created_at": stamp,
            "last_transaction_at": stamp, **extra}


class HistoryHub(EnvelopeHub):
    """A broker that honors its creation-time filter and pages by a token,
    `page_size` orders a page, newest first, the way the real one answers."""

    def __init__(self, orders, page_size=2, fail_history=False):
        super().__init__({})
        self.orders, self.page_size = orders, page_size
        self.fail_history = fail_history

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        if tool == "get_accounts":
            out = {"accounts": [{"account_number": "463605220"}]}
        elif "created_at_gte" not in args:
            out = {"orders": self.orders}        # the regular sweep's one page
        else:
            if self.fail_history:
                raise RuntimeError("broker down")
            since = args["created_at_gte"]
            rows = [o for o in self.orders if o["created_at"] >= since]
            start = int(args.get("cursor") or 0)
            page = rows[start:start + self.page_size]
            more = start + self.page_size < len(rows)
            out = {"orders": page,
                   "next": str(start + self.page_size) if more else None}
        return {"data": {"data": out, "guide": "ignore me"}}

    def history_calls(self):
        return [args for _c, tool, args in self.calls
                if tool == "get_equity_orders" and "created_at_gte" in args]


def _grant(state, days, connector_id="robinhood-trading"):
    state.bundle_path.parent.mkdir(parents=True, exist_ok=True)
    state.bundle_path.write_text(json.dumps(
        {"schema_version": 2,
         "trade_history": {connector_id: {"days": days}} if days else {}}))


def _shipped(client):
    return [row["order_id"] for batch in client.batches for row in batch]


ACCOUNT_KEY = "robinhood-trading\x1f463605220"


async def test_no_grant_ships_nothing_and_never_asks_for_history(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 0)
    hub = HistoryHub([_order("a", 1), _order("b", 40)])
    client = Client()
    r = FillsReporter(state, hub, client)
    await r.sweep()
    await r.sweep()
    assert client.batches == []
    assert hub.history_calls() == []
    assert HistoryMarker(tmp_path).days(ACCOUNT_KEY) == 0


async def test_a_grant_ships_the_window_and_nothing_older(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 30)
    orders = [_order("in-1", 1), _order("in-2", 3, state="cancelled"),
              _order("in-3", 29), _order("old-1", 31), _order("old-2", 200)]
    hub = HistoryHub(orders)
    client = Client()
    await FillsReporter(state, hub, client).sweep()
    assert sorted(_shipped(client)) == ["in-1", "in-2", "in-3"]
    # Every order on first sight is still anchored, so the old ones stay put.
    assert len(Anchor(tmp_path).known(ACCOUNT_KEY)) == 5


async def test_the_backfill_pages_and_asks_with_no_status_filter(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 90)
    hub = HistoryHub([_order(f"o{i}", i) for i in range(5)], page_size=2)
    await FillsReporter(state, hub, Client()).sweep()
    calls = hub.history_calls()
    assert [c.get("cursor") for c in calls] == [None, "2", "4"]
    assert all("state" not in c for c in calls)
    assert calls[0]["created_at_gte"].endswith("Z")
    assert calls[0]["account_number"] == "463605220"


async def test_the_window_opens_days_ago_in_utc(tmp_path):
    from nakagai_edge.edge.fills import history_since
    assert history_since(30, 86400 * 31) == "1970-01-02T00:00:00Z"


async def test_an_anchored_row_inside_the_window_ships_exactly_once(tmp_path):
    state = _rh_only(tmp_path)
    orders = [_order("a", 2), _order("old", 60)]
    hub = HistoryHub(orders)
    client = Client()
    r = FillsReporter(state, hub, client)
    await r.sweep()                                   # anchors both, no grant
    assert client.batches == []
    _grant(state, 30)
    await r.sweep()
    await r.sweep()
    again = FillsReporter(state, hub, client)         # and across a restart
    await again.sweep()
    assert _shipped(client) == ["a"]


async def test_the_marker_moves_only_after_a_complete_backfill(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 30)
    hub = HistoryHub([_order("a", 2)], fail_history=True)
    client = Client()
    r = FillsReporter(state, hub, client)
    await r.sweep()
    assert HistoryMarker(tmp_path).days(ACCOUNT_KEY) == 0
    assert client.batches == []
    hub.fail_history = False
    await r.sweep()
    assert HistoryMarker(tmp_path).days(ACCOUNT_KEY) == 30
    assert _shipped(client) == ["a"]
    calls = len(hub.history_calls())
    await r.sweep()                                   # done: not read again
    assert len(hub.history_calls()) == calls


async def test_an_unreadable_history_page_moves_no_marker(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 30)

    class BrokenPage(HistoryHub):
        async def call(self, connector_id, tool, args, **kw):
            out = await super().call(connector_id, tool, args, **kw)
            if args.get("cursor") == "2":
                out["data"]["data"]["orders"] = "not a list"
            return out

    hub = BrokenPage([_order(f"o{i}", i) for i in range(4)], page_size=2)
    client = Client()
    await FillsReporter(state, hub, client).sweep()
    assert HistoryMarker(tmp_path).days(ACCOUNT_KEY) == 0
    assert client.batches == []


async def test_a_larger_grant_backfills_the_difference(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 30)
    hub = HistoryHub([_order("recent", 5), _order("older", 60),
                      _order("ancient", 400)])
    client = Client()
    r = FillsReporter(state, hub, client)
    await r.sweep()
    assert _shipped(client) == ["recent"]
    _grant(state, 90)
    await r.sweep()
    assert _shipped(client) == ["recent", "older"]
    assert HistoryMarker(tmp_path).days(ACCOUNT_KEY) == 90


async def test_a_smaller_grant_reads_nothing_again(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 90)
    hub = HistoryHub([_order("a", 5)])
    r = FillsReporter(state, hub, Client())
    await r.sweep()
    calls = len(hub.history_calls())
    _grant(state, 30)
    await r.sweep()
    assert len(hub.history_calls()) == calls


async def test_a_map_with_no_since_never_backfills(tmp_path):
    # The alien broker declares no `since`: a grant changes nothing.
    state = _state(tmp_path, [ALIEN_CONNECTOR])
    _grant(state, 365, connector_id="alien-broker")
    hub = FlatHub({"accounts_list": {"accounts": [{"acct": "AL-1"}]},
                   "orders": {"working": [{"ref": "x-9", "tkr": "spy",
                                           "stage": "FILLED"}]}})
    client = Client()
    r = FillsReporter(state, hub, client)
    await r.sweep()
    await r.sweep()
    assert client.batches == []
    assert [tool for _c, tool, _a in hub.calls] == [
        "accounts_list", "orders", "accounts_list", "orders"]
    assert Anchor(tmp_path).known("alien-broker\x1fAL-1") == {key(
        {"order_id": "x-9", "status": "FILLED"})}
    assert HistoryMarker(tmp_path).days("alien-broker\x1fAL-1") == 0


async def test_a_broker_ignoring_the_filter_still_ships_nothing_provably_older(
        tmp_path):
    # Defense behind the broker's own filter: a fill before the window opened
    # was created before it too.
    state = _rh_only(tmp_path)
    _grant(state, 30)

    class Ignores(HistoryHub):
        async def call(self, connector_id, tool, args, **kw):
            self.calls.append((connector_id, tool, dict(args)))
            if tool == "get_accounts":
                out = {"accounts": [{"account_number": "463605220"}]}
            else:
                out = {"orders": self.orders}
            return {"data": {"data": out, "guide": "ignore me"}}

    client = Client()
    await FillsReporter(state, Ignores([_order("in", 2), _order("old", 90)]),
                        client).sweep()
    assert _shipped(client) == ["in"]


async def test_the_backfill_reads_at_most_its_page_cap(tmp_path, monkeypatch):
    from nakagai_edge.edge import fills
    monkeypatch.setattr(fills, "HISTORY_PAGES", 3)
    state = _rh_only(tmp_path)
    _grant(state, 365)
    hub = HistoryHub([_order(f"o{i}", i) for i in range(20)], page_size=2)
    client = Client()
    await FillsReporter(state, hub, client).sweep()
    assert len(hub.history_calls()) == 3
    assert len(_shipped(client)) == 6


async def test_a_large_backfill_ships_in_batches_in_one_sweep(tmp_path, monkeypatch):
    from nakagai_edge.edge import fills
    monkeypatch.setattr(fills, "SHIP_LIMIT", 2)
    state = _rh_only(tmp_path)
    _grant(state, 30)
    client = Client()
    await FillsReporter(state, HistoryHub([_order(f"o{i}", i) for i in range(5)]),
                        client).sweep()
    assert [len(b) for b in client.batches] == [2, 2, 1]


async def test_filled_quantity_is_read_and_shipped(tmp_path):
    state = _rh_only(tmp_path)
    _grant(state, 30)
    client = Client()
    await FillsReporter(state, HistoryHub(
        [_order("p", 1, quantity="10", cumulative_quantity="4")]), client).sweep()
    row = client.batches[0][0]
    assert row["quantity"] == 10.0
    assert row["filled_quantity"] == 4.0


def test_the_grant_is_read_off_the_stored_bundle(tmp_path):
    from nakagai_edge.edge.sync import trade_history_days
    state = EdgeState(tmp_path)
    assert trade_history_days(state, "robinhood-trading") == 0   # no bundle
    _grant(state, 90)
    assert trade_history_days(state, "robinhood-trading") == 90
    assert trade_history_days(state, "other") == 0
    state.bundle_path.write_text(json.dumps(
        {"trade_history": {"robinhood-trading": {"days": True}}}))
    assert trade_history_days(state, "robinhood-trading") == 0
    state.bundle_path.write_text(json.dumps({"trade_history": ["junk"]}))
    assert trade_history_days(state, "robinhood-trading") == 0
