"""Closed trades: the broker's own realized profit or loss, one row per
closing trade, sent only from the moment the edge connected unless the owner
grants a history window."""

import asyncio
import contextlib
import json
import logging
import time
from datetime import datetime, timezone

import pytest

from nakagai_edge.edge.closed_trades import (ClosedTradesReporter, SendFrom,
                                             choose_span, trade_key)
from nakagai_edge.edge.state import EdgeState
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio

DAY = 86400
KEY = "robinhood-trading\x1f463605220"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _iso(at: float) -> str:
    return datetime.fromtimestamp(at, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _trade(symbol, at, pnl="12.50", **extra):
    return {"symbol": symbol, "timestamp": _iso(at), "side": "sell",
            "quantity": "10", "price": "101.25", "realized_gain": pnl, **extra}


SPAN_DAYS = {"week": 7, "month": 30, "3month": 90, "all": None}


class Broker:
    """Robinhood's closed-trades tool in its own envelope: newest first,
    `page_size` a page, a cursor token, and a span it honors unless told not
    to."""

    def __init__(self, trades, page_size=2, honors_span=True, fail=False):
        self.account_key = "ag1"
        self.trades, self.page_size = trades, page_size
        self.honors_span, self.fail = honors_span, fail
        self.calls = []

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((tool, dict(args)))
        if tool == "get_accounts":
            out = {"accounts": [{"account_number": "463605220"}]}
        else:
            if self.fail:
                raise RuntimeError("broker down")
            rows = sorted(self.trades, key=lambda t: t["timestamp"] or "",
                          reverse=True)
            reach = SPAN_DAYS[args["span"]]
            if self.honors_span and reach is not None:
                floor = _iso(time.time() - reach * DAY)
                rows = [r for r in rows if (r["timestamp"] or "") >= floor]
            start = int(args.get("cursor") or 0)
            page = rows[start:start + self.page_size]
            more = start + self.page_size < len(rows)
            out = {"trades": page,
                   "next_cursor": str(start + self.page_size) if more else None}
        return {"data": {"data": out, "guide": "ignore me"}}

    def reads(self):
        return [args for tool, args in self.calls
                if tool == "get_pnl_trade_history"]


class Client:
    def __init__(self, fail=False):
        self.batches, self.fail = [], fail

    def report_closed_trades(self, trades):
        if self.fail:
            raise RuntimeError("platform down")
        self.batches.append(trades)
        return {"ok": True}


def _state(tmp_path, connectors=(ROBINHOOD_CONNECTOR,)):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(
        json.dumps({"connectors": list(connectors)}))
    return EdgeState(tmp_path)


def _grant(state, days, connector_id="robinhood-trading"):
    state.bundle_path.parent.mkdir(parents=True, exist_ok=True)
    state.bundle_path.write_text(json.dumps(
        {"schema_version": 2,
         "trade_history": {connector_id: {"days": days}} if days else {}}))


def _shipped(client):
    return [row["symbol"] for batch in client.batches for row in batch]


# ---- first sight and the send-from instant -------------------------------

async def test_first_sight_sends_nothing_and_records_send_from(tmp_path):
    now = time.time()
    broker = Broker([_trade("OLD", now - 2 * DAY), _trade("OLDER", now - 3 * DAY)])
    client = Client()
    await ClosedTradesReporter(_state(tmp_path), broker, client).sweep()
    assert client.batches == []
    entry = SendFrom(tmp_path).get(KEY)
    assert entry["days"] == 0
    assert now - 1 <= entry["from"] <= time.time()
    assert len(broker.reads()) == 1
    assert broker.reads()[0]["span"] == "week"


async def test_a_trade_after_send_from_ships_once_and_survives_a_restart(tmp_path):
    state = _state(tmp_path)
    broker = Broker([_trade("OLD", time.time() - DAY)])
    client = Client()
    r = ClosedTradesReporter(state, broker, client)
    await r.sweep()
    broker.trades.append(_trade("NEW", time.time() + 5))
    await r.sweep()
    await r.sweep()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert _shipped(client) == ["NEW"]
    row = client.batches[0][0]
    assert row == {"connector_id": "robinhood-trading", "account": "463605220",
                   "closed_at": row["closed_at"], "symbol": "NEW",
                   "side": "sell", "quantity": 10.0, "price": 101.25,
                   "realized_pnl": 12.5, "trade_key": trade_key(row)}


async def test_a_failed_read_creates_and_moves_nothing(tmp_path):
    state = _state(tmp_path)
    broker = Broker([_trade("X", time.time() + 5)], fail=True)
    client = Client()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert SendFrom(tmp_path).get(KEY) is None
    assert not (tmp_path / "cache" / "closed-trades-from.json").exists()
    _grant(state, 30)
    await ClosedTradesReporter(state, broker, client).sweep()
    assert SendFrom(tmp_path).get(KEY) is None
    assert client.batches == []


async def test_an_unreadable_page_moves_nothing(tmp_path):
    class NotAList(Broker):
        async def call(self, connector_id, tool, args, **kw):
            out = await super().call(connector_id, tool, args, **kw)
            if tool == "get_pnl_trade_history":
                out["data"]["data"]["trades"] = "nope"
            return out

    await ClosedTradesReporter(_state(tmp_path), NotAList([]), Client()).sweep()
    assert SendFrom(tmp_path).get(KEY) is None


# ---- the history window --------------------------------------------------

async def test_a_grant_moves_send_from_back_once_and_ships_only_inside_it(tmp_path):
    state = _state(tmp_path)
    now = time.time()
    broker = Broker([_trade("D2", now - 2 * DAY), _trade("D20", now - 20 * DAY),
                     _trade("D40", now - 40 * DAY)])
    client = Client()
    r = ClosedTradesReporter(state, broker, client)
    await r.sweep()                                   # first sight
    _grant(state, 30)
    await r.sweep()
    assert sorted(_shipped(client)) == ["D2", "D20"]
    assert broker.reads()[-1]["span"] == "month"
    entry = SendFrom(tmp_path).get(KEY)
    assert entry["days"] == 30
    assert abs(entry["from"] - (now - 30 * DAY)) < 60
    await r.sweep()
    assert sorted(_shipped(client)) == ["D2", "D20"]
    assert SendFrom(tmp_path).get(KEY) == entry


async def test_a_larger_grant_moves_again_and_a_smaller_one_does_not(tmp_path):
    state = _state(tmp_path)
    now = time.time()
    broker = Broker([_trade("D5", now - 5 * DAY), _trade("D60", now - 60 * DAY),
                     _trade("D200", now - 200 * DAY)])
    client = Client()
    r = ClosedTradesReporter(state, broker, client)
    _grant(state, 30)
    await r.sweep()
    assert _shipped(client) == ["D5"]
    _grant(state, 90)
    await r.sweep()
    assert _shipped(client) == ["D5", "D60"]
    assert broker.reads()[-1]["span"] == "3month"
    moved = SendFrom(tmp_path).get(KEY)
    assert moved["days"] == 90
    _grant(state, 30)
    await r.sweep()
    assert SendFrom(tmp_path).get(KEY) == moved
    assert _shipped(client) == ["D5", "D60"]


async def test_a_grant_beyond_a_quarter_asks_for_all(tmp_path):
    state = _state(tmp_path)
    _grant(state, 365)
    broker = Broker([_trade("D200", time.time() - 200 * DAY)])
    client = Client()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert broker.reads()[0]["span"] == "all"
    assert _shipped(client) == ["D200"]


async def test_a_broker_ignoring_the_span_ships_nothing_older(tmp_path):
    state = _state(tmp_path)
    _grant(state, 7)
    now = time.time()
    broker = Broker([_trade("IN", now - DAY), _trade("OUT", now - 8 * DAY),
                     _trade("WAY-OUT", now - 300 * DAY)], honors_span=False)
    client = Client()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert _shipped(client) == ["IN"]


async def test_unreadable_naive_or_missing_closed_at_is_dropped(tmp_path):
    state = _state(tmp_path)
    _grant(state, 30)
    now = time.time()
    broker = Broker([_trade("GOOD", now - DAY),
                     {**_trade("NAIVE", now - DAY), "timestamp": "2026-11-25T10:00:00"},
                     {**_trade("JUNK", now - DAY), "timestamp": "yesterday"},
                     {**_trade("NONE", now - DAY), "timestamp": None}],
                    page_size=10, honors_span=False)
    client = Client()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert _shipped(client) == ["GOOD"]


async def test_a_negative_realized_pnl_survives(tmp_path):
    state = _state(tmp_path)
    _grant(state, 30)
    client = Client()
    await ClosedTradesReporter(state, Broker([_trade("LOSS", time.time() - DAY,
                                                     pnl="-42.17")]),
                               client).sweep()
    assert client.batches[0][0]["realized_pnl"] == -42.17


# ---- paging --------------------------------------------------------------

async def test_a_regular_sweep_with_nothing_new_makes_one_call(tmp_path):
    state = _state(tmp_path)
    _grant(state, 30)
    now = time.time()
    broker = Broker([_trade(f"S{i}", now - i * DAY) for i in range(1, 9)],
                    page_size=2)
    client = Client()
    r = ClosedTradesReporter(state, broker, client)
    await r.sweep()
    assert len(_shipped(client)) == 8                 # the grant pages it all
    before = len(broker.reads())
    await r.sweep()
    assert len(broker.reads()) == before + 1
    assert broker.reads()[-1].get("cursor") is None


async def test_a_regular_sweep_reads_on_until_it_reaches_known_rows(tmp_path):
    state = _state(tmp_path)
    broker = Broker([], page_size=2)
    client = Client()
    r = ClosedTradesReporter(state, broker, client)
    await r.sweep()                                   # first sight
    now = time.time()
    broker.trades.extend(_trade(f"N{i}", now + 10 + i) for i in range(5))
    await r.sweep()
    assert sorted(_shipped(client)) == [f"N{i}" for i in range(5)]


async def test_truncation_warns_only_with_a_token_remaining(tmp_path, monkeypatch,
                                                          caplog):
    from nakagai_edge.edge import closed_trades
    monkeypatch.setattr(closed_trades, "PAGE_LIMIT", 3)
    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    state = _state(tmp_path)
    _grant(state, 30)
    now = time.time()
    broker = Broker([_trade(f"S{i}", now - i * 3600) for i in range(1, 9)],
                    page_size=2)
    client = Client()
    await ClosedTradesReporter(state, broker, client).sweep()
    assert len(broker.reads()) == 3
    assert len(_shipped(client)) == 6
    assert "older closed trades are not sent" in caplog.text
    assert SendFrom(tmp_path).get(KEY)["days"] == 30


async def test_exactly_the_page_cap_and_done_does_not_warn(tmp_path, monkeypatch,
                                                          caplog):
    from nakagai_edge.edge import closed_trades
    monkeypatch.setattr(closed_trades, "PAGE_LIMIT", 3)
    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    state = _state(tmp_path)
    _grant(state, 30)
    now = time.time()
    broker = Broker([_trade(f"S{i}", now - i * 3600) for i in range(1, 7)],
                    page_size=2)
    await ClosedTradesReporter(state, broker, Client()).sweep()
    assert len(broker.reads()) == 3
    assert "older closed trades" not in caplog.text


# ---- spans ---------------------------------------------------------------

def test_the_narrowest_covering_span_is_chosen():
    from nakagai_edge.config import load_specs
    cap = load_specs({"connectors": [ROBINHOOD_CONNECTOR]})[
        "robinhood-trading"].capability("list_closed_trades")
    now = 1_000_000_000.0
    assert choose_span(cap, now, now) == "week"
    assert choose_span(cap, now - 7 * DAY, now) == "week"
    assert choose_span(cap, now - 8 * DAY, now) == "month"
    assert choose_span(cap, now - 90 * DAY, now) == "quarter"
    assert choose_span(cap, now - 91 * DAY, now) == "all"


def test_an_untranslated_span_widens_to_one_the_map_knows():
    from nakagai_edge.config import load_specs
    cap = load_specs({"connectors": [ALIEN_CONNECTOR]})[
        "alien-broker"].capability("list_closed_trades")
    assert choose_span(cap, 0.0, 1.0) == "all"
    narrow = cap.model_copy(update={"values": {**cap.values,
                                               "span": {"week": ["W"]}}})
    assert choose_span(narrow, 0.0, 100 * DAY) is None


async def test_the_alien_broker_is_asked_in_its_own_words(tmp_path):
    class Alien:
        account_key = "ag1"

        def __init__(self):
            self.calls = []

        async def call(self, connector_id, tool, args, **kw):
            self.calls.append((tool, dict(args)))
            if tool == "accounts_list":
                return {"data": {"accounts": [{"acct": "AL-1"}]}}
            return {"data": {"closes": [
                {"tkr": "spy", "at": _iso(time.time() + 5), "action": "BUY_TO_COVER",
                 "qty": "3", "px": "500", "pl": "-7"}]}}

    hub = Alien()
    state = _state(tmp_path, (ALIEN_CONNECTOR,))
    client = Client()
    r = ClosedTradesReporter(state, hub, client)
    await r.sweep()
    assert hub.calls[1] == ("realized", {"acct": "AL-1", "range": "EVERYTHING"})
    row = client.batches[0][0]
    assert (row["symbol"], row["side"], row["realized_pnl"]) == ("SPY", "buy", -7.0)


async def test_a_connector_without_closed_trades_is_skipped(tmp_path):
    no_closed = {**ROBINHOOD_CONNECTOR,
                 "capabilities": {k: v for k, v
                                  in ROBINHOOD_CONNECTOR["capabilities"].items()
                                  if k != "list_closed_trades"}}
    broker = Broker([])
    await ClosedTradesReporter(_state(tmp_path, (no_closed,)), broker,
                               Client()).sweep()
    assert broker.calls == []


# ---- shipping and identity -----------------------------------------------

async def test_a_platform_failure_leaves_rows_for_the_next_cycle(tmp_path):
    state = _state(tmp_path)
    _grant(state, 30)
    broker = Broker([_trade("A", time.time() - DAY)])
    r = ClosedTradesReporter(state, broker, Client(fail=True))
    await r.sweep()
    good = Client()
    r._client = good
    await r.sweep()
    assert _shipped(good) == ["A"]


async def test_large_batches_drain_in_one_sweep(tmp_path, monkeypatch):
    from nakagai_edge.edge import closed_trades
    monkeypatch.setattr(closed_trades, "SHIP_LIMIT", 2)
    state = _state(tmp_path)
    _grant(state, 30)
    now = time.time()
    client = Client()
    await ClosedTradesReporter(state, Broker(
        [_trade(f"S{i}", now - i * 3600) for i in range(1, 6)]), client).sweep()
    assert [len(b) for b in client.batches] == [2, 2, 1]


def test_trade_key_is_stable_and_covers_every_value():
    row = {"connector_id": "c", "account": "1", "closed_at": "2026-11-25T15:00:00Z",
           "symbol": "NVDA", "side": "sell", "quantity": 10.0, "price": 101.25,
           "realized_pnl": -3.5}
    assert trade_key(row) == trade_key(dict(reversed(list(row.items()))))
    # Pinned: the platform dedupes on this, so it must not drift between runs
    # or releases.
    assert trade_key(row) == (
        "34aae4ad8ef05f2dffd03d0716841cc6b584e24f3d4a57315016b650d1a6d3d3")
    import hashlib
    expected = hashlib.sha256(json.dumps(
        {k: row[k] for k in sorted(row)}, separators=(",", ":")).encode()
    ).hexdigest()
    assert trade_key(row) == expected
    assert trade_key({**row, "realized_pnl": -3.51}) != trade_key(row)
    assert trade_key({**row, "account": "2"}) != trade_key(row)


# ---- the runtime loop ----------------------------------------------------

class _Sweeper:
    def __init__(self, fail=False):
        self.runs, self.fail = 0, fail

    async def sweep(self):
        self.runs += 1
        if self.fail:
            raise RuntimeError("boom")
        return []


@pytest.mark.parametrize("fills_fail,closed_fail",
                         [(False, False), (True, False), (False, True)])
async def test_the_fills_loop_runs_both_and_survives_either_failing(
        tmp_path, monkeypatch, fills_fail, closed_fail):
    import nakagai_edge.edge.runtime as runtime
    from nakagai_edge.edge.audit import EdgeAudit
    from nakagai_edge.edge.brake import Brake
    monkeypatch.setattr(runtime, "FILLS_INTERVAL_S", 0.01)
    monkeypatch.setattr(runtime, "sync_once", lambda *a, **k: False)

    class _Reporter:
        async def snapshot_and_push(self):
            return {"connectors": []}

    state = _state(tmp_path, ())
    fills, closed = _Sweeper(fills_fail), _Sweeper(closed_fail)
    audit = EdgeAudit(state)
    tasks = await runtime._loops(state, object(), object(), audit, _Reporter(),
                                 Brake(state, object(), object(), audit),
                                 fills, closed)
    for _ in range(200):
        if fills.runs >= 2 and closed.runs >= 2:
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    assert fills.runs >= 2 and closed.runs >= 2
