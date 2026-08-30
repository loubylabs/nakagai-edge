"""An executed entry becomes a supervised position. A platform that sends no
warrant must still leave a truthful record: unguarded, never invisible."""

import logging

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.capability import Capability
from nakagai_edge.config import ConnectorSpec
from nakagai_edge.edge.executor import supervise
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import load

PLACE_ORDER = Capability(
    tool="place_equity_order",
    args={"symbol": "symbol", "side": "side", "order_type": "order_type",
          "quantity": "quantity", "limit_price": "limit_price",
          "stop_price": "stop_price", "time_in_force": "time_in_force",
          "account": "account_number"},
    outbound_types={field: "string" for field in (
        "symbol", "side", "order_type", "quantity", "limit_price",
        "stop_price", "time_in_force", "account")},
    # What the broker says BACK. `fill_price` is what the entry price is read
    # from; it used to be guessed from a list of candidate key names compiled
    # into executor.py, which is the leak the capability layer exists to stop.
    fields={"order_id": ["order_id"], "fill_price": ["average_price"]},
    values={"side": {"buy": ["buy", "buy_to_open", "buy_to_cover"],
                     "sell": ["sell", "sell_to_open", "sell_short"]},
            "order_type": {"limit": ["limit"], "market": ["market"]}})

# No market type declaration, so nothing here can build an exit.
NO_MARKET_CAP = PLACE_ORDER.model_copy(update={
    "values": {"side": PLACE_ORDER.values["side"],
               "order_type": {"limit": ["limit"]}}})

ENTRY_ARGS = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
              "quantity": 100, "limit_price": 47.55, "stop_price": 46.20,
              "order_type": "limit", "time_in_force": "day"}


class FakeHub:
    def __init__(self, cap=PLACE_ORDER):
        # `cap=None` is a connector that declares no place_order at all, which
        # is a different failure from declaring an unusable one.
        self._spec = ConnectorSpec(
            id="demo", kind="mcp-http", role="broker",
            capabilities={"place_order": cap} if cap else {})

    def spec(self, connector_id):
        return self._spec


class RaisingHub:
    """A hub whose spec() blows up mid-lookup, the way a misbehaving registry
    or a stale connector cache actually could. Used to prove supervise()'s
    try/except really guards a raise, not just a None short-circuit."""

    def spec(self, connector_id):
        raise RuntimeError("registry unavailable")


def _intent():
    return {"connector_id": "demo", "tool": "place_equity_order",
            "args": dict(ENTRY_ARGS)}


def _record(warrant=None):
    doc = {"approval_id": "ap_1", "signal_id": "sig_1"}
    if warrant is not None:
        doc["exit_warrant"] = warrant
    return doc


def test_an_executed_entry_becomes_a_supervised_position(tmp_path):
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, "ap_1", _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42"}})
    rec = load(state)["ap_1"]
    assert rec["symbol"] == "AAPL"
    assert rec["direction"] == "long"
    assert rec["entry_qty"] == 100.0
    assert rec["stop"] == 46.20
    assert rec["signal_id"] == "sig_1"
    assert rec["state"] == "armed"
    assert rec["account"] == "463605220"


def test_a_platform_that_sent_no_warrant_leaves_an_unguarded_record(tmp_path):
    # Version skew is not a policy decision. The entry was approved and it
    # executed; the owner must be able to SEE that it is unprotected.
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, "ap_1", _intent(), _record(), {})
    rec = load(state)["ap_1"]
    assert rec["state"] == "unguarded"
    assert rec["warrant"] is None


def test_an_entry_with_no_stop_is_not_supervised_at_all(tmp_path):
    state = EdgeState(tmp_path)
    intent = _intent()
    del intent["args"]["stop_price"]
    supervise(FakeHub(), state, "ap_1", intent, _record({"grant_id": "wr_1"}), {})
    assert load(state) == {}


def test_a_connector_that_declares_no_place_order_is_not_supervised(tmp_path):
    # Nothing declared means nothing to read the order through, and a ledger
    # record keyed on guessed field names would be worse than no record.
    state = EdgeState(tmp_path)
    supervise(FakeHub(None), state, "ap_1", _intent(),
              _record({"grant_id": "wr_1"}), {})
    assert load(state) == {}


def test_a_place_order_map_missing_a_field_is_not_supervised(tmp_path):
    # Declared, but not completely: with no stop key there is no level, so
    # there is nothing for the brake to watch.
    #
    # The spec is built whole and then broken by hand because config.py now
    # refuses this map at parse time, so no registry can produce one. The check
    # inside supervise() stays anyway: a Capability constructed in code never
    # went past that validator, and the alternative is a ledger record keyed on
    # field names nobody declared.
    state = EdgeState(tmp_path)
    hub = FakeHub()
    hub.spec("demo").capabilities["place_order"] = PLACE_ORDER.model_copy(
        update={"args": {k: v for k, v in PLACE_ORDER.args.items()
                         if k != "stop_price"}})
    supervise(hub, state, "ap_1", _intent(), _record({"grant_id": "wr_1"}), {})
    assert load(state) == {}


def test_a_reported_fill_price_beats_the_order_price(tmp_path):
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, "ap_1", _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42", "average_price": "47.61"}})
    assert load(state)["ap_1"]["entry_price"] == 47.61


def test_an_undeclared_fill_price_falls_back_to_the_order_price(tmp_path):
    # The connector names no path to a fill price, and the broker's payload uses
    # a key the edge used to guess at. Nothing may read it: a price the map
    # never pointed at is a price no connector author chose. This is the test
    # that fails if the guess-list in executor.py ever comes back.
    state = EdgeState(tmp_path)
    hub = FakeHub()
    hub.spec("demo").capabilities["place_order"] = PLACE_ORDER.model_copy(
        update={"fields": {}})
    supervise(hub, state, "ap_1", _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42", "average_price": "47.61"}})
    assert load(state)["ap_1"]["entry_price"] == 47.55


def test_a_zero_fill_price_falls_back_to_the_order_price(tmp_path):
    # A broker can plausibly echo "0" for an order accepted but not yet
    # filled. Trusting it would put a zero-priced entry into every R this
    # position ever reports.
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, "ap_1", _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42", "average_price": "0"}})
    assert load(state)["ap_1"]["entry_price"] == 47.55


def test_supervision_never_raises_into_the_executor(tmp_path):
    # A bookkeeping failure must never relabel a really-executed trade. This
    # must make supervise() actually raise inside its body (a hub.spec() that
    # blows up on an otherwise-complete intent), not merely short-circuit on
    # a None from read_entry: removing the try/except must fail this test.
    state = EdgeState(tmp_path)
    supervise(RaisingHub(), state, "ap_1", _intent(),
              _record({"grant_id": "wr_1"}), {"data": {"order_id": "42"}})
    assert load(state) == {}


def test_an_unclassifiable_order_side_records_unguarded(tmp_path):
    # "sell_short" is neither buy nor sell for this connector's declared
    # values, so direction cannot be read. Failing open to "short" would put
    # a wrong sign into reconcile's broker comparison; failing closed leaves
    # the position visible but never acted on.
    state = EdgeState(tmp_path)
    intent = _intent()
    intent["args"]["side"] = "transfer"
    supervise(FakeHub(), state, "ap_1", intent, _record({"grant_id": "wr_1"}), {})
    rec = load(state)["ap_1"]
    assert rec["direction"] == ""
    assert rec["state"] == "unguarded"
    assert rec["warrant"] is None
    assert "unclassifiable order side" in rec["anomaly"]


def test_an_entry_with_no_recognizable_account_records_unguarded(tmp_path):
    # None of GuardrailsConfig's default arg_names ("account_number",
    # "account_id", "account") appear in the order, so account cannot be
    # named. An empty account would never appear in reconcile's answered()
    # set, so the record would never sync to broker truth again; recording it
    # unguarded keeps that visible instead of silently orphaning it.
    state = EdgeState(tmp_path)
    intent = _intent()
    del intent["args"]["account_number"]
    supervise(FakeHub(), state, "ap_1", intent, _record({"grant_id": "wr_1"}), {})
    rec = load(state)["ap_1"]
    assert rec["account"] == ""
    assert rec["state"] == "unguarded"
    assert rec["warrant"] is None
    assert "no account on the order" in rec["anomaly"]


# ---- final round: the disqualifications must compose, and be complete -----


def test_both_an_unclassifiable_side_and_a_missing_account_are_reported(tmp_path):
    # Reporting only the side sends the owner to fix one thing and leaves the
    # other waiting behind it. The anomaly is the only place either problem is
    # ever named, so it has to name both.
    state = EdgeState(tmp_path)
    intent = _intent()
    intent["args"]["side"] = "transfer"
    del intent["args"]["account_number"]
    supervise(FakeHub(), state, "ap_1", intent, _record({"grant_id": "wr_1"}), {})
    rec = load(state)["ap_1"]
    assert rec["state"] == "unguarded"
    assert "unclassifiable order side" in rec["anomaly"]
    assert "no account on the order" in rec["anomaly"]


def test_a_connector_with_no_market_order_type_records_unguarded(tmp_path):
    # Spec section 9: no place_order map OR no market type declaration means this
    # connector's positions cannot be supervised. The platform mints the
    # warrant from the entry order and cannot see an edge-side connector
    # field, so nothing but this check keeps the record out of `armed`, and
    # `armed` here would report guarded: true for a brake that can never
    # build an exit.
    state = EdgeState(tmp_path)
    supervise(FakeHub(NO_MARKET_CAP), state, "ap_1", _intent(),
              _record({"grant_id": "wr_1"}), {})
    rec = load(state)["ap_1"]
    assert rec["state"] == "unguarded"
    assert rec["warrant"] is None
    assert "canonical market exit" in rec["anomaly"]


def test_the_disqualification_is_persisted_on_the_record(tmp_path):
    # `anomaly` is not enough: reconcile pops it on the next clean sweep, and
    # the renewal path re-armed the record sixty seconds after entry. `blocked`
    # is the durable half, written once here and read by every downstream gate.
    state = EdgeState(tmp_path)
    supervise(FakeHub(NO_MARKET_CAP), state, "ap_1", _intent(),
              _record({"grant_id": "wr_1"}), {})
    assert load(state)["ap_1"]["blocked"] == "connector cannot express a canonical market exit"


def test_a_healthy_position_records_an_empty_blocked_field(tmp_path):
    # Present and falsy, not absent: every gate reads it, and a field that
    # only appears on failures invites a `.get()` that quietly means "fine".
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, "ap_1", _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42"}})
    rec = load(state)["ap_1"]
    assert rec["blocked"] == ""
    assert rec["state"] == "armed"


def test_a_swallowed_bookkeeping_failure_says_so_in_the_log(tmp_path, caplog):
    # The swallow stays: it must never relabel a genuinely executed trade. But
    # a silent swallow leaves a LIVE position with no ledger record and no
    # trace of why, which is the one outcome nobody can debug after the fact.
    state = EdgeState(tmp_path)
    with caplog.at_level(logging.WARNING, logger="nakagai.edge"):
        supervise(RaisingHub(), state, "ap_1", _intent(),
                  _record({"grant_id": "wr_1"}), {"data": {"order_id": "42"}})
    assert load(state) == {}
    assert "registry unavailable" in caplog.text
