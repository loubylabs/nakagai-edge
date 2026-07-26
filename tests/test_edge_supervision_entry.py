"""An executed entry becomes a supervised position. A platform that sends no
warrant must still leave a truthful record: unguarded, never invisible."""

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.config import ConnectorSpec, GuardrailsConfig, OrderShape
from nakagai_edge.edge.executor import supervise
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import load

SHAPE = OrderShape(
    symbol_keys=["symbol"], side_keys=["side"], quantity_keys=["quantity"],
    price_keys=["limit_price"], stop_keys=["stop_price"],
    stock_tools=["place_equity_order"],
    market_order_args={"order_type": "market"})

ENTRY_ARGS = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
              "quantity": 100, "limit_price": 47.55, "stop_price": 46.20}


class FakeHub:
    def __init__(self, shape=SHAPE):
        self._spec = ConnectorSpec(
            id="demo", kind="mcp-http", role="broker",
            guardrails=GuardrailsConfig(order_shape=shape))

    def spec(self, connector_id):
        return self._spec


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
    supervise(FakeHub(), state, _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42"}})
    rec = load(state)["ap_1"]
    assert rec["symbol"] == "AAPL"
    assert rec["direction"] == "long"
    assert rec["entry_qty"] == 100.0
    assert rec["stop"] == 46.20
    assert rec["signal_id"] == "sig_1"
    assert rec["state"] == "armed"


def test_a_platform_that_sent_no_warrant_leaves_an_unguarded_record(tmp_path):
    # Version skew is not a policy decision. The entry was approved and it
    # executed; the owner must be able to SEE that it is unprotected.
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, _intent(), _record(), {})
    rec = load(state)["ap_1"]
    assert rec["state"] == "unguarded"
    assert rec["warrant"] is None


def test_an_entry_with_no_stop_is_not_supervised_at_all(tmp_path):
    state = EdgeState(tmp_path)
    intent = _intent()
    del intent["args"]["stop_price"]
    supervise(FakeHub(), state, intent, _record({"grant_id": "wr_1"}), {})
    assert load(state) == {}


def test_a_connector_with_no_declared_shape_is_not_supervised(tmp_path):
    state = EdgeState(tmp_path)
    supervise(FakeHub(OrderShape()), state, _intent(),
              _record({"grant_id": "wr_1"}), {})
    assert load(state) == {}


def test_a_reported_fill_price_beats_the_order_price(tmp_path):
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, _intent(), _record({"grant_id": "wr_1"}),
              {"data": {"order_id": "42", "average_price": "47.61"}})
    assert load(state)["ap_1"]["entry_price"] == 47.61


def test_supervision_never_raises_into_the_executor(tmp_path):
    # A bookkeeping failure must never relabel a really-executed trade.
    state = EdgeState(tmp_path)
    supervise(FakeHub(), state, {"connector_id": "demo", "tool": "t"},
              _record(), None)
    assert load(state) == {}
