"""Reading an entry order and deriving its market exit from the connector's
own `order_shape` declaration. Nothing is guessed: a connector that has not
declared how it expresses a market order cannot be supervised."""

from nakagai_edge.config import OrderShape
from nakagai_edge.warrant import closing_side, exit_order_args, read_entry

SHAPE = OrderShape(
    symbol_keys=["symbol"], side_keys=["side"], quantity_keys=["quantity"],
    price_keys=["limit_price"], stop_keys=["stop_price"],
    stock_tools=["place_equity_order"],
    market_order_args={"order_type": "market", "time_in_force": "day"})

ENTRY = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
         "quantity": 100, "limit_price": 47.55, "stop_price": 46.20}


def test_an_entry_is_read_into_the_five_fields_the_ledger_needs():
    assert read_entry(SHAPE, ENTRY) == {
        "symbol": "AAPL", "side": "buy", "qty": 100.0,
        "price": 47.55, "stop": 46.20}


def test_an_entry_without_a_stop_is_not_supervisable():
    args = {k: v for k, v in ENTRY.items() if k != "stop_price"}
    assert read_entry(SHAPE, args) is None


def test_a_nested_entry_is_refused_rather_than_guessed():
    # Constructing an order INTO a nested payload places a real trade on a
    # guess. v1 declines and the position records as unguarded.
    assert read_entry(SHAPE, {"order": dict(ENTRY)}) is None


def test_an_undeclared_shape_reads_nothing():
    assert read_entry(OrderShape(), ENTRY) is None


def test_a_buy_closes_with_the_first_declared_sell_value():
    assert closing_side(SHAPE, "buy") == "sell"


def test_a_sell_closes_with_the_first_declared_buy_value():
    assert closing_side(SHAPE, "sell_short") == "buy"


def test_an_unrecognised_side_has_no_closing_side():
    assert closing_side(SHAPE, "hodl") == ""


def test_the_exit_reuses_the_entry_payload_minus_its_prices():
    # Derived from the args the broker already accepted, so account_number and
    # any broker-required extra come along for free.
    assert exit_order_args(SHAPE, ENTRY, 40.0) == {
        "account_number": "463605220", "symbol": "AAPL", "side": "sell",
        "quantity": 40.0, "order_type": "market", "time_in_force": "day"}


def test_the_exit_is_none_without_declared_market_arguments():
    shape = SHAPE.model_copy(update={"market_order_args": {}})
    assert exit_order_args(shape, ENTRY, 40.0) is None


def test_the_exit_is_none_for_a_nested_entry():
    assert exit_order_args(SHAPE, {"order": dict(ENTRY)}, 40.0) is None
