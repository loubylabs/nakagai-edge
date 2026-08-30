"""Reading an entry order and deriving its market exit from the connector's
own `place_order` capability. Nothing is guessed: a connector that has not
declared how it expresses a market order cannot be supervised."""

from nakagai_edge.capability import Capability
from nakagai_edge.warrant import closing_side, exit_order_args, read_entry

CAP = Capability(
    tool="place_equity_order",
    args={"symbol": "symbol", "side": "side", "order_type": "order_type",
          "quantity": "quantity", "limit_price": "limit_price",
          "stop_price": "stop_price", "time_in_force": "time_in_force",
          "account": "account_number"},
    outbound_types={field: "string" for field in (
        "symbol", "side", "order_type", "quantity", "limit_price",
        "stop_price", "time_in_force", "account")},
    values={"side": {"buy": ["buy", "buy_to_open", "buy_to_cover"],
                     "sell": ["sell", "sell_to_open", "sell_short"]},
            "order_type": {"limit": ["limit"], "market": ["market"]}})

ENTRY = {"account_number": "463605220", "symbol": "AAPL", "side": "buy",
         "quantity": 100, "limit_price": 47.55, "stop_price": 46.20,
         "order_type": "limit", "time_in_force": "day"}


def test_an_entry_is_read_without_requiring_its_order_type():
    assert read_entry(CAP, ENTRY) == {
        "symbol": "AAPL", "side": "buy", "qty": 100.0,
        "price": 47.55, "stop": 46.20}


def test_an_entry_without_a_stop_is_not_supervisable():
    args = {k: v for k, v in ENTRY.items() if k != "stop_price"}
    assert read_entry(CAP, args) is None


def test_a_nested_entry_is_refused_rather_than_guessed():
    # Constructing an order INTO a nested payload places a real trade on a
    # guess. v1 declines and the position records as unguarded.
    assert read_entry(CAP, {"order": dict(ENTRY)}) is None


def test_an_undeclared_map_reads_nothing():
    # A capability that names a tool but no argument keys is where the order
    # fields live nowhere at all. Reading it would mean guessing key names.
    assert read_entry(Capability(tool="place_equity_order"), ENTRY) is None


def test_a_partially_declared_map_reads_nothing():
    # Four of the five is not four fifths of a supervised position: without a
    # declared stop key there is no level, and with no level nothing to watch.
    cap = CAP.model_copy(update={"args": {
        k: v for k, v in CAP.args.items() if k != "stop_price"}})
    assert read_entry(cap, ENTRY) is None


def test_a_buy_closes_with_the_first_declared_sell_value():
    assert closing_side(CAP, "buy") == "sell"


def test_a_sell_closes_with_the_first_declared_buy_value():
    # "sell_short" is a later alias, not the first: a side is classified by
    # the whole declared list, and closed with the head of the opposite one.
    assert closing_side(CAP, "sell_short") == "buy"


def test_an_unrecognised_side_has_no_closing_side():
    assert closing_side(CAP, "hodl") == ""


def test_the_exit_reuses_the_entry_payload_minus_its_prices():
    # Derived from the args the broker already accepted, so account_number and
    # any broker-required extra come along for free.
    assert exit_order_args(CAP, ENTRY, 40.0) == {
        "account_number": "463605220", "symbol": "AAPL", "side": "sell",
        "quantity": "40", "order_type": "market", "time_in_force": "day"}


def test_the_exit_is_none_without_a_declared_market_order_type():
    cap = CAP.model_copy(update={"values": {"side": CAP.values["side"],
                                             "order_type": {"limit": ["limit"]}}})
    assert exit_order_args(cap, ENTRY, 40.0) is None


def test_the_exit_is_none_for_a_nested_entry():
    assert exit_order_args(CAP, {"order": dict(ENTRY)}, 40.0) is None


def test_the_exit_replaces_the_entry_order_type_with_market():
    result = exit_order_args(CAP, ENTRY, 40.0)
    assert result is not None
    assert result["order_type"] == "market"
