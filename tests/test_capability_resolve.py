import pytest

import nakagai_edge.capability as capability_module
from nakagai_edge.capability import (
    Capability,
    CapabilityError,
    extract,
    resolve,
)

BALANCE = Capability(
    tool="get_portfolio",
    args={"account": "account_number"},
    fields={"equity": ["data.total_value", "data.equity"],
            "cash": ["data.cash"],
            "buying_power": ["data.buying_power.buying_power"]})

POSITIONS = Capability(
    tool="get_equity_positions",
    args={"account": "account_number"},
    items=["data.positions", "data.results"],
    fields={"symbol": ["symbol"], "quantity": ["quantity"],
            "avg_price": ["average_buy_price"]})

ORDER = Capability(
    tool="place_equity_order",
    args={"symbol": "symbol", "side": "side", "order_type": "type",
          "quantity": "quantity", "limit_price": "limit_price",
          "stop_price": "stop_price", "time_in_force": "time_in_force",
          "account": "account_number"},
    outbound_types={field: "string" for field in (
        "symbol", "side", "order_type", "quantity", "limit_price",
        "stop_price", "time_in_force", "account")},
    values={"side": {"buy": ["buy", "buy_to_open"],
                     "sell": ["sell", "sell_short"]},
            "order_type": {"limit": ["limit"], "market": ["market"]}})


def test_resolve_renames_canonical_args_to_broker_keys():
    tool, args = resolve("get_balance", BALANCE, {"account": "463605220"})
    assert tool == "get_portfolio"
    assert args == {"account_number": "463605220"}


def test_resolve_drops_none_valued_args():
    tool, args = resolve("get_balance", BALANCE, {"account": None})
    assert args == {}


def test_resolve_translates_canonical_side_to_the_brokers_first_alias():
    _, args = resolve("place_order", ORDER, {
        "symbol": "AAPL", "side": "buy", "order_type": "limit",
        "quantity": 5, "limit_price": 187.20, "stop_price": 180.0,
        "time_in_force": "gfd", "account": "463605220"})
    assert args["side"] == "buy"
    assert args["quantity"] == "5"


def test_order_entry_fields_stay_separate_from_outbound_requirements():
    # A broker's accepted entry is read through these five durable fields. Its
    # outbound requirement adds type, time-in-force, and account, without
    # making a historical position unreadable because it has no order type.
    assert capability_module.ENTRY_FIELDS == (
        "symbol", "side", "quantity", "limit_price", "stop_price")
    assert capability_module.OUTBOUND_ORDER_FIELDS == (
        "symbol", "side", "order_type", "quantity", "limit_price",
        "stop_price", "time_in_force", "account")


def test_resolve_translates_the_complete_canonical_limit_order():
    tool, args = resolve("place_order", ORDER, {
        "symbol": "aapl", "side": "buy", "order_type": "limit",
        "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
        "time_in_force": "gfd", "account": "463605220"})
    assert tool == "place_equity_order"
    assert args == {
        "symbol": "aapl", "side": "buy", "type": "limit",
        "quantity": "5", "limit_price": "187.2", "stop_price": "180.0",
        "time_in_force": "gfd", "account_number": "463605220"}


@pytest.mark.parametrize("field, value", [
    ("quantity", 5.5),
    ("quantity", True),
    ("limit_price", float("nan")),
    ("stop_price", float("inf")),
])
def test_resolve_refuses_invalid_equity_order_numbers(field, value):
    args = {"symbol": "AAPL", "side": "buy", "order_type": "limit",
            "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
            "time_in_force": "gfd", "account": "463605220"}
    args[field] = value
    with pytest.raises(CapabilityError, match=field):
        resolve("place_order", ORDER, args)


def test_resolve_refuses_a_missing_required_equity_order_field():
    args = {"symbol": "AAPL", "side": "buy", "order_type": "limit",
            "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
            "time_in_force": "gfd", "account": "463605220"}
    del args["order_type"]
    with pytest.raises(CapabilityError, match="order_type"):
        resolve("place_order", ORDER, args)


def test_resolve_refuses_an_unknown_order_type():
    args = {"symbol": "AAPL", "side": "buy", "order_type": "stop_limit",
            "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
            "time_in_force": "gfd", "account": "463605220"}
    with pytest.raises(CapabilityError, match="order_type"):
        resolve("place_order", ORDER, args)


def test_resolve_refuses_a_lossy_number_conversion():
    numeric = ORDER.model_copy(update={
        "outbound_types": {**ORDER.outbound_types, "quantity": "number"}})
    args = {"symbol": "AAPL", "side": "buy", "order_type": "limit",
            "quantity": "5", "limit_price": 187.2, "stop_price": 180.0,
            "time_in_force": "gfd", "account": "463605220"}
    with pytest.raises(CapabilityError, match="quantity"):
        resolve("place_order", numeric, args)


def test_resolve_preserves_a_declared_numeric_broker_argument():
    numeric = ORDER.model_copy(update={
        "outbound_types": {**ORDER.outbound_types, "quantity": "number"}})
    _, args = resolve("place_order", numeric, {
        "symbol": "AAPL", "side": "buy", "order_type": "limit",
        "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
        "time_in_force": "gfd", "account": "463605220"})
    assert args["quantity"] == 5


def test_resolve_refuses_an_unknown_canonical_order_field():
    args = {"symbol": "AAPL", "side": "buy", "order_type": "limit",
            "quantity": 5, "limit_price": 187.2, "stop_price": 180.0,
            "time_in_force": "gfd", "account": "463605220",
            "trail_price": 1.0}
    with pytest.raises(CapabilityError, match="trail_price"):
        resolve("place_order", ORDER, args)


def test_resolve_refuses_an_arg_the_connector_never_declared():
    cap = Capability(tool="get_portfolio", args={})
    with pytest.raises(CapabilityError, match="account"):
        resolve("get_balance", cap, {"account": "1"})


def test_resolve_refuses_an_unknown_capability():
    with pytest.raises(CapabilityError, match="no_such_capability"):
        resolve("no_such_capability", BALANCE, {})


def test_extract_reads_a_scalar_capability_through_its_paths():
    payload = {"data": {"total_value": "104238.55", "cash": "50.00",
                        "buying_power": {"buying_power": "12038.10"}}}
    assert extract("get_balance", BALANCE, payload) == {
        "equity": "104238.55", "cash": "50.00", "buying_power": "12038.10"}


def test_extract_returns_none_when_a_required_scalar_field_is_missing():
    assert extract("get_balance", BALANCE, {"data": {"cash": "50.00"}}) is None


def test_extract_reads_a_list_capability_and_coerces_decision_fields():
    payload = {"data": {"positions": [
        {"symbol": "aapl", "quantity": "25", "average_buy_price": "187.20"}]}}
    assert extract("list_positions", POSITIONS, payload) == [
        {"symbol": "AAPL", "quantity": 25.0, "avg_price": 187.20}]


def test_extract_falls_back_to_the_second_items_path():
    payload = {"data": {"results": [{"symbol": "MSFT", "quantity": "3"}]}}
    rows = extract("list_positions", POSITIONS, payload)
    assert rows == [{"symbol": "MSFT", "quantity": 3.0}]


def test_extract_drops_a_row_whose_required_field_is_unreadable():
    payload = {"data": {"positions": [
        {"symbol": "AAPL", "quantity": "many"},
        {"symbol": "MSFT", "quantity": "3"}]}}
    rows = extract("list_positions", POSITIONS, payload)
    assert rows == [{"symbol": "MSFT", "quantity": 3.0}]


def test_extract_returns_none_when_no_items_path_matches():
    # NOT []. The map named a node the payload does not have, so this answer
    # was not read; "the broker holds nothing" is a different fact and it has
    # its own value. An agent that cannot tell them apart reads a broken map as
    # a flat account and buys what it already holds.
    assert extract("list_positions", POSITIONS, {"data": {}}) is None


def test_extract_returns_none_when_the_items_root_is_not_a_list():
    assert extract("list_positions", POSITIONS,
                   {"data": {"positions": "none"}}) is None


def test_extract_returns_an_empty_list_only_for_a_genuinely_empty_one():
    # The one shape that means "nothing held": the broker sent a list and it
    # had nothing in it.
    assert extract("list_positions", POSITIONS, {"data": {"positions": []}}) == []


def test_read_row_enforces_required_fields():
    from nakagai_edge.capability import read_row
    assert read_row("list_positions", POSITIONS,
                    {"symbol": "aapl", "quantity": "25"}) == {
        "symbol": "AAPL", "quantity": 25.0}
    assert read_row("list_positions", POSITIONS,
                    {"symbol": "aapl", "quantity": "many"}) is None


def test_read_partial_keeps_what_it_could_read():
    from nakagai_edge.capability import read_partial
    assert read_partial("list_positions", POSITIONS,
                        {"symbol": "aapl", "quantity": "many"}) == {
        "symbol": "AAPL"}
