import pytest

from nakagai_edge.capability import Capability, CapabilityError, extract, resolve

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
    args={"symbol": "symbol", "side": "side", "quantity": "quantity",
          "price": "limit_price", "stop": "stop_price",
          "account": "account_number"},
    values={"side": {"buy": ["buy", "buy_to_open"],
                     "sell": ["sell", "sell_short"]}},
    market_args={"type": "market"})


def test_resolve_renames_canonical_args_to_broker_keys():
    tool, args = resolve("get_balance", BALANCE, {"account": "463605220"})
    assert tool == "get_portfolio"
    assert args == {"account_number": "463605220"}


def test_resolve_drops_none_valued_args():
    tool, args = resolve("get_balance", BALANCE, {"account": None})
    assert args == {}


def test_resolve_translates_canonical_side_to_the_brokers_first_alias():
    _, args = resolve("place_order", ORDER, {"symbol": "AAPL", "side": "buy",
                                             "quantity": 5})
    assert args["side"] == "buy"
    assert args["quantity"] == 5


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
