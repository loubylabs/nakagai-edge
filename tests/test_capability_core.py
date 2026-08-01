from nakagai_edge.capability import (
    CAPABILITIES,
    Capability,
    coerce,
    first,
    walk,
)


def test_walk_follows_dotted_segments():
    assert walk({"data": {"total_value": "104.55"}}, "data.total_value") == "104.55"


def test_walk_returns_none_through_a_non_dict():
    assert walk({"data": ["not", "a", "dict"]}, "data.total_value") is None
    assert walk({"data": "scalar"}, "data.total_value") is None


def test_walk_returns_none_for_a_missing_segment():
    assert walk({"data": {}}, "data.total_value") is None


def test_first_takes_the_earliest_path_that_yields_a_value():
    payload = {"data": {"equity": "1.00", "total_value": "2.00"}}
    assert first(payload, ["data.total_value", "data.equity"]) == "2.00"
    assert first(payload, ["data.missing", "data.equity"]) == "1.00"
    assert first(payload, ["data.missing"]) is None


def test_coerce_upper_str_uppercases_a_symbol():
    cap = Capability(tool="t")
    assert coerce("symbol", "aapl", cap) == "AAPL"


def test_coerce_float_parses_a_string_quantity():
    cap = Capability(tool="t")
    assert coerce("quantity", "25", cap) == 25.0


def test_coerce_float_rejects_an_unparseable_value():
    cap = Capability(tool="t")
    assert coerce("quantity", "many", cap) is None


def test_coerce_float_refuses_a_non_finite_value():
    cap = Capability(tool="t")
    for bad in ("nan", "inf", "-inf", "Infinity", float("nan"), float("inf")):
        assert coerce("quantity", bad, cap) is None


def test_coerce_side_resolves_through_the_connector_alias_map():
    cap = Capability(tool="t", values={"side": {
        "buy": ["buy", "buy_to_open"], "sell": ["sell", "sell_short"]}})
    assert coerce("side", "BUY_TO_OPEN", cap) == "buy"
    assert coerce("side", "sell_short", cap) == "sell"
    assert coerce("side", "exercise", cap) is None


def test_coerce_verbatim_passes_scalars_through_untouched():
    cap = Capability(tool="t")
    assert coerce("equity", "104,238.55", cap) == "104,238.55"
    assert coerce("equity", 104238.55, cap) == 104238.55


def test_coerce_verbatim_rejects_a_container():
    cap = Capability(tool="t")
    assert coerce("equity", {"buying_power": "1.00"}, cap) is None


def test_vocabulary_covers_the_seven_v1_capabilities():
    assert set(CAPABILITIES) == {
        "list_accounts", "get_balance", "list_positions", "get_quote",
        "list_orders", "place_order", "cancel_order"}


def test_write_capabilities_are_marked_as_writes():
    assert CAPABILITIES["place_order"].is_write is True
    assert CAPABILITIES["cancel_order"].is_write is True
    assert CAPABILITIES["get_balance"].is_write is False
