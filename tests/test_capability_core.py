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


def test_coerce_refuses_a_boolean_rather_than_counting_it_as_one():
    # `true` is not a quantity, a price or a stop. Python makes bool a subclass
    # of int, so without an explicit check float(True) is 1.0: a position sized
    # one share, or a stop at $1.00, invented out of a field that said nothing
    # numeric. Verbatim fields refuse it for the same reason.
    cap = Capability(tool="t")
    for field in ("quantity", "price", "stop", "symbol", "equity"):
        assert coerce(field, True, cap) is None
        assert coerce(field, False, cap) is None


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


# ---- sizing an order in money instead of shares ---------------------------
#
# Robinhood sizes a dollar-based order in money and leaves the share count null
# until it fills. Before `notional` existed the journal recorded that such a
# trade happened and not how large it was, silently: `quantity` is optional, so
# the row was never dropped and nothing logged.

def _orders_cap(**fields):
    """A `list_orders` map over the field paths a caller cares about."""
    base = {"order_id": ["id"], "symbol": ["symbol"]}
    return Capability(tool="get_equity_orders", items=["data.orders"],
                      fields={**base, **fields})


def test_a_dollar_based_order_reads_its_size_into_notional():
    from nakagai_edge.capability import read_row
    cap = _orders_cap(quantity=["quantity"], notional=["dollar_based_amount"])
    row = read_row("list_orders", cap,
                   {"id": "ord-1", "symbol": "nvda", "quantity": None,
                    "dollar_based_amount": "250.00"})
    assert row == {"order_id": "ord-1", "symbol": "NVDA", "notional": 250.0}
    # Absent, not zero. A reader must be able to tell "sized in dollars" from
    # "zero shares", which is the distinction `coerce` returning None protects.
    assert "quantity" not in row


def test_a_share_based_order_reads_quantity_and_no_notional():
    from nakagai_edge.capability import read_row
    cap = _orders_cap(quantity=["quantity"], notional=["dollar_based_amount"])
    row = read_row("list_orders", cap,
                   {"id": "ord-2", "symbol": "nvda", "quantity": "3"})
    assert row == {"order_id": "ord-2", "symbol": "NVDA", "quantity": 3.0}
    assert "notional" not in row


def test_notional_is_a_separate_word_and_never_becomes_quantity():
    """The units guard, and the reason this is two fields rather than a
    fallback path on one.

    `first()` takes the earliest non-None path, so declaring
    `quantity: [quantity, dollar_based_amount]` WOULD fill the field, with
    dollars, in a share count, and nothing downstream could tell. The module
    docstring states the invariant this protects: a wrong map "can never make
    `quantity` mean notional". Keeping the words separate is what makes that
    true rather than aspirational.
    """
    assert "notional" in CAPABILITIES["list_orders"].optional
    assert "quantity" in CAPABILITIES["list_orders"].optional
    # Neither is required, so an order sized either way still journals.
    assert CAPABILITIES["list_orders"].required == ("order_id", "symbol")


def test_notional_is_numeric_not_a_relayed_display_string():
    """FLOAT beside `quantity` and `fill_price`, not VERBATIM beside `equity`.

    A size is arithmetic; a balance is a figure the broker worded. Getting this
    backwards would hand the platform a string where it stores a float.
    """
    cap = _orders_cap(notional=["dollar_based_amount"])
    assert coerce("notional", "250.00", cap) == 250.0
    assert coerce("notional", "not a number", cap) is None
    assert coerce("notional", True, cap) is None
