"""Reading an entry order and deriving its market exit, through two connectors
that agree on nothing but the meaning. The brake builds a real order from this,
so every rule here is about refusing rather than guessing."""

from nakagai_edge.config import load_specs
from nakagai_edge.warrant import closing_side, exit_order_args, read_entry
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})
ALIEN = SPECS["alien-broker"].capability("place_order")
RH = SPECS["robinhood-trading"].capability("place_order")

ALIEN_ENTRY = {"acct": "AL-1", "ticker": "aapl", "action": "BUY_TO_OPEN",
               "qty": 25, "limit": 187.20, "trigger": 180.00,
               "kind": "LIMIT", "tif": "day"}
RH_ENTRY = {"account_number": "463605220", "symbol": "aapl", "side": "buy",
            "quantity": 25, "limit_price": 187.20, "stop_price": 180.00,
            "type": "limit", "time_in_force": "gfd"}


def test_read_entry_reads_both_shapes_identically():
    expected = {"symbol": "AAPL", "side": "buy_to_open", "qty": 25.0,
                "price": 187.20, "stop": 180.00}
    assert read_entry(ALIEN, ALIEN_ENTRY) == {**expected, "side": "buy_to_open"}
    assert read_entry(RH, RH_ENTRY) == {**expected, "side": "buy"}


def test_read_entry_does_not_require_order_type_from_a_position_read():
    entry = {key: value for key, value in RH_ENTRY.items() if key != "type"}
    assert read_entry(RH, entry) == {
        "symbol": "AAPL", "side": "buy", "qty": 25.0,
        "price": 187.20, "stop": 180.00}


def test_read_entry_refuses_an_entry_with_no_stop():
    assert read_entry(RH, {k: v for k, v in RH_ENTRY.items()
                           if k != "stop_price"}) is None


def test_read_entry_refuses_a_null_under_a_declared_key():
    # Declared and present is not the same as filled in. A None here would
    # read as the string "NONE" and build an exit for an instrument no broker
    # has. Later layers do catch that as a symbol mismatch, but this is the
    # first one, and the first one is what keeps a malformed entry from ever
    # becoming an order.
    assert read_entry(RH, {**RH_ENTRY, "symbol": None}) is None


def test_read_entry_refuses_a_container_under_a_declared_key():
    # The nested case, one level down under a key that IS declared. Both of
    # these are on string fields on purpose: a container under quantity or
    # price is caught a second time when float() raises, but str() never
    # raises, so a dict under `symbol` would stringify into a fake instrument
    # and a list under `side` into a side no broker has. This guard is the
    # only thing standing there.
    assert read_entry(RH, {**RH_ENTRY, "symbol": {"ticker": "aapl"}}) is None
    assert read_entry(RH, {**RH_ENTRY, "side": ["buy"]}) is None


def test_closing_side_inverts_through_each_connectors_own_words():
    # The entry is recognized by any alias the connector lists; the exit is
    # sent as the FIRST one on the opposite side, which is why a connector puts
    # the spelling that is correct whether it opens or closes at the front.
    assert closing_side(ALIEN, "buy_to_open") == "SELL"
    assert closing_side(RH, "buy") == "sell"
    assert closing_side(RH, "sell_short") == "buy"


def test_exit_order_args_resolves_a_canonical_market_order():
    args = exit_order_args(ALIEN, ALIEN_ENTRY, 10.0)
    assert args == {"acct": "AL-1", "ticker": "aapl", "action": "SELL",
                    "kind": "MARKET", "qty": "10", "tif": "day"}
    assert "limit" not in args and "trigger" not in args


def test_exit_order_args_keeps_the_raw_symbol_casing_the_broker_accepted():
    assert exit_order_args(RH, RH_ENTRY, 5.0)["symbol"] == "aapl"


def test_exit_order_args_refuses_without_a_market_order_type_value():
    bad = ALIEN.model_copy(update={"values": {"side": ALIEN.values["side"],
                                               "order_type": {"limit": ["LIMIT"]}}})
    assert exit_order_args(bad, ALIEN_ENTRY, 10.0) is None


def test_exit_order_args_refuses_a_connector_with_no_time_in_force():
    bare_entry = {key: value for key, value in RH_ENTRY.items()
                  if key != "time_in_force"}
    assert exit_order_args(RH, bare_entry, 5.0) is None
