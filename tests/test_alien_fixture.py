from nakagai_edge.capability import CAPABILITIES, extract, resolve
from nakagai_edge.config import load_specs
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})


def test_both_connectors_declare_the_same_capabilities():
    alien = set(SPECS["alien-broker"].capability_names)
    rh = set(SPECS["robinhood-trading"].capability_names)
    assert alien == rh
    assert alien == set(CAPABILITIES)
    assert "list_positions" in alien


def test_the_two_connectors_resolve_the_same_call_differently():
    args = {"account": "X"}
    assert resolve("list_positions",
                   SPECS["alien-broker"].capability("list_positions"),
                   args) == ("holdings", {"acct": "X"})
    assert resolve("list_positions",
                   SPECS["robinhood-trading"].capability("list_positions"),
                   args) == ("get_equity_positions", {"account_number": "X"})


def test_the_two_connectors_extract_the_same_positions_from_different_shapes():
    alien_payload = {"holdings": [{"ticker": "aapl", "qty": "25",
                                   "cost": "187.20"}]}
    rh_payload = {"data": {"positions": [{"symbol": "AAPL", "quantity": "25",
                                          "average_buy_price": "187.20"}]}}
    expected = [{"symbol": "AAPL", "quantity": 25.0, "avg_price": 187.20}]
    assert extract("list_positions",
                   SPECS["alien-broker"].capability("list_positions"),
                   alien_payload) == expected
    assert extract("list_positions",
                   SPECS["robinhood-trading"].capability("list_positions"),
                   rh_payload) == expected


def test_the_two_connectors_spell_a_buy_differently():
    _, alien = resolve("place_order",
                       SPECS["alien-broker"].capability("place_order"),
                       {"side": "buy"})
    _, rh = resolve("place_order",
                    SPECS["robinhood-trading"].capability("place_order"),
                    {"side": "buy"})
    assert alien == {"action": "BUY_TO_OPEN"}
    assert rh == {"side": "buy"}


def test_the_two_connectors_extract_the_same_orders_from_different_shapes():
    alien_payload = {"working": [{"ref": "AL-ORD-1", "tkr": "aapl",
                                  "action": "BUY_TO_OPEN", "qty": "25",
                                  "state": "working"}]}
    rh_payload = {"data": {"orders": [{"id": "AL-ORD-1", "symbol": "AAPL",
                                       "side": "buy", "quantity": "25",
                                       "state": "working"}]}}
    expected = [{"order_id": "AL-ORD-1", "symbol": "AAPL", "side": "buy",
                "quantity": 25.0, "status": "working"}]
    assert extract("list_orders",
                   SPECS["alien-broker"].capability("list_orders"),
                   alien_payload) == expected
    assert extract("list_orders",
                   SPECS["robinhood-trading"].capability("list_orders"),
                   rh_payload) == expected


def test_the_two_connectors_resolve_a_cancel_differently():
    args = {"order_id": "ORD-1", "account": "X"}
    assert resolve("cancel_order",
                   SPECS["alien-broker"].capability("cancel_order"),
                   args) == ("scrub", {"ref": "ORD-1", "acct": "X"})
    assert resolve("cancel_order",
                   SPECS["robinhood-trading"].capability("cancel_order"),
                   args) == ("cancel_order",
                             {"order_id": "ORD-1", "account_number": "X"})
