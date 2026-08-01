import pytest

from nakagai_edge.capability import CAPABILITIES, CapabilityError
from nakagai_edge.config import ConnectorSpec, load_specs
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

BASE = {"id": "demo", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp/", "enabled": True}


def test_a_connector_parses_its_capability_map():
    spec = ConnectorSpec(**BASE, capabilities={
        "get_balance": {"tool": "get_portfolio",
                        "args": {"account": "account_number"},
                        "fields": {"equity": ["data.total_value"]}}})
    cap = spec.capability("get_balance")
    assert cap.tool == "get_portfolio"
    assert cap.fields["equity"] == ["data.total_value"]


def test_an_unknown_capability_name_is_rejected_at_parse_time():
    with pytest.raises(ValueError, match="get_horoscope"):
        ConnectorSpec(**BASE, capabilities={"get_horoscope": {"tool": "x"}})


def test_a_position_map_with_no_symbol_path_is_rejected_at_parse_time():
    # The release path this closes: read_partial skips the required check on
    # purpose, so without a `symbol` path the sweep produces rows nothing can
    # name, and a position that cannot be named is missing from both
    # held_quantities and unreadable(), which reconcile reads as closed. The
    # record is RELEASED, its stop stops being watched, and the portfolio
    # document still displays the row. Nothing downstream catches it, so
    # parse time is the only place it can be caught.
    with pytest.raises(ValueError, match="demo.*list_positions.*symbol"):
        ConnectorSpec(**BASE, capabilities={
            "list_positions": {"tool": "holdings",
                               "items": ["holdings"],
                               "fields": {"quantity": ["qty"]}}})


def test_a_position_map_with_no_quantity_path_is_rejected_at_parse_time():
    with pytest.raises(ValueError, match="demo.*list_positions.*quantity"):
        ConnectorSpec(**BASE, capabilities={
            "list_positions": {"tool": "holdings",
                               "items": ["holdings"],
                               "fields": {"symbol": ["ticker"]}}})


def test_a_map_declaring_no_fields_at_all_is_rejected():
    # The likeliest shape of the mistake: `fields` forgotten entirely, which
    # today reads as a capability that is present and mapped.
    with pytest.raises(ValueError, match="demo.*get_balance.*equity"):
        ConnectorSpec(**BASE, capabilities={"get_balance": {"tool": "balances"}})


def test_a_map_declaring_every_required_field_parses():
    spec = ConnectorSpec(**BASE, capabilities={
        "list_positions": {"tool": "holdings", "items": ["holdings"],
                           "fields": {"symbol": ["ticker"], "quantity": ["qty"]}}})
    assert spec.capability("list_positions").fields["symbol"] == ["ticker"]


def test_the_write_capabilities_require_nothing_and_parse_with_no_fields():
    # place_order and cancel_order are written, not read: their vocabulary
    # entries have empty `required` tuples, so a map for either carries no
    # `fields` at all and must not be caught by the check above.
    assert CAPABILITIES["place_order"].required == ()
    assert CAPABILITIES["cancel_order"].required == ()
    spec = ConnectorSpec(**BASE, capabilities={
        "place_order": {"tool": "submit", "args": {"symbol": "ticker"},
                        "market_args": {"kind": "MARKET"}},
        "cancel_order": {"tool": "scrub", "args": {"order_id": "ref"}}})
    assert spec.capability("place_order").fields == {}


def test_both_shipped_fixture_connectors_map_every_field_they_need():
    # These two are what every migrated path is tested against. If either had
    # a hole, the validator would be describing a rule the fixtures break.
    specs = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})
    assert sorted(specs) == ["alien-broker", "robinhood-trading"]
    for spec in specs.values():
        for name, cap in spec.capabilities.items():
            assert set(CAPABILITIES[name].required) <= set(cap.fields)


def test_an_unmapped_capability_refuses_and_names_the_connector():
    spec = ConnectorSpec(**BASE)
    with pytest.raises(CapabilityError, match="demo.*get_balance"):
        spec.capability("get_balance")


def test_the_account_key_defaults_from_the_guardrail_arg_names():
    spec = ConnectorSpec(**BASE, capabilities={
        "list_positions": {"tool": "get_equity_positions",
                           "items": ["data.positions"],
                           "fields": {"symbol": ["symbol"],
                                      "quantity": ["quantity"]}}})
    assert spec.capability("list_positions").args["account"] == "account_number"


def test_an_explicit_account_key_wins_over_the_default():
    spec = ConnectorSpec(**BASE, guardrails={"accounts": {"arg_names": ["acct"]}},
                         capabilities={
        "list_positions": {"tool": "positions", "args": {"account": "account_id"},
                           "items": ["positions"],
                           "fields": {"symbol": ["sym"], "quantity": ["qty"]}}})
    assert spec.capability("list_positions").args["account"] == "account_id"


def test_capability_names_are_reported_for_the_agent():
    spec = ConnectorSpec(**BASE, capabilities={
        "get_balance": {"tool": "get_portfolio",
                        "fields": {"equity": ["equity"]}}})
    assert spec.capability_names == ["get_balance"]


def test_load_specs_carries_capabilities_through_the_registry():
    specs = load_specs({"connectors": [
        {**BASE, "capabilities": {"get_quote": {
            "tool": "get_quotes", "args": {"symbols": "symbols"},
            "items": ["data.quotes"],
            "fields": {"symbol": ["symbol"], "price": ["last_trade_price"]}}}}]})
    assert specs["demo"].capability("get_quote").tool == "get_quotes"


def test_a_capability_that_takes_no_account_gets_no_account_key():
    spec = ConnectorSpec(**BASE, capabilities={
        "list_accounts": {"tool": "get_accounts",
                          "items": ["data.accounts"],
                          "fields": {"account": ["account_number"]}}})
    assert "account" not in spec.capability("list_accounts").args


def test_two_specs_sharing_a_capability_object_each_get_their_own_account_key():
    from nakagai_edge.capability import Capability
    shared = Capability(tool="get_equity_positions",
                        items=["data.positions"],
                        fields={"symbol": ["symbol"], "quantity": ["quantity"]})
    first = ConnectorSpec(**BASE, capabilities={"list_positions": shared})
    second = ConnectorSpec(**{**BASE, "id": "other"},
                           guardrails={"accounts": {"arg_names": ["acct"]}},
                           capabilities={"list_positions": shared})
    assert first.capability("list_positions").args["account"] == "account_number"
    assert second.capability("list_positions").args["account"] == "acct"
