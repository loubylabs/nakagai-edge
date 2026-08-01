import pytest

from nakagai_edge.capability import CapabilityError
from nakagai_edge.config import ConnectorSpec, load_specs

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
