import re
from pathlib import Path

import pytest
import yaml

from nakagai_edge.capability import (CAPABILITIES, OUTBOUND_ORDER_FIELDS,
                                     CapabilityError, resolve)
from nakagai_edge.config import ConnectorSpec, load_specs
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR

BASE = {"id": "demo", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp/", "enabled": True}

ORDER_ARGS = {"symbol": "ticker", "side": "action", "order_type": "kind",
              "quantity": "qty", "limit_price": "limit",
              "stop_price": "trigger", "time_in_force": "tif",
              "account": "acct"}
ORDER_TYPES = {field: "string" for field in OUTBOUND_ORDER_FIELDS}


def _order_map(*dropped: str) -> dict:
    """A complete place_order map minus the named argument keys."""
    return {"place_order": {
        "tool": "submit",
        "args": {k: v for k, v in ORDER_ARGS.items() if k not in dropped},
        "outbound_types": {**ORDER_TYPES},
        "values": {"side": {"buy": ["BUY"], "sell": ["SELL"]},
                   "order_type": {"limit": ["LIMIT"], "market": ["MARKET"]}}}}


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
    # `fields` at all and must not be caught by the check above. `args` is a
    # separate rule with its own validator, which is why place_order still
    # names all five order keys here.
    assert CAPABILITIES["place_order"].required == ()
    assert CAPABILITIES["cancel_order"].required == ()
    spec = ConnectorSpec(**BASE, capabilities={
        "place_order": {"tool": "submit", "args": ORDER_ARGS,
                        "outbound_types": ORDER_TYPES,
                        "values": {"side": {"buy": ["BUY"], "sell": ["SELL"]},
                                   "order_type": {"limit": ["LIMIT"], "market": ["MARKET"]}}},
        "cancel_order": {"tool": "scrub", "args": {"order_id": "ref"}}})
    assert spec.capability("place_order").fields == {}


# ---- the argument side of the same rule -----------------------------------
#
# A place_order map is the only one whose ARGUMENT keys stay load-bearing after
# the order is gone: warrant.read_entry reads the executed payload back through
# them to build the ledger record the brake watches.
#
# Both tests below match on the COMPUTED clause, "no argument key for <list>;",
# and not merely on a field name. The message also carries the constant tail
# "all of symbol, side, quantity, price, stop must be mapped", which names all
# five whatever the code worked out, so a looser pattern passes even when the
# computed list is wrong or empty and pins nothing at all.


def test_an_order_map_with_no_stop_price_key_is_rejected_at_parse_time():
    # The release path this closes: with no `stop` key, read_entry returns None
    # for every order this connector places, so executor.supervise returns
    # early. No ledger record, no `blocked` reason, no anomaly and no audit
    # line: the position is missing from get_open_risk entirely while the
    # Portfolio page still shows it, and nothing anywhere says why.
    with pytest.raises(ValueError,
                       match="demo.*place_order.*no argument key for stop_price;"):
        ConnectorSpec(**BASE, capabilities=_order_map("stop_price"))


def test_an_order_map_missing_two_keys_names_both():
    # One hidden behind another costs a second edit and a second deploy to
    # find, with real orders placed in between.
    with pytest.raises(ValueError,
                       match="demo.*no argument key for limit_price, stop_price;"):
        ConnectorSpec(**BASE, capabilities=_order_map("limit_price", "stop_price"))


def test_an_order_map_declaring_all_five_parses():
    spec = ConnectorSpec(**BASE, capabilities=_order_map())
    assert spec.capability("place_order").args["stop_price"] == "trigger"


def test_an_order_map_with_no_outbound_type_is_rejected_at_parse_time():
    cap = _order_map()["place_order"]
    del cap["outbound_types"]["order_type"]
    with pytest.raises(ValueError, match="demo.*outbound type.*order_type"):
        ConnectorSpec(**BASE, capabilities={"place_order": cap})


def test_an_order_map_with_an_unknown_outbound_type_is_rejected_at_parse_time():
    cap = _order_map()["place_order"]
    cap["outbound_types"]["quantity"] = "integer"
    with pytest.raises(ValueError, match="demo.*unsupported outbound types.*integer"):
        ConnectorSpec(**BASE, capabilities={"place_order": cap})


def test_an_order_map_cannot_add_a_canonical_order_type_value():
    cap = _order_map()["place_order"]
    cap["values"]["order_type"]["stop_limit"] = ["stop_limit"]
    with pytest.raises(ValueError, match="demo.*unsupported canonical order_type.*stop_limit"):
        ConnectorSpec(**BASE, capabilities={"place_order": cap})


def test_a_connector_that_declares_no_place_order_still_parses():
    # A quotes or positions connector is not a broken broker, and this rule is
    # about what an order IS, not about what every connector must serve.
    spec = ConnectorSpec(**BASE, capabilities={
        "get_quote": {"tool": "ticker", "args": {"symbols": "tickers"},
                      "fields": {"symbol": ["tkr"], "price": ["last"]}}})
    assert spec.capability_names == ["get_quote"]


def test_both_shipped_fixture_connectors_map_every_field_they_need():
    # These two are what every migrated path is tested against. If either had
    # a hole, the validator would be describing a rule the fixtures break.
    specs = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})
    assert sorted(specs) == ["alien-broker", "robinhood-trading"]
    for spec in specs.values():
        for name, cap in spec.capabilities.items():
            assert set(CAPABILITIES[name].required) <= set(cap.fields)
        # And the argument side: both of these place orders, so both have to
        # be readable back into a supervised position.
        assert set(OUTBOUND_ORDER_FIELDS) <= set(spec.capability("place_order").args)
        assert set(OUTBOUND_ORDER_FIELDS) <= set(spec.capability("place_order").outbound_types)


def test_the_readme_registry_example_parses_and_resolves_as_written():
    """The README's own map is executed, not just read.

    It is printed under "adding a broker is data, not code", which is an
    invitation to copy it, so an example that no longer parses teaches the
    exact misconfiguration the validator above exists to refuse. Pinned here
    rather than proofread, because prose drifts and a test does not.
    """
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    blocks = re.findall(r"```yaml\n(.*?)```", readme, re.DOTALL)
    entries = [e for b in blocks for e in (yaml.safe_load(b) or [])
               if isinstance(e, dict) and "capabilities" in e]
    assert len(entries) == 1, "the README should show exactly one registry map"

    spec = load_specs({"connectors": entries})["alien-broker"]
    tool, args = resolve("place_order", spec.capability("place_order"),
                         {"symbol": "AAPL", "side": "buy", "order_type": "limit",
                          "quantity": 10, "limit_price": 187.20,
                          "stop_price": 180.0, "time_in_force": "day",
                          "account": "AL-1"})
    assert tool == "submit"
    # The broker's own words throughout, and `buy` sent as the first alias
    # `values.side` lists, which is what the paragraph under the example says.
    assert args == {"ticker": "AAPL", "action": "BUY", "kind": "LIMIT",
                    "qty": "10", "limit": "187.2", "trigger": "180.0",
                    "tif": "day", "acct": "AL-1"}


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
