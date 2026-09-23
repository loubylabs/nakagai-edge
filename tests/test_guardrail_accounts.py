"""Account refusals name no other account; a broker with no confirmed account
takes no write (platform spec 2026-09-23, sections 4.4 and 4.8)."""

from nakagai_edge.config import ConnectorSpec
from nakagai_edge.guardrails import evaluate


def spec(role="broker", **accounts):
    return ConnectorSpec(id="broker", kind="mcp-http", role=role, url="https://x.test/mcp",
                         enabled=True, guardrails={"read_only_tools": ["get_*"],
                                                   "allow_writes": True, "accounts": accounts})


def test_a_refused_account_names_only_itself_and_the_connector():
    v = evaluate(spec(allow=["463605220"], read=["5QU41901"]), "get_portfolio",
                 {"account_number": "999"})
    assert v.decision == "deny"
    assert "'999'" in v.reason and "'broker'" in v.reason
    assert "463605220" not in v.reason and "5QU41901" not in v.reason


def test_the_presence_refusal_names_no_account():
    v = evaluate(spec(allow=["463605220"], read=["5QU41901"]), "place_equity_order", {})
    assert v.decision == "deny" and "names no account" in v.reason
    assert "463605220" not in v.reason and "5QU41901" not in v.reason


def test_a_broker_with_no_confirmed_account_takes_no_write():
    v = evaluate(spec(), "place_equity_order", {"account_number": "463605220"})
    assert v.decision == "deny" and "no account confirmed" in v.reason


def test_a_broker_with_no_confirmed_account_still_reads_every_account():
    assert evaluate(spec(), "get_portfolio", {"account_number": "463605220"}).decision == "allow"


def test_a_signals_connector_with_no_tiers_is_unchanged():
    assert evaluate(spec(role="signals"), "place_thing", {}).decision == "allow"
