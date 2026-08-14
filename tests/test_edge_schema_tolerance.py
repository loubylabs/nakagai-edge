"""A broker that breaks its own output schema, through the real session.

test_response_schema.py owns the rule. This owns the wiring: that hub.call
reaches the classifier at all, that a tolerated payload still arrives as data,
that a fatal one still raises, and that the log says so once rather than every
sweep.
"""

import logging

import pytest
import yaml

from nakagai_edge.hub import ConnectorHub

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture()
def hub(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "connectors.yaml").write_text(yaml.safe_dump({
        "connectors": [{
            "id": "lying-broker", "name": "Lying broker", "kind": "mcp-stdio",
            "role": "broker", "enabled": True, "command": "uv",
            "args": ["run", "python", "tests/fixtures/lying_broker_mcp.py"],
            "guardrails": {"read_only_tools": ["get_*"], "tools": {"allow": ["get_*"]}},
        }]}))
    return ConnectorHub(tmp_path)


async def test_an_undeclared_property_still_returns_data(hub):
    """The live failure, end to end: the sweep gets its accounts back."""
    try:
        out = await hub.call("lying-broker", "get_accounts", {}, account_key="test-account")
    finally:
        await hub.aclose()
    accounts = out["data"]["accounts"]
    assert accounts[0]["account_number"] == "463605220"
    assert accounts[0]["unsettled_funds"] == "0.0000"


async def test_a_wrong_type_still_raises(hub):
    """The property this change must not cost: a declared field changing shape
    is still a refusal, not a warning."""
    with pytest.raises(RuntimeError, match="Invalid structured content"):
        try:
            await hub.call("lying-broker", "get_accounts_wrong_type", {},
                           account_key="test-account")
        finally:
            await hub.aclose()


async def test_the_tolerated_payload_is_logged_once(hub, caplog):
    """Tolerating is not silence, and it is not a log line every 300 seconds
    either: the sweep runs on a timer and would otherwise repeat this forever."""
    with caplog.at_level(logging.WARNING, logger="nakagai.edge"):
        try:
            await hub.call("lying-broker", "get_accounts", {},
                           account_key="test-account")
            await hub.call("lying-broker", "get_accounts", {},
                           account_key="test-account")
        finally:
            await hub.aclose()
    warnings = [r for r in caplog.records if "unsettled_funds" in r.getMessage()]
    assert len(warnings) == 1, [r.getMessage() for r in warnings]
    assert "lying-broker" in warnings[0].getMessage()
    assert "get_accounts" in warnings[0].getMessage()
