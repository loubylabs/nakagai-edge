"""The snapshot loop's assembly and push. Fixture-driven: a fake hub, no
network, no broker. The numbers only ever originate from the edge's own
broker calls; an agent poke carries no data."""

import asyncio

import httpx
import pytest
import yaml

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.portfolio import (
    PORTFOLIO_INTERVAL_S, REFRESH_MIN_INTERVAL_S, PortfolioReporter,
    broker_specs, connector_snapshot, tiered_accounts)
from nakagai_edge.config import ConnectorSpec

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _spec(**over):
    base = dict(id="robinhood-trading", kind="mcp-http", role="broker",
                url="https://x.test/mcp", enabled=True,
                guardrails={"tools": {"allow": ["get_*"]},
                            "read_only_tools": ["get_*"],
                            "accounts": {"allow": ["463605220"],
                                         "read": ["5QU41901"]}})
    base.update(over)
    return ConnectorSpec(**base)


ACCOUNTS = {"accounts": [
    {"account_number": "5QU41901", "type": "margin", "is_default": True},
    {"account_number": "463605220", "type": "cash", "nickname": "Agentic"},
]}
PORTFOLIO = {"total_value": "1000", "cash": "1000", "currency": "USD",
             "buying_power": {"buying_power": "1000.0000"}}
POSITIONS = {"positions": [{"symbol": "SPY", "quantity": "10",
                            "average_buy_price": "500.00", "type": "equity"}]}


class FakeHub:
    """hub.call's contract: {"data": <downstream>}, raising on refusal.
    Robinhood nests its own {"data": ..., "guide": ...} envelope, so the
    canned responses here nest one too, to prove _unwrap peels it."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def call(self, connector_id, tool, args):
        self.calls.append((connector_id, tool, dict(args)))
        key = (tool, str(args.get("account_number", "")))
        out = self.responses[key]
        if isinstance(out, Exception):
            raise out
        return {"connector": connector_id, "tool": tool, "is_write": False,
                "is_error": False, "data": {"data": out, "guide": "ignore me"}}


def _hub_ok():
    return FakeHub({
        ("get_accounts", ""): ACCOUNTS,
        ("get_portfolio", "463605220"): PORTFOLIO,
        ("get_equity_positions", "463605220"): POSITIONS,
        ("get_portfolio", "5QU41901"): {"total_value": "2500", "cash": "100"},
        ("get_equity_positions", "5QU41901"): {"positions": []},
    })


# ---- tiering -------------------------------------------------------------

def test_tiered_accounts_unions_both_tiers_with_their_labels():
    got = tiered_accounts(_spec(), ACCOUNTS["accounts"])
    by_num = {a["account_number"]: tier for a, tier in got}
    assert by_num == {"463605220": "full", "5QU41901": "read"}


def test_no_account_lists_means_every_listed_account_at_full_tier():
    s = _spec(guardrails={"tools": {"allow": ["get_*"]},
                          "read_only_tools": ["get_*"]})
    got = tiered_accounts(s, ACCOUNTS["accounts"])
    assert [(a["account_number"], t) for a, t in got] == [
        ("5QU41901", "full"), ("463605220", "full")]


def test_a_configured_account_the_broker_did_not_list_is_still_fetched():
    got = tiered_accounts(_spec(), [])
    assert {a["account_number"] for a, _ in got} == {"463605220", "5QU41901"}


# ---- assembly ------------------------------------------------------------

async def test_connector_snapshot_carries_totals_positions_and_tiers():
    entry = await connector_snapshot(_hub_ok(), _spec())
    assert entry["id"] == "robinhood-trading" and entry["error"] == ""
    by_num = {a["account_number"]: a for a in entry["accounts"]}
    agentic = by_num["463605220"]
    assert agentic["tier"] == "full" and agentic["nickname"] == "Agentic"
    assert agentic["portfolio"]["total_value"] == "1000"
    assert agentic["positions"][0]["symbol"] == "SPY"
    margin = by_num["5QU41901"]
    assert margin["tier"] == "read"
    assert margin["portfolio"]["total_value"] == "2500"
    assert margin["positions"] == []


async def test_one_accounts_failure_does_not_blank_its_siblings():
    hub = _hub_ok()
    hub.responses[("get_portfolio", "5QU41901")] = RuntimeError("broker hiccup")
    entry = await connector_snapshot(hub, _spec())
    by_num = {a["account_number"]: a for a in entry["accounts"]}
    assert "broker hiccup" in by_num["5QU41901"]["error"]
    assert by_num["5QU41901"]["portfolio"] == {}
    assert by_num["463605220"]["portfolio"]["total_value"] == "1000"
    assert by_num["463605220"]["error"] == ""


async def test_a_dead_get_accounts_degrades_to_a_connector_level_error():
    hub = FakeHub({("get_accounts", ""): RuntimeError("token expired")})
    entry = await connector_snapshot(hub, _spec())
    assert "token expired" in entry["error"]
    assert entry["accounts"] == []


# ---- spec discovery ------------------------------------------------------

def test_broker_specs_reads_only_enabled_mcp_brokers(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "connectors.yaml").write_text(yaml.safe_dump(
        {"connectors": [
            {"id": "robinhood-trading", "kind": "mcp-http", "role": "broker",
             "url": "https://x.test/mcp", "enabled": True},
            {"id": "demo-broker", "kind": "mcp-stdio", "role": "broker",
             "command": "python", "enabled": True},
            {"id": "disabled-broker", "kind": "mcp-http", "role": "broker",
             "url": "https://y.test/mcp", "enabled": False},
            {"id": "alpaca-data", "kind": "data", "role": "data", "enabled": True},
        ]}))
    assert [s.id for s in broker_specs(tmp_path)] == [
        "robinhood-trading", "demo-broker"]


def test_broker_specs_with_no_registry_is_empty(tmp_path):
    assert broker_specs(tmp_path) == []


# ---- the reporter: one path, rate-limited, pushes to the platform ---------

class _State:
    def __init__(self, root):
        self.root = root


def _reporter(tmp_path, hub, handler):
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(yaml.safe_dump(
        {"connectors": [{
            "id": "robinhood-trading", "kind": "mcp-http", "role": "broker",
            "url": "https://x.test/mcp", "enabled": True,
            "guardrails": {"tools": {"allow": ["get_*"]},
                           "read_only_tools": ["get_*"],
                           "accounts": {"allow": ["463605220"],
                                        "read": ["5QU41901"]}}}]}))
    client = PlatformClient("http://platform.test", "nk_agent_x",
                            transport=httpx.MockTransport(handler))
    return PortfolioReporter(_State(tmp_path), hub, client)


async def test_snapshot_and_push_posts_the_document(tmp_path):
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["body"] = request.read().decode()
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    reporter = _reporter(tmp_path, _hub_ok(), handler)
    doc = await reporter.snapshot_and_push()
    assert seen["path"] == "/api/agent/portfolio"
    assert "463605220" in seen["body"] and "5QU41901" in seen["body"]
    assert doc["connectors"][0]["id"] == "robinhood-trading"


async def test_two_pokes_inside_the_window_are_one_broker_sweep(tmp_path):
    hub = _hub_ok()
    reporter = _reporter(tmp_path, hub,
                         lambda r: httpx.Response(200, json={"ok": True}))
    first = await reporter.snapshot_and_push()
    calls_after_first = len(hub.calls)
    second = await reporter.snapshot_and_push()
    assert len(hub.calls) == calls_after_first   # no second sweep
    assert second == first                        # the fresh-enough snapshot back


async def test_a_down_platform_does_not_lose_the_snapshot(tmp_path):
    """Best-effort push: the sweep result is still returned (the refresh tool
    hands it to the agent) even when the POST fails."""
    def handler(request):
        return httpx.Response(503, json={"detail": "down"})

    reporter = _reporter(tmp_path, _hub_ok(), handler)
    doc = await reporter.snapshot_and_push()
    assert doc["connectors"][0]["accounts"]      # figures survived the 503


def test_the_report_client_method_hits_the_agent_portfolio_route():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["auth"] = request.headers["authorization"]
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    c = PlatformClient("http://platform.test", "nk_agent_x",
                       transport=httpx.MockTransport(handler))
    out = c.report_portfolio([{"id": "robinhood-trading", "error": "",
                               "accounts": []}])
    assert out["ok"] is True
    assert seen["path"] == "/api/agent/portfolio"
    assert seen["auth"] == "Bearer nk_agent_x"
