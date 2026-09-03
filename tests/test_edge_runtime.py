"""Edge runtime surface: freshness gate, tool passthrough, hub wiring."""

import asyncio
import contextlib
import json
import logging
import time

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.capability import Capability
from nakagai_edge.config import ConnectorSpec, load_specs
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.candidate import CandidateWakeScope
from nakagai_edge.edge.runtime import (_prepared_order, build_hub, create_edge_mcp,
                                       freshness_error)
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle, sync_once
from tests.fixtures.alien_registry import ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR
from nakagai_edge.signing import args_hash


class _NoFills:
    """The fill sweep, stubbed out. These tests are about the other loops, and a
    real sweep would dial the hub for order history no fixture here declares."""

    async def sweep(self):
        return []

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _state(tmp_path):
    s = EdgeState(tmp_path)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(s, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                     "connectors": {"connectors": []},
                     "signing_public_key": "k"}, "v1")
    return s


def _candidate_spec():
    return load_specs({"connectors": [ROBINHOOD_CONNECTOR]})["robinhood-trading"]


CANONICAL_ORDER = {
    "symbol": "AAPL", "side": "buy", "order_type": "limit",
    "quantity": 3, "limit_price": 211.25, "stop_price": 207.0,
    "time_in_force": "day", "account": "broker-account",
}

BROKER_ORDER = {
    "symbol": "AAPL", "side": "buy", "type": "limit", "quantity": "3",
    "limit_price": "211.25", "stop_price": "207.0",
    "time_in_force": "day", "account_number": "broker-account",
}


def _prepared_response(**overrides):
    prepared = {
        "connector_id": "robinhood-trading", "account_id": "broker-account",
        "tool": "place_equity_order", "args": dict(BROKER_ORDER),
        "args_hash": args_hash(BROKER_ORDER), **overrides}
    return {
        "candidate_id": "candidate-1",
        "decision": "accepted",
        "mechanical_status": "prepared",
        "mechanical_reason": "",
        "approval_id": "",
        "signal_id": "signal-1",
        "canonical_order": dict(CANONICAL_ORDER),
        "prepared_order": prepared,
    }


def _candidate_scope(state, candidate_id="candidate-1"):
    return CandidateWakeScope(state).begin({
        "seq": 1, "kind": "execution_candidate", "response_required": True,
        "candidate_id": candidate_id, "expires_at": time.time() + 300,
    })


def _outcome_ack(request):
    payload = json.loads(request.content)
    return httpx.Response(200, json={
        "candidate_id": request.url.path.split("/")[-2],
        "decision": "accepted",
        "mechanical_status": payload["mechanical_status"],
        "mechanical_reason": payload["mechanical_reason"],
        "approval_id": payload["approval_id"],
    })


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the freshness gate and
    tool passthrough, not the portfolio path, so a no-op stand-in keeps their
    intent unchanged."""

    async def snapshot_and_push(self, **_kwargs):
        return {"connectors": []}


class _ReadyReporter:
    def __init__(self):
        self.calls = []

    async def snapshot_and_push(self, **kwargs):
        self.calls.append(kwargs)
        return {"connectors": [{
            "id": "robinhood-trading", "status": "ok", "accounts": [{
                "account_number": "broker-account", "status": "ok",
                "positions": [], "open_orders": [],
            }],
        }]}


async def test_call_connector_denied_on_stale_policy(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL
    result = await mcp.call_tool("call_connector",
                                 {"connector_id": "x", "tool": "get_quote"})
    text = result.content[0].text
    assert "policy stale" in text


async def test_get_approval_denied_on_stale_policy(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL
    result = await mcp.call_tool("get_approval", {"approval_id": "anything"})
    text = result.content[0].text
    assert "policy stale" in text


async def test_status_tool_works_even_stale(tmp_path, monkeypatch):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)
    result = await mcp.call_tool("get_connector_status", {})
    text = result.content[0].text
    doc = json.loads(text)
    assert "connectors" in doc
    assert doc["schema_error"] == ""


async def test_status_tool_carries_the_schema_error(tmp_path):
    """The one tool that answers on stale policy, so it is the one place an
    agent can learn WHY the rest is refusing. `policy_fresh: false` alone reads
    as a network problem; a refused bundle needs the owner, not a retry."""
    state = _state(tmp_path)
    client = PlatformClient(
        "https://api.test", "t",
        transport=httpx.MockTransport(lambda r: httpx.Response(
            200, json={"bundle_version": "v2", "connectors": {"connectors": []}},
            headers={"etag": "v2"})))
    assert sync_once(state, client) is False

    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    result = await mcp.call_tool("get_connector_status", {})
    text = result.content[0].text
    doc = json.loads(text)
    assert "upgrade the platform" in doc["schema_error"].lower()
    assert str(BUNDLE_SCHEMA) in doc["schema_error"]


def test_build_hub_exports_agent_token_env(tmp_path, monkeypatch):
    monkeypatch.delenv("NAKAGAI_AGENT_TOKEN", raising=False)
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    import os
    assert os.environ["NAKAGAI_AGENT_TOKEN"] == "nk_agent_t"
    assert hub.root == state.root


def test_freshness_error_is_json_with_is_error(tmp_path):
    doc = json.loads(freshness_error())
    assert doc["is_error"] is True and "policy stale" in doc["error"]


async def test_agent_checkin_forwards_to_the_platform_and_is_not_gated_on_staleness(
        tmp_path, monkeypatch):
    """agent_checkin talks straight to the platform rather than reading cached
    policy, so it must keep working past the freshness TTL that blocks every
    connector tool - the check-in is exactly what would let a reconnecting
    edge prove itself alive."""
    seen = []

    def handler(req):
        seen.append(json.loads(req.content))
        assert req.url.path == "/api/agent/checkin"
        return httpx.Response(200, json={"ok": True, "mandate": {"preset": "advisor"}})

    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s TTL

    result = await mcp.call_tool("agent_checkin", {
        "status": "scanning", "note": "watching NVDA",
        "account_equity": 100_000.0, "day_pnl": -1_500.0})
    text = result.content[0].text
    doc = json.loads(text)

    assert doc == {"ok": True, "mandate": {"preset": "advisor"}}
    assert seen == [{"status": "scanning", "note": "watching NVDA",
                     "account_equity": 100_000.0, "day_pnl": -1_500.0}]


async def test_agent_checkin_platform_error_returns_is_error_json(tmp_path):
    """The platform being unreachable or rejecting the token comes back as an
    ordinary is_error payload, never a traceback."""
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(401)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("agent_checkin", {"status": "scanning"})
    text = result.content[0].text
    doc = json.loads(text)

    assert doc["is_error"] is True
    assert "revoked" in doc["error"]


async def test_candidate_tools_publish_candidate_id_rationale_and_memory_ids(tmp_path):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    tools = {tool.name: tool for tool in await mcp.list_tools()}

    assert "place_order" not in tools
    for name in ("accept_candidate", "abstain_candidate"):
        assert set(tools[name].input_schema["properties"]) == {
            "candidate_id", "rationale", "memory_ids"}
        assert set(tools[name].input_schema["required"]) == {
            "candidate_id", "rationale"}


async def test_candidate_scope_allows_only_the_same_candidate_decision(tmp_path):
    requests = []

    def handler(req):
        requests.append(req.url.path)
        return httpx.Response(200, json={
            "candidate_id": "candidate-1", "decision": "abstained"})

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    wrong = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-2", "rationale": "wrong wake"})
    right = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-1", "rationale": "bounded decision"})

    assert json.loads(wrong.content[0].text)["is_error"] is True
    assert "candidate-1" in json.loads(wrong.content[0].text)["error"]
    assert json.loads(right.content[0].text)["decision"] == "abstained"
    assert requests == ["/api/agent/candidates/candidate-1/abstain"]


async def test_candidate_scope_denies_direct_platform_writes(tmp_path):
    requests = []

    def handler(req):
        requests.append(req.url.path)
        return httpx.Response(200, json={"ok": True})

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    calls = [
        ("agent_checkin", {"status": "idle"}),
        ("claim_message", {"message_seq": 1}),
        ("send_message", {"text": "x", "room_id": "desk",
                          "idempotency_key": "x"}),
        ("request_peer", {"agent_ids": ["a2"], "text": "x",
                          "idempotency_key": "x"}),
        ("refresh_portfolio", {}),
    ]
    for tool, args in calls:
        result = await mcp.call_tool(tool, args)
        doc = json.loads(result.content[0].text)
        assert doc["is_error"] is True, tool
        assert "candidate wake" in doc["error"], tool

    assert requests == []


@pytest.mark.parametrize("signal_id", [None, "", "   ", 7])
def test_prepared_order_requires_one_nonempty_platform_signal_id(signal_id):
    response = _prepared_response()
    if signal_id is None:
        del response["signal_id"]
    else:
        response["signal_id"] = signal_id

    with pytest.raises(ValueError, match="signal_id"):
        _prepared_order(
            "candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_rejects_additional_response_fields():
    response = _prepared_response()
    response["unreviewed"] = True

    with pytest.raises(ValueError, match="candidate acceptance.*fields"):
        _prepared_order(
            "candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_returns_the_exact_signal_binding():
    prepared = _prepared_order(
        "candidate-1", _prepared_response(), _candidate_spec(), "broker-account")

    assert prepared["signal_id"] == "signal-1"


@pytest.mark.parametrize("missing", [
    "connector_id", "account_id", "tool", "args", "args_hash",
])
def test_prepared_order_rejects_each_missing_required_field(missing):
    response = _prepared_response()
    response["prepared_order"].pop(missing)

    with pytest.raises(ValueError, match="missing or additional"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_rejects_additional_fields():
    response = _prepared_response(unreviewed=True)

    with pytest.raises(ValueError, match="missing or additional"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


@pytest.mark.parametrize(("field", "value"), [
    ("connector_id", 7), ("account_id", 7), ("tool", 7),
    ("args", []), ("args_hash", 7),
])
def test_prepared_order_rejects_wrong_field_types(field, value):
    response = _prepared_response(**{field: value})

    with pytest.raises(ValueError):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


@pytest.mark.parametrize(("field", "value", "message"), [
    ("connector_id", "other-broker", "evidence connector"),
    ("account_id", "other-account", "evidence account"),
    ("tool", "alternate_approved_write", "place_order tool"),
])
def test_prepared_order_rejects_authority_substitution(field, value, message):
    response = _prepared_response(**{field: value})

    with pytest.raises(ValueError, match=message):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_rejects_account_substitution_inside_frozen_args():
    response = _prepared_response()
    response["prepared_order"]["args"] = {
        **response["prepared_order"]["args"], "account_number": "other-account"}
    response["prepared_order"]["args_hash"] = args_hash(
        response["prepared_order"]["args"])

    with pytest.raises(ValueError, match="local capability map"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_rejects_args_hash_mismatch():
    response = _prepared_response(args_hash="wrong")

    with pytest.raises(ValueError, match="hash mismatch"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_requires_an_exact_canonical_order():
    response = _prepared_response()
    del response["canonical_order"]

    with pytest.raises(ValueError, match="canonical_order"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_prepared_order_rejects_local_capability_map_drift():
    response = _prepared_response()
    response["canonical_order"]["quantity"] = 4

    with pytest.raises(ValueError, match="local capability map"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


def test_blocked_candidate_response_cannot_carry_a_prepared_order():
    response = _prepared_response()
    response["mechanical_status"] = "blocked"

    with pytest.raises(ValueError, match="blocked.*prepared"):
        _prepared_order("candidate-1", response, _candidate_spec(), "broker-account")


async def test_accept_candidate_refreshes_broker_evidence_then_submits_exact_order(
        tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    calls = []
    broker_args = dict(BROKER_ORDER)

    def handler(req):
        calls.append((req.url.path, json.loads(req.content) if req.content else None))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/accept"):
            return httpx.Response(200, json={
                "candidate_id": "candidate-1", "decision": "accepted",
                "mechanical_status": "prepared", "mechanical_reason": "",
                "approval_id": "", "signal_id": "signal-1",
                "canonical_order": dict(CANONICAL_ORDER),
                "prepared_order": {
                    "connector_id": "robinhood-trading",
                    "account_id": "broker-account",
                    "tool": "place_equity_order", "args": broker_args,
                    "args_hash": args_hash(broker_args),
                },
            })
        raise AssertionError(req.url.path)

    class Hub:
        account_key = "ag1"

        def __init__(self):
            self.calls = []

        async def call(self, connector_id, tool, args, **kwargs):
            self.calls.append((connector_id, tool, args, kwargs))
            return {"approval_required": True, "approval_id": "approval-1",
                    "status": "pending", "expires_at": 123.0, "is_write": True}

    class Reporter:
        def __init__(self):
            self.calls = 0

        async def snapshot_and_push(self, **kwargs):
            self.calls += 1
            assert kwargs == {"force": True, "require_ack": True}
            return {"connectors": [{
                "id": "robinhood-trading", "status": "ok", "accounts": [{
                    "account_number": "broker-account", "status": "ok",
                    "positions": [], "open_orders": [],
                }],
            }]}

    state = _state(tmp_path)
    _candidate_scope(state)
    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])

    async def evidence(_hub, _spec, account):
        assert account == "broker-account"
        return {"status": "ok", "equity": 100_000.0, "day_pnl": -250.5}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub, reporter = Hub(), Reporter()
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, reporter,
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "broker state is current"})
    doc = json.loads(result.content[0].text)

    assert calls == [
        ("/api/agent/checkin", {"status": "research",
                                "note": "candidate broker evidence refreshed",
                                "account_equity": 100_000.0,
                                "day_pnl": -250.5}),
        ("/api/agent/candidates/candidate-1/accept",
         {"rationale": "broker state is current"}),
    ]
    assert reporter.calls == 1
    assert hub.calls == [("robinhood-trading", "place_equity_order", broker_args, {
        "account_key": "ag1", "signal_id": "signal-1",
        "candidate_id": "candidate-1",
        "intent_account": "broker-account", "require_approval": True})]
    assert doc == {
        "candidate_id": "candidate-1",
        "decision": "accepted",
        "mechanical_status": "prepared",
        "mechanical_reason": "",
        "approval_id": "approval-1",
        "status": "pending",
    }
    assert "prepared_order" not in doc


async def test_accept_candidate_sends_a_populated_memory_ids_list(tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    calls = []
    broker_args = dict(BROKER_ORDER)

    def handler(req):
        calls.append((req.url.path, json.loads(req.content) if req.content else None))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/accept"):
            return httpx.Response(200, json={
                "candidate_id": "candidate-1", "decision": "accepted",
                "mechanical_status": "prepared", "mechanical_reason": "",
                "approval_id": "", "signal_id": "signal-1",
                "canonical_order": dict(CANONICAL_ORDER),
                "prepared_order": {
                    "connector_id": "robinhood-trading",
                    "account_id": "broker-account",
                    "tool": "place_equity_order", "args": broker_args,
                    "args_hash": args_hash(broker_args),
                },
            })
        raise AssertionError(req.url.path)

    class Hub:
        account_key = "ag1"

        def __init__(self):
            self.calls = []

        async def call(self, connector_id, tool, args, **kwargs):
            self.calls.append((connector_id, tool, args, kwargs))
            return {"approval_required": True, "approval_id": "approval-1",
                    "status": "pending", "expires_at": 123.0, "is_write": True}

    class Reporter:
        async def snapshot_and_push(self, **kwargs):
            return {"connectors": [{
                "id": "robinhood-trading", "status": "ok", "accounts": [{
                    "account_number": "broker-account", "status": "ok",
                    "positions": [], "open_orders": [],
                }],
            }]}

    state = _state(tmp_path)
    _candidate_scope(state)
    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])

    async def evidence(_hub, _spec, account):
        return {"status": "ok", "equity": 100_000.0, "day_pnl": -250.5}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "matches a stated fact",
        "memory_ids": ["mem-7"]})
    doc = json.loads(result.content[0].text)

    accept_call = next(body for path, body in calls if path.endswith("/accept"))
    assert accept_call == {"rationale": "matches a stated fact",
                           "memory_ids": ["mem-7"]}
    assert doc["decision"] == "accepted"


async def test_accept_candidate_refuses_more_than_ten_memory_ids_locally(tmp_path):
    """Refused before any checkin or broker contact: an oversized list is a
    local mistake, not a reason to touch the broker first.

    Uses _ReadyReporter, not the no-op _Reporter, so a cap check that slipped
    below the portfolio refresh would still show up here: `reporter.calls`
    pins that snapshot_and_push itself was never reached, not just that the
    two HTTP paths this fixture happens to stub came back empty."""
    requests = []

    def handler(req):
        requests.append(req.url.path)
        raise AssertionError("an oversized memory_ids list must never be acted on")

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    reporter = _ReadyReporter()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), reporter,
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "too many beliefs cited",
        "memory_ids": [f"mem-{i}" for i in range(11)]})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert "10" in doc["error"]
    assert requests == []
    assert hub.calls == []
    assert reporter.calls == []


async def test_accept_candidate_rejects_local_map_mismatch_without_connector_contact(
        tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    broker_args = dict(BROKER_ORDER)
    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/outcome"):
            return _outcome_ack(req)
        canonical = dict(CANONICAL_ORDER)
        canonical["quantity"] = 4
        response = _prepared_response()
        response["canonical_order"] = canonical
        return httpx.Response(200, json=response)

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])
    monkeypatch.setattr(runtime, "balance_evidence", lambda *_args: None)

    async def evidence(*_args):
        return {"status": "ok", "equity": 10.0, "day_pnl": 0.0}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _ReadyReporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "accept"})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert "local capability map" in doc["error"]
    assert hub.calls == []
    outcome = next(body for path, body in requests if path.endswith("/outcome"))
    assert outcome["mechanical_status"] == "blocked"
    assert "local capability map" in outcome["mechanical_reason"]


async def test_accept_candidate_rejects_blocked_response_with_prepared_order(
        tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/outcome"):
            return _outcome_ack(req)
        response = _prepared_response()
        response["mechanical_status"] = "blocked"
        return httpx.Response(200, json=response)

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])

    async def evidence(*_args):
        return {"status": "ok", "equity": 10.0, "day_pnl": 0.0}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient(
        "https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = Hub()
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(
        state, hub, client, audit, _ReadyReporter(),
        Brake(state, hub, client, audit))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "accept"})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert "blocked candidate response" in doc["error"]
    assert hub.calls == []
    outcome = next(body for path, body in requests if path.endswith("/outcome"))
    assert outcome["mechanical_status"] == "blocked"
    assert "blocked candidate response" in outcome["mechanical_reason"]


async def test_accept_candidate_stale_local_policy_creates_no_approval(
        tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    broker_args = dict(BROKER_ORDER)
    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/outcome"):
            return _outcome_ack(req)
        return httpx.Response(200, json=_prepared_response())

    class Hub:
        account_key = "ag1"

        def __init__(self):
            self.calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])

    async def evidence(*_args):
        return {"status": "ok", "equity": 10.0, "day_pnl": 0.0}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _ReadyReporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)
    _candidate_scope(state)

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "accept"})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert "policy stale" in doc["error"]
    assert hub.calls == []
    outcome = next(body for path, body in requests if path.endswith("/outcome"))
    assert outcome["mechanical_status"] == "blocked"
    assert "policy stale" in outcome["mechanical_reason"]


@pytest.mark.parametrize("failure", ["portfolio", "balance"])
async def test_accepted_blocked_judgment_survives_unreadable_broker_evidence(
        tmp_path, monkeypatch, failure):
    from nakagai_edge.edge import runtime

    calls = []

    def handler(req):
        calls.append((req.url.path, json.loads(req.content)))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/accept"):
            return httpx.Response(200, json={
                "candidate_id": "candidate-1", "decision": "accepted",
                "mechanical_status": "blocked",
                "mechanical_reason": "broker evidence unreadable",
            })
        raise AssertionError(req.url.path)

    class Reporter(_ReadyReporter):
        async def snapshot_and_push(self, **kwargs):
            if failure == "portfolio":
                raise EdgeClientError("portfolio POST failed")
            return await super().snapshot_and_push(**kwargs)

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])

    async def evidence(*_args):
        if failure == "balance":
            return {"status": "unreadable", "equity": None, "day_pnl": None}
        return {"status": "ok", "equity": 10.0, "day_pnl": -1.0}

    monkeypatch.setattr(runtime, "balance_evidence", evidence)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "accept despite unreadable data"})
    doc = json.loads(result.content[0].text)

    assert doc["decision"] == "accepted"
    assert doc["mechanical_status"] == "blocked"
    assert calls[0] == ("/api/agent/checkin", {
        "status": "research", "note": "candidate broker evidence unreadable",
        "account_equity": None, "day_pnl": None,
    })
    assert calls[1][0].endswith("/accept")
    assert hub.calls == []


async def test_prepared_order_is_not_submitted_after_portfolio_report_failure(
        tmp_path, monkeypatch):
    from nakagai_edge.edge import runtime

    requests = []

    def handler(req):
        body = json.loads(req.content)
        requests.append((req.url.path, body))
        if req.url.path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        if req.url.path.endswith("/accept"):
            return httpx.Response(200, json=_prepared_response())
        if req.url.path.endswith("/outcome"):
            return _outcome_ack(req)
        raise AssertionError(req.url.path)

    class Reporter:
        async def snapshot_and_push(self, **kwargs):
            raise EdgeClientError("portfolio POST failed")

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    spec = _candidate_spec()
    spec.guardrails.accounts.allow = ["broker-account"]
    monkeypatch.setattr(runtime, "broker_specs", lambda _root: [spec])
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("accept_candidate", {
        "candidate_id": "candidate-1", "rationale": "accept"})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert hub.calls == []
    outcome = next(body for path, body in requests if path.endswith("/outcome"))
    assert outcome["mechanical_status"] == "blocked"
    assert "portfolio" in outcome["mechanical_reason"]


async def test_abstain_candidate_never_contacts_a_connector(tmp_path):
    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"candidate_id": "candidate-1",
                                         "decision": "abstained"})

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-1", "rationale": "evidence conflicts"})

    assert json.loads(result.content[0].text) == {
        "candidate_id": "candidate-1", "decision": "abstained"}
    assert requests == [("/api/agent/candidates/candidate-1/abstain",
                         {"rationale": "evidence conflicts"})]
    assert hub.calls == []


async def test_abstain_candidate_omits_memory_ids_when_empty(tmp_path):
    """An edge talking to an older platform must send the same request it
    always has: memory_ids absent from the wire body, not an empty list."""
    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"candidate_id": "candidate-1",
                                         "decision": "abstained"})

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-1", "rationale": "no memory rows",
        "memory_ids": []})

    assert json.loads(result.content[0].text) == {
        "candidate_id": "candidate-1", "decision": "abstained"}
    assert requests == [("/api/agent/candidates/candidate-1/abstain",
                         {"rationale": "no memory rows"})]
    assert "memory_ids" not in requests[0][1]


async def test_abstain_candidate_sends_a_populated_memory_ids_list(tmp_path):
    requests = []

    def handler(req):
        requests.append((req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"candidate_id": "candidate-1",
                                         "decision": "abstained"})

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-1", "rationale": "conflicts with a stated fact",
        "memory_ids": ["mem-1", "mem-2"]})

    assert json.loads(result.content[0].text) == {
        "candidate_id": "candidate-1", "decision": "abstained"}
    assert requests == [("/api/agent/candidates/candidate-1/abstain",
                         {"rationale": "conflicts with a stated fact",
                          "memory_ids": ["mem-1", "mem-2"]})]
    assert hub.calls == []


async def test_abstain_candidate_refuses_more_than_ten_memory_ids_locally(tmp_path):
    """The platform validates this list too, but the edge refuses an
    oversized list itself rather than let the platform reject the whole
    vote."""
    requests = []

    def handler(req):
        requests.append(req.url.path)
        raise AssertionError("an oversized memory_ids list must never reach the wire")

    class Hub:
        account_key = "ag1"
        calls = []

        async def call(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    state = _state(tmp_path)
    _candidate_scope(state)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    hub = Hub()
    mcp = create_edge_mcp(state, hub, client, EdgeAudit(state), _Reporter(),
                          Brake(state, hub, client, EdgeAudit(state)))

    result = await mcp.call_tool("abstain_candidate", {
        "candidate_id": "candidate-1", "rationale": "too many beliefs cited",
        "memory_ids": [f"mem-{i}" for i in range(11)]})
    doc = json.loads(result.content[0].text)

    assert doc["is_error"] is True
    assert "10" in doc["error"]
    assert requests == []
    assert hub.calls == []


async def test_local_chat_tools_are_available_while_policy_is_stale(
        tmp_path, monkeypatch):
    """Chat is direct platform correspondence, never a connector call."""
    seen = []

    def handler(req):
        seen.append((req.method, req.url.path, json.loads(req.content or b"{}")))
        return httpx.Response(200, json={"ok": True, "seq": 17})

    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)

    result = await mcp.call_tool("list_peers", {})
    assert json.loads(result.content[0].text) == {"ok": True, "seq": 17}
    assert seen == [("GET", "/api/agent/peers", {})]


async def test_chat_tool_schemas_are_linked_and_await_events_is_absent(tmp_path):
    state = _state(tmp_path)
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    tools = {tool.name: tool for tool in await mcp.list_tools()}
    assert {"list_peers", "claim_message", "send_message", "request_peer"} <= tools.keys()
    assert "await_events" not in tools
    assert tools["send_message"].input_schema["required"] == [
        "text", "room_id", "idempotency_key"]
    assert "reply_to_seq" not in tools["send_message"].input_schema["required"]
    assert tools["request_peer"].input_schema["required"] == [
        "agent_ids", "text", "idempotency_key"]


async def test_write_tool_edge_client_error_returns_is_error_json(tmp_path):
    """A write reaching RemoteApprovalQueue.enqueue while the platform is
    down/429/404 raises EdgeClientError from PlatformClient._check. That must
    be caught by _guarded's contract like every other handled exception, not
    escape past call_connector."""
    from nakagai_edge.edge.client import EdgeClientError

    state = _state(tmp_path)

    class BoomHub:
        account_key = "ag1"

        def spec(self, connector_id):
            assert connector_id == "demo"
            return ConnectorSpec(id="demo", kind="mcp-http", role="broker")

        async def call(self, connector_id, tool, args, **kw):
            raise EdgeClientError("platform rejected the agent token. Was it revoked?")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    boom_hub = BoomHub()
    mcp = create_edge_mcp(state, boom_hub, client, audit, _Reporter(),
                          Brake(state, boom_hub, client, audit))
    result = await mcp.call_tool("call_connector",
                                 {"connector_id": "demo", "tool": "place_order",
                                  "args_json": "{}"})
    text = result.content[0].text
    doc = json.loads(text)
    assert doc["is_error"] is True
    assert "revoked" in doc["error"]


DEMO_SPEC = ConnectorSpec(
    id="demo", kind="mcp-http", role="broker",
    capabilities={"get_quote": Capability(
        tool="get_quotes", args={"symbols": "symbols"}, items=["quotes"],
        fields={"symbol": ["symbol"], "price": ["price"]})})


def _supervised_position(**over) -> dict:
    # expires_at is an epoch float, the way warrant.build_warrant_payload
    # writes it, because `guarded` reads it now: an ISO string is not a clock
    # anything in the edge can compare against.
    rec = {"position_id": "ap_1", "symbol": "AAPL", "connector_id": "demo",
           "account": "123", "direction": "long", "entry_price": 100.0,
           "stop": 95.0, "entry_qty": 10.0, "confirmed_qty": 10.0,
           "state": "armed",
           "warrant": {"trigger": {"type": "price_below", "level": 95.0},
                      "expires_at": 4_102_444_800.0}}
    rec.update(over)
    return rec


async def test_get_open_risk_reports_positions_with_live_prices(tmp_path):
    """The tool must convert _quotes()'s {symbol: full quote} shape to the
    bare {symbol: price} that open_risk() still expects - the boundary
    correction this task exists to land."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())

    class QuoteHub:
        account_key = "ag1"

        def spec(self, connector_id):
            return DEMO_SPEC

        async def call(self, connector_id, tool, args, **kw):
            assert tool == "get_quotes"
            return {"data": {"quotes": [{"symbol": "AAPL", "price": 92.0}]}}

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = QuoteHub()
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert out["armed"] is True
    assert out["disarmed_positions"] == []
    row = out["positions"][0]
    assert row["position_id"] == "ap_1"
    assert row["price"] == 92.0
    assert row["guarded"] is True
    assert out["portfolio_heat"] == row["open_risk"]


# ---- the quote feed goes through each connector's own map -----------------
#
# The read the brake's whole existence depends on. Whatever tool a connector's
# `get_quote` entry names must still be classified read-only, or the edge's own
# guardrails deny it and the brake goes blind in silence; brake.tick's famine
# signal is what makes that audible.

QUOTE_SPECS = load_specs({"connectors": [ALIEN_CONNECTOR, ROBINHOOD_CONNECTOR]})

QUOTE_PAYLOADS = {
    # The alien broker wraps nothing and spells every field its own way.
    "ticker": {"ticks": [{"tkr": "aapl", "last": "92.00"}]},
    # Robinhood nests its own {"data": ..., "guide": ...} envelope, which the
    # map roots at rather than anything in runtime.py peeling it.
    "get_quotes": {
        "data": {"quotes": [{"symbol": "MSFT",
                             "last_trade_price": "310.50"}]},
        "guide": "ignore me"},
}


class MapQuoteHub:
    """Answers through the real connector maps, keyed by the tool asked for."""

    def __init__(self, payloads=None, fail=frozenset()):
        self.account_key = "ag1"
        self.calls = []
        self.payloads = QUOTE_PAYLOADS if payloads is None else payloads
        self.fail = fail

    def spec(self, connector_id):
        return QUOTE_SPECS[connector_id]

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, dict(args)))
        if connector_id in self.fail:
            raise RuntimeError(f"{connector_id} is not answering")
        return {"data": self.payloads[tool]}


def _two_brokers(tmp_path):
    """One supervised position on each connector, distinct symbols."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position(connector_id="alien-broker",
                                       symbol="AAPL", account="AL-1"))
    record(state, _supervised_position(position_id="ap_2",
                                       connector_id="robinhood-trading",
                                       symbol="MSFT", account="463605220"))
    return state


async def test_quotes_dial_each_connectors_own_quote_tool(tmp_path):
    """`ticker` with `tickers` for the alien broker, `get_quotes` with
    `symbols` for Robinhood, and the same canonical quote shape out of both.

    The tool name and the argument key both come from the map, and both are
    asserted: a broker handed the right tool under the wrong argument key
    answers about nothing at all, and the brake then goes quiet in exactly the
    way a working brake looks from outside.
    """
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub()

    quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    by_tool = {tool: args for _, tool, args in hub.calls}
    assert by_tool == {"ticker": {"tickers": ["AAPL"]},
                       "get_quotes": {"symbols": ["MSFT"]}}
    assert set(quotes) == {"AAPL", "MSFT"}
    assert quotes["AAPL"]["price"] == 92.00
    assert quotes["MSFT"]["price"] == 310.50
    for quote in quotes.values():
        # The full normalized shape, not a bare price: usable() needs the
        # receipt time and the book to judge freshness and spread.
        assert set(quote) == {"price", "bid", "ask", "ts"}
        assert quote["ts"] > 0


async def test_a_connector_that_will_not_answer_costs_only_its_own_symbols(
        tmp_path):
    """No quote means no tick for that symbol, which means no fire. That is
    the safe direction but not a quiet one: the missing symbol is what
    brake.tick counts toward its famine signal. The other connector's
    positions must still be priced."""
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub(fail={"alien-broker"})

    quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    assert set(quotes) == {"MSFT"}, "the sweep survives one dead connector"


async def test_a_connector_declaring_no_get_quote_is_skipped_with_a_reason(
        tmp_path, caplog):
    """A connector with no `get_quote` entry cannot be asked to guess one. It
    is skipped by name in the log, not dialed on a hopeful tool name, and it
    does not take the other connectors' quotes down with it."""
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub()
    hub.spec = lambda cid: (
        ConnectorSpec(id=cid, kind="mcp-http", role="broker")
        if cid == "alien-broker" else QUOTE_SPECS[cid])

    with caplog.at_level(logging.WARNING, logger="nakagai.edge"):
        quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    assert set(quotes) == {"MSFT"}
    assert [c[0] for c in hub.calls] == ["robinhood-trading"], (
        "an unmapped connector must not be dialed on a guessed tool name")
    assert "alien-broker" in caplog.text and "get_quote" in caplog.text


async def test_a_quote_payload_the_map_cannot_read_is_famine_not_an_empty_book(
        tmp_path, caplog):
    """An answer this connector's map cannot read must reach the brake as
    silence, not as a quote list that happens to be empty.

    `extract` returns None rather than [] for a payload that holds no list
    where the map says one lives. Iterating that would raise inside the sweep
    and take the OTHER connector's quotes down with it; coercing it back to []
    would be worse, because a broker whose shape changed under a stale map
    would look exactly like a market with nothing to say and the famine signal
    would never fire.
    """
    import nakagai_edge.edge.runtime as runtime
    state = _two_brokers(tmp_path)
    hub = MapQuoteHub(payloads={**QUOTE_PAYLOADS,
                                "ticker": {"ticks": {"tkr": "aapl"}}})

    with caplog.at_level(logging.WARNING, logger="nakagai.edge"):
        quotes = await runtime._quotes(hub, state, ["AAPL", "MSFT"])

    assert set(quotes) == {"MSFT"}, "one unreadable answer, one live connector"
    assert "alien-broker" in caplog.text


async def test_get_open_risk_keeps_terminal_records_out_of_the_heat(tmp_path):
    """portfolio_heat answers "what if every stop hit at once", so a position
    that already closed must not sit in it forever. reconcile() skips TERMINAL,
    so nothing downstream ever zeroes such a record. The row itself stays
    visible: only the figure changes."""
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    record(state, _supervised_position(position_id="ap_2",
                                       state="outcome_unknown"))

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert {r["position_id"] for r in out["positions"]} == {"ap_1", "ap_2"}
    live = next(r for r in out["positions"] if r["position_id"] == "ap_1")
    assert live["open_risk"] > 0
    assert out["portfolio_heat"] == round(live["open_risk"], 2)


async def test_get_open_risk_says_so_when_the_ledger_was_lost(tmp_path):
    """An empty positions list reads as "you have no open risk". After a
    corrupt ledger it means "the brake forgot every position it was watching",
    which is the opposite, so the tool has to say which one it is."""
    state = _state(tmp_path)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text("{not json")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert out["positions"] == []
    assert "supervised.corrupt.json" in out["ledger_fault"]


async def test_get_open_risk_survives_a_dead_quote_feed(tmp_path, monkeypatch):
    """A quote feed that fails outright (not just one connector inside it)
    must not hide the book: an agent needs to see its own open risk most
    urgently when things are degraded."""
    import nakagai_edge.edge.runtime as runtime
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())

    async def boom(hub, state, symbols):
        raise RuntimeError("no route to broker")

    monkeypatch.setattr(runtime, "_quotes", boom)

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert out["positions"][0]["position_id"] == "ap_1"
    assert out["positions"][0]["price"] is None


async def test_get_open_risk_reports_unguarded_under_a_global_disarm(tmp_path):
    """Fix round 1: guarded was computed from the ledger record alone (warrant
    + state == armed) and never consulted brake.armed()/disarmed_positions(),
    the two mechanisms `brake off` actually drives. After a global disarm
    nothing fires on a confirmed breach, so a still-True `guarded` here would
    tell the owner the opposite of the truth."""
    from nakagai_edge.edge.brake import set_local_disarm
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    set_local_disarm(state, all_positions=True)

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert out["armed"] is False
    assert out["positions"][0]["guarded"] is False


async def test_get_open_risk_is_self_consistent_after_a_per_position_disarm(tmp_path):
    """A position released with `brake off --position` must never show up
    both in disarmed_positions AND as guarded: true in the same payload -
    that combination is the exact self-contradiction the fix round closes."""
    from nakagai_edge.edge.brake import set_local_disarm
    from nakagai_edge.edge.supervision import record
    state = _state(tmp_path)
    record(state, _supervised_position())
    set_local_disarm(state, position_id="ap_1")

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    audit = EdgeAudit(state)
    hub = build_hub(state, client)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    result = await mcp.call_tool("get_open_risk", {})
    text = result.content[0].text
    out = json.loads(text)

    assert out["disarmed_positions"] == ["ap_1"]
    guarded_ids = {r["position_id"] for r in out["positions"] if r["guarded"]}
    assert not (guarded_ids & set(out["disarmed_positions"]))
    assert out["positions"][0]["guarded"] is False


# ---- the tool schemas the platform cannot fetch for itself ----------------
#
# The platform never dials a broker, so a connector's tool list and JSON
# schemas reach it only if this edge ships them. They ride the connector
# report, not the agent-facing status tools: `list_connectors` is read by an
# agent that pays tokens for every byte, and `list_connector_tools` already
# serves the schemas per connector, on demand.

SPEC = {"id": "demo", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp/", "enabled": True}

LISTED_TOOL = {
    "name": "get_account",
    "title": "Get account",
    "description": "Balances for one account",
    "inputSchema": {"type": "object",
                    "properties": {"account": {"type": "string"}}},
    "outputSchema": {"type": "object"},
    "annotations": {"readOnlyHint": True},
    "_meta": {"noise": True},
}


def test_to_dict_carries_each_tools_name_description_and_schema():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[LISTED_TOOL]).to_dict(with_tools=True)
    assert row["tool_count"] == 1
    # Exactly three keys: outputSchema, annotations and _meta are the
    # downstream's business, and shipping them would put bytes on the wire
    # every cycle that nothing upstream reads.
    assert row["tools"] == [{
        "name": "get_account",
        "description": "Balances for one account",
        "inputSchema": {"type": "object",
                        "properties": {"account": {"type": "string"}}}}]


def test_to_dict_leaves_the_schemas_out_unless_they_are_asked_for():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[LISTED_TOOL]).to_dict()
    assert "tools" not in row and row["tool_count"] == 1


def test_to_dict_of_a_connector_that_never_connected_still_has_a_tool_list():
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC)).to_dict(with_tools=True)
    assert row["status"] == "disconnected"
    assert row["tools"] == [] and row["tool_count"] == 0


def test_a_tool_that_publishes_no_description_or_schema_still_reports_both_keys():
    # A predictable payload: the platform reads three keys off every entry
    # rather than three keys off some of them.
    from nakagai_edge.hub import Connection
    row = Connection(spec=ConnectorSpec(**SPEC), status="connected",
                     tools=[{"name": "ping"}]).to_dict(with_tools=True)
    assert row["tools"] == [{"name": "ping", "description": "", "inputSchema": {}}]


LIVE_BUNDLE = {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
               "connectors": {"connectors": [ROBINHOOD_CONNECTOR]},
               "signing_public_key": "k"}


def _live_connector(tmp_path, handler=None):
    """A hub whose robinhood connector is CONNECTED and has published a schema.

    Nothing is dialed: the Connection is placed by hand in exactly the state a
    successful `_run_connection` leaves behind (status connected, `tools`
    snapshotted from the downstream's list_tools). Without a live one, every
    assertion about the schemas is vacuous: an unconnected connector carries an
    empty tool list, so `inputSchema` is absent from every payload whatever the
    flags say.

    The bundle is synced through `sync_once` rather than `apply_bundle` because
    that is what stamps `fetched_at`, and without a fresh stamp `list_connectors`
    answers the stale-policy refusal instead of a connector list, which is
    another way for these assertions to pass while proving nothing.
    """
    from nakagai_edge.hub import Connection

    def _bundle_only(req):
        if req.url.path == "/api/agent/bundle":
            return httpx.Response(200, json=LIVE_BUNDLE, headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler or _bundle_only))
    assert sync_once(state, client) is True
    hub = build_hub(state, client)
    hub._conns["robinhood-trading"] = Connection(
        spec=hub.spec("robinhood-trading"), status="connected",
        tools=[LISTED_TOOL])
    return state, client, hub


async def test_a_live_connectors_schemas_reach_the_platform_and_not_the_agent(
        tmp_path):
    """Both halves of the split, asserted off one live connector.

    The platform can NEVER read a broker's schemas for itself, so the syncer's
    call has to carry them. `list_connectors` answers "which broker can be
    asked what" to an agent that pays tokens for every byte, and
    `list_connector_tools` already serves the schemas per connector on demand,
    so the same call must not carry them there.
    """
    state, client, hub = _live_connector(tmp_path)
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))

    reported = hub.status(with_tools=True)["connectors"]
    row = next(c for c in reported if c["id"] == "robinhood-trading")
    assert row["status"] == "connected"
    assert row["tools"] == [{"name": "get_account",
                             "description": "Balances for one account",
                             "inputSchema": LISTED_TOOL["inputSchema"]}]

    for tool in ("list_connectors", "get_connector_status"):
        result = await mcp.call_tool(tool, {})
        text = result.content[0].text
        # The connector itself must be in the answer, or this proves nothing:
        # a stale-policy refusal carries no schemas either.
        assert "robinhood-trading" in text, f"{tool} never answered"
        assert "inputSchema" not in text, f"{tool} shipped downstream schemas"
        assert "get_account" not in text, f"{tool} shipped downstream tool names"


async def test_the_syncer_ships_a_live_connectors_schemas_upstream(
        tmp_path, monkeypatch):
    """The one path that carries a broker's schemas off this machine.

    Everything else in this file proves the hub CAN produce them. This proves
    the loop actually asks: the platform has no other source, so a syncer that
    stopped passing `with_tools` would leave the capability-map derivation with
    nothing to read, and nothing anywhere would go red.
    """
    import nakagai_edge.edge.runtime as runtime
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    seen = []

    def handler(req):
        if req.url.path == "/api/agent/connectors":
            seen.append(json.loads(req.content)["connectors"])
            return httpx.Response(200, json={"ok": True, "connectors": 1})
        if req.url.path == "/api/agent/bundle":
            return httpx.Response(200, json=LIVE_BUNDLE, headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    state, client, hub = _live_connector(tmp_path, handler)
    audit = EdgeAudit(state)
    tasks = await runtime._loops(state, hub, client, audit, _Reporter(),
                                 Brake(state, hub, client, audit), _NoFills())
    for _ in range(200):
        if seen:
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t

    assert seen, "the syncer never reported connector status"
    row = next(c for c in seen[0] if c["id"] == "robinhood-trading")
    assert row["tools"] == [{"name": "get_account",
                             "description": "Balances for one account",
                             "inputSchema": LISTED_TOOL["inputSchema"]}]
    await hub.aclose()


def test_status_carries_the_flag_to_every_row_it_builds(tmp_path):
    """`with_tools` is a property of the call, not of one branch inside it: a
    registered connector with no live Connection object gets the same treatment
    as one that has it. The client decides what to SAY about a connector that
    is not connected; the hub reports uniformly."""
    _, _, hub = _live_connector(tmp_path)
    hub._conns.clear()      # registered, never dialed: the placeholder branch
    rows = hub.status(with_tools=True)["connectors"]
    assert [r["status"] for r in rows] == ["disconnected"]
    assert rows[0]["tools"] == []


async def test_the_syncer_survives_a_report_that_raises(tmp_path, monkeypatch, caplog):
    """The connector report is best-effort and the syncer runs forever: one
    uncaught exception out of it would kill every later sync too, not just
    this cycle's report. The schema guard runs inside that call, so it must
    not become a new way for the loop to die."""
    import nakagai_edge.edge.runtime as runtime
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    calls = {"bundle": 0, "report": 0}

    def handler(req):
        if req.url.path == "/api/agent/bundle":
            calls["bundle"] += 1
            return httpx.Response(200, json={"bundle_version": "v1",
                                             "schema_version": BUNDLE_SCHEMA,
                                             "connectors": {"connectors": []},
                                             "signing_public_key": "k"},
                                  headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))

    def boom(connectors):
        calls["report"] += 1
        raise RuntimeError("the schema guard blew up")

    client.report_connectors = boom
    hub = build_hub(state, client)
    audit = EdgeAudit(state)

    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    tasks = await runtime._loops(state, hub, client, audit, _Reporter(),
                                 Brake(state, hub, client, audit), _NoFills())
    for _ in range(200):
        if calls["report"] >= 2:
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t

    assert calls["report"] >= 2, "the loop died on the first raising report"
    assert calls["bundle"] >= 2, "sync_once stopped running too, not just the report"
    assert any("connector" in r.message.lower() for r in caplog.records)
    await hub.aclose()
