"""The platform's tools, under names an agent can find.

The invariant under test is that promotion changed which NAMES exist, not how a
call travels. If a promoted tool can reach the platform by a route
`call_connector` cannot, this feature widened authority while looking cosmetic.

Nothing here stubs the guardrails. The hub is a REAL ConnectorHub over a real
in-memory MCP server standing in for the platform, so classification, the
allow_writes gate and the audit journal are the production ones: a fixture that
faked `hub.call` could not tell a verdict apart from a verdict it invented.
"""

import contextlib
import json
import time
from dataclasses import dataclass

import httpx
import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, ConfigDict, Field

pytest.importorskip("mcp")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.remote import RemoteApprovalQueue
from nakagai_edge.edge.runtime import create_edge_mcp
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle
from nakagai_edge.hub import ConnectorError, ConnectorHub
from tests.fixtures import platform_mcp

pytestmark = pytest.mark.anyio

PLATFORM = "nakagai-mcp"

# Names the edge already serves itself. Local always wins: never
# prefixed, never both. Verified against nakagai_platform/mcp_server.py.
COLLIDE = {"agent_checkin", "call_connector", "get_approval",
           "get_connector_status", "list_connector_tools", "list_peers",
           "claim_message", "send_message", "request_peer",
           "accept_candidate", "abstain_candidate"}

# The shipped registry entry, guardrails included: this is what
# config/connectors.yaml in the platform repo actually sends down the bundle,
# and the whole point of the verdict tests is that the shipped policy is what
# decides, not something this file arranged.
SHIPPED_GUARDRAILS = {"read_only_tools": ["get_*", "list_*", "validate_rule_spec"]}

PLATFORM_ENTRY = {
    "id": PLATFORM,
    "name": "Nakagai MCP (this platform)",
    "kind": "mcp-http",
    "role": "signals",
    "url": "https://api.test/mcp/",
    "enabled": True,
    "guardrails": SHIPPED_GUARDRAILS,
}


class _FeePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    commission_bps: float = Field(ge=0)
    slippage_bps: float = Field(ge=0)


class _ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    initial_equity: float = Field(gt=0)
    fees: _FeePolicy


_nested_platform = MCPServer("nested-platform-fake")
_nested_calls: list[dict] = []


@_nested_platform.tool()
def research_with_policy(execution: _ExecutionPolicy, note: str = "daily") -> str:
    """Exercise a generated root $defs schema through real MCP dispatch."""
    args = {"execution": execution.model_dump(), "note": note}
    _nested_calls.append(args)
    return json.dumps(args)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Reporter:
    """The portfolio path is not what these tests are about."""

    async def snapshot_and_push(self):
        return {"connectors": []}


def _state(tmp_path, guardrails):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": [
                             {**PLATFORM_ENTRY, "guardrails": guardrails}]},
                         "signing_public_key": "k"}, "v1")
    return state


def _dead_platform():
    """The HTTP side of the platform, which these tests never need."""
    return PlatformClient("https://api.test", "t",
                          transport=httpx.MockTransport(lambda r: httpx.Response(500)))


@dataclass
class _Edge:
    mcp: object
    hub: object
    audit: EdgeAudit
    down: dict

    async def call(self, name: str, /, **args) -> dict:
        # Positional-only: `call_connector` takes an argument called `tool`,
        # and this helper must be able to pass it through.
        result = await self.mcp.call_tool(name, args)
        return json.loads(result.content[0].text)

    async def names(self) -> list[str]:
        return [t.name for t in await self.mcp.list_tools()]

    def go_dark(self) -> None:
        """The platform stops answering, after the tools were promoted."""
        self.down["now"] = True


@contextlib.asynccontextmanager
async def _promoted_edge(tmp_path, *, guardrails=SHIPPED_GUARDRAILS,
                         upstream=platform_mcp.mcp):
    """A real edge server with the platform's tools already promoted."""
    from tests.fixtures.inproc import connected_session

    state = _state(tmp_path, guardrails)
    client = _dead_platform()
    down = {"now": False}

    async def connect(spec):
        if down["now"]:
            raise ConnectorError("no route to the platform")
        return connected_session(upstream)

    hub = ConnectorHub(state.root, connect=connect,
                       approvals=RemoteApprovalQueue(client, state, "ag1"))
    hub.account_key = "ag1"
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(state, hub, client, audit, _Reporter(),
                          Brake(state, hub, client, audit))
    platform_mcp.calls.clear()
    try:
        await mcp.promote_platform_tools()
        yield _Edge(mcp=mcp, hub=hub, audit=audit, down=down)
    finally:
        await hub.aclose()


@pytest.fixture
async def edge(tmp_path):
    async with _promoted_edge(tmp_path) as e:
        yield e


async def test_platform_tools_are_promoted(edge):
    names = set(await edge.names())
    assert {"get_signals", "get_runs", "run_backtest"} <= names
    assert "await_events" not in names
    # Sixteen: the platform's twenty-eight minus the eleven local collisions and
    # the event reader. A number, so that a promotion that quietly dropped
    # half of them is a failure rather than a smaller success.
    assert len(names) == 20 + 16, sorted(names)


async def test_a_promoted_tool_publishes_the_platforms_own_argument_schema(edge):
    """The trap this closes: a forwarder declared `**kwargs` registers cleanly
    and publishes ONE required string argument called "kwargs". Every call then
    fails validation, and nothing about the registration says so. The signature
    is built from the platform's own inputSchema instead, which is also why the
    agent reads the platform's types rather than a restatement of them."""
    spec = next(t for t in await edge.mcp.list_tools() if t.name == "get_signals")
    props = spec.input_schema["properties"]
    assert "kwargs" not in props
    assert props["include_suppressed"]["type"] == "boolean"
    assert props["since"]["type"] == "string"
    assert not spec.input_schema.get("required")
    # A required argument stays required, so the agent is told before the call.
    write = next(t for t in await edge.mcp.list_tools()
                 if t.name == "set_autoexecute_allowlist")
    assert write.input_schema["required"] == ["reason"]


async def test_nested_generated_schema_is_self_contained_and_forwards_objects(tmp_path):
    _nested_calls.clear()
    execution = {
        "initial_equity": 25_000.0,
        "fees": {"commission_bps": 1.5, "slippage_bps": 2.0},
    }

    guardrails = {**SHIPPED_GUARDRAILS, "allow_writes": True}
    async with _promoted_edge(
            tmp_path, upstream=_nested_platform, guardrails=guardrails) as edge:
        spec = next(
            tool for tool in await edge.mcp.list_tools()
            if tool.name == "research_with_policy"
        )
        assert "$defs" not in spec.input_schema
        assert spec.input_schema["properties"]["execution"] == {
            "additionalProperties": False,
            "properties": {
                "initial_equity": {
                    "exclusiveMinimum": 0,
                    "title": "Initial Equity",
                    "type": "number",
                },
                "fees": {
                    "additionalProperties": False,
                    "properties": {
                        "commission_bps": {
                            "minimum": 0,
                            "title": "Commission Bps",
                            "type": "number",
                        },
                        "slippage_bps": {
                            "minimum": 0,
                            "title": "Slippage Bps",
                            "type": "number",
                        },
                    },
                    "required": ["commission_bps", "slippage_bps"],
                    "title": "_FeePolicy",
                    "type": "object",
                },
            },
            "required": ["initial_equity", "fees"],
            "title": "_ExecutionPolicy",
            "type": "object",
        }

        doc = await edge.call("research_with_policy", execution=execution)

    assert doc["is_error"] is False
    assert doc["data"] == {"execution": execution, "note": "daily"}
    assert _nested_calls == [{"execution": execution, "note": "daily"}]


async def test_unsupported_root_schema_skips_one_tool_and_promotes_the_next(
        tmp_path, caplog):
    class _SchemaHub:
        account_key = "ag1"

        async def list_tools(self, connector_id):
            assert connector_id == PLATFORM
            return {"tools": [
                {
                    "name": "root_composed",
                    "description": "Cannot be represented by a Python signature.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                        "allOf": [{"required": ["value"]}],
                    },
                },
                {
                    "name": "plain_after_bad",
                    "description": "Must still be promoted.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                    },
                },
                {
                    "name": "boolean_property_schema",
                    "description": "Cannot become WithJsonSchema metadata.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"value": True},
                    },
                },
            ]}

    state = _state(tmp_path, SHIPPED_GUARDRAILS)
    client = _dead_platform()
    hub = _SchemaHub()
    audit = EdgeAudit(state)
    mcp = create_edge_mcp(
        state, hub, client, audit, _Reporter(), Brake(state, hub, client, audit)
    )

    with caplog.at_level("WARNING", logger="nakagai.edge"):
        await mcp.promote_platform_tools()

    names = {tool.name for tool in await mcp.list_tools()}
    assert "root_composed" not in names
    assert "plain_after_bad" in names
    assert "boolean_property_schema" not in names
    assert "root_composed" in caplog.text
    assert "boolean_property_schema" in caplog.text
    assert "call_connector" in caplog.text


async def test_collisions_resolve_to_the_local_tool(edge):
    names = await edge.names()
    for name in COLLIDE:
        assert names.count(name) == 1, f"{name} is defined twice"
    assert not [n for n in names
                if n.startswith("platform_") or n.startswith("nakagai_")]
    await edge.call("list_peers")
    assert platform_mcp.calls == [], "list_peers went upstream; the local one must win"


@pytest.mark.parametrize("name, args", [
    ("await_events", {}),
    ("list_peers", {}),
    ("claim_message", {"message_seq": 41}),
    ("send_message", {"text": "answer", "room_id": "desk", "idempotency_key": "reply-1"}),
    ("request_peer", {"agent_ids": ["a2"], "text": "check", "idempotency_key": "ask-1"}),
], ids=["event-reader", "peers", "claim", "message", "request"])
async def test_reserved_chat_tools_never_reach_the_platform_connector(edge, name, args):
    doc = await edge.call("call_connector", connector_id=PLATFORM, tool=name,
                          args_json=json.dumps(args))

    assert doc["is_error"] is True
    assert "reserved" in doc["error"]
    assert platform_mcp.calls == []


async def test_a_promoted_call_reaches_the_platform_with_what_was_asked(edge):
    doc = await edge.call("get_signals", since="2026-08-01")
    assert doc["connector"] == PLATFORM and doc["tool"] == "get_signals"
    # Only what the agent supplied. An omitted optional is not sent, because
    # `call_connector` would not have sent it either.
    assert platform_mcp.calls == [
        ("get_signals", {"include_suppressed": False, "since": "2026-08-01"})]


async def test_a_promoted_call_goes_through_the_guardrail_door(edge):
    """`_guarded` is the only place a connector call is journalled. No audit
    record means the call reached `hub.call` some other way, which is the one
    thing this feature must never do."""
    before = len(edge.audit.pending())
    await edge.call("get_signals")
    events = edge.audit.pending()[before:]
    assert [e["kind"] for e in events] == ["call"]
    assert events[0]["connector_id"] == PLATFORM
    assert events[0]["tool"] == "get_signals"
    # The name it came in under is recorded and nothing else: `_guarded`
    # documents that `capability` never reaches hub.call and never moves a
    # verdict.
    assert events[0]["detail"]["capability"] == "promoted:get_signals"


async def test_stale_policy_denies_a_promoted_tool(edge, monkeypatch):
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past the 900s TTL
    doc = await edge.call("get_signals")
    assert "policy stale" in doc["error"]
    assert platform_mcp.calls == []


async def test_platform_down_keeps_the_tool_listed(edge):
    """Losing seventeen tools silently is the failure that started all of this.
    A legible error beats a vanishing surface."""
    edge.go_dark()
    await edge.hub.invalidate(PLATFORM)

    assert "get_signals" in await edge.names()
    doc = await edge.call("get_signals")
    assert doc["is_error"] is True
    assert "platform" in doc["error"].lower()


# ---- the entry point changed, the verdict did not -------------------------
#
# Asserted on a WRITE, because a read passing through both doors proves much
# less: reads are what the shipped guardrails allow, so they would agree even
# if promotion had skipped classification entirely. Both regimes are run: the
# shipped one, where `set_autoexecute_allowlist` is refused because the
# platform connector is read-only, and one with allow_writes on, where the call
# actually lands. A promotion that widened authority passes neither.


@pytest.mark.parametrize("guardrails, lands", [
    (SHIPPED_GUARDRAILS, False),
    ({**SHIPPED_GUARDRAILS, "allow_writes": True}, True),
], ids=["shipped-read-only", "writes-allowed"])
async def test_a_write_earns_the_same_verdict_through_both_doors(
        tmp_path, guardrails, lands):
    args = {"reason": "the owner asked", "add": ["AAPL"]}

    async with _promoted_edge(tmp_path, guardrails=guardrails) as edge:
        before = len(edge.audit.pending())
        promoted = await edge.call("set_autoexecute_allowlist", **args)
        promoted_arrived = list(platform_mcp.calls)
        promoted_events = edge.audit.pending()[before:]

        platform_mcp.calls.clear()
        before = len(edge.audit.pending())
        raw = await edge.call("call_connector", connector_id=PLATFORM,
                              tool="set_autoexecute_allowlist",
                              args_json=json.dumps(args))
        raw_arrived = list(platform_mcp.calls)
        raw_events = edge.audit.pending()[before:]

    assert promoted == raw, "the entry point moved the verdict"
    assert promoted_arrived == raw_arrived, "the two doors sent different bytes"
    assert ([e["kind"] for e in promoted_events]
            == [e["kind"] for e in raw_events]), "one door was journalled differently"

    # And the verdict is the one the shipped policy asks for, so neither
    # half above can agree by both being wrong.
    if lands:
        assert promoted["is_write"] is True
        assert promoted_arrived == [("set_autoexecute_allowlist", {
            "reason": "the owner asked", "add": ["AAPL"], "drop": None})]
        assert [e["kind"] for e in promoted_events] == ["call"]
    else:
        assert promoted["is_error"] is True
        assert "read-only" in promoted["error"]
        assert promoted_arrived == [], "a denied write reached the platform"
        assert [e["kind"] for e in promoted_events] == ["denial"]
