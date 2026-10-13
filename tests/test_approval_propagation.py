from __future__ import annotations

import ast
import inspect
import time
from pathlib import Path

import pytest
import yaml

from nakagai_edge.edge.remote import RemoteApprovalQueue, intents
from nakagai_edge.edge.runtime import build_hub
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.hub import ConnectorHub
from nakagai_edge.hub import GuardrailDenied
from tests.fixtures.echo_mcp import mcp as echo_server
from tests.fixtures.inproc import connect_to


class RecordingQueue:
    def __init__(self) -> None:
        self.calls = []

    def enqueue(self, account_key, connector_id, tool, args, **kwargs):
        self.calls.append((account_key, connector_id, tool, args, kwargs))
        return type("Record", (), {
            "id": "approval-a", "status": "pending", "expires_at": time.time() + 60,
        })()


def configured_hub(tmp_path, queue) -> ConnectorHub:
    config = tmp_path / "config"
    config.mkdir()
    (config / "connectors.yaml").write_text(yaml.safe_dump({"connectors": [{
        "id": "echo", "name": "Echo", "kind": "mcp-http", "role": "broker",
        "url": "https://echo.test/mcp", "enabled": True,
        "guardrails": {
            "read_only_tools": ["echo"], "allow_writes": True,
            "approvals": {"require_for": ["place_*"]},
            # A broker takes writes only for a confirmed account; these calls
            # name none, so the account key requirement is off here.
            "accounts": {"allow": ["1"], "require_account_arg": False},
        },
    }]}))
    return ConnectorHub(tmp_path, connect=connect_to(echo_server), approvals=queue)


@pytest.mark.anyio
async def test_connector_hub_forwards_required_account_key(tmp_path) -> None:
    queue = RecordingQueue()
    hub = configured_hub(tmp_path, queue)

    signature = inspect.signature(ConnectorHub.call)
    assert signature.parameters["account_key"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["account_key"].default is inspect.Parameter.empty
    assert "workspace" not in signature.parameters

    result = await hub.call(
        "echo", "place_equity_order", {"symbol": "SPY"}, account_key="agent-a"
    )
    assert result["approval_id"] == "approval-a"
    # Only the owner's tap decides: the hub never answers for them, so a
    # guarded write returns exactly the pending record and nothing else.
    assert set(result) == {"connector", "tool", "is_write", "approval_required",
                           "approval_id", "status", "expires_at", "message"}
    assert queue.calls[0][0] == "agent-a"
    await hub.aclose()


@pytest.mark.anyio
async def test_read_only_hub_call_requires_identity_without_touching_queue(tmp_path) -> None:
    queue = RecordingQueue()
    hub = configured_hub(tmp_path, queue)

    result = await hub.call("echo", "echo", {"text": "hi"}, account_key="agent-a")

    assert result["is_write"] is False
    assert queue.calls == []
    await hub.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("args", [
    {"symbol": "SPY", "side": "buy"},
    {"symbol": "SPY", "side": "sell", "reduce_only": True},
], ids=["signed-proposal-executor", "signed-brake-exit"])
async def test_verified_internal_writes_skip_the_queue(tmp_path, args) -> None:
    queue = RecordingQueue()
    hub = configured_hub(tmp_path, queue)
    try:
        result = await hub.call(
            "echo", "place_equity_order", args,
            account_key="agent-a", approved=True,
        )
    finally:
        await hub.aclose()

    assert result["is_write"] is True
    assert queue.calls == []


class RemoteClient:
    def __init__(self) -> None:
        self.gets = []

    def get_approval(self, approval_id):
        self.gets.append(approval_id)
        return {
            "id": approval_id, "connector_id": "broker", "tool": "place_order",
            "args": {}, "status": "pending",
        }


@pytest.mark.anyio
async def test_hub_write_sent_for_approval_is_refused_toward_order_proposals(
        tmp_path) -> None:
    state = EdgeState(tmp_path)
    hub = configured_hub(tmp_path, RemoteApprovalQueue(RemoteClient(), state, "agent-a"))
    try:
        with pytest.raises(GuardrailDenied, match="propose_order"):
            await hub.call(
                "echo", "place_equity_order", {"symbol": "SPY"},
                account_key="agent-a",
            )
    finally:
        await hub.aclose()

    assert intents(state) == {}


def test_remote_queue_reads_by_local_identity_without_authorizing_hosted_storage(
        tmp_path) -> None:
    client = RemoteClient()
    queue = RemoteApprovalQueue(client, EdgeState(tmp_path), "agent-a")

    fetched = queue.get("agent-a", "approval-a")
    assert fetched is not None and fetched.account_key == "agent-a"
    assert client.gets == ["approval-a"]
    with pytest.raises(ValueError, match="account_key"):
        queue.get("", "approval-a")
    assert client.gets == ["approval-a"]


def test_runtime_uses_paired_agent_id_and_refuses_a_missing_identity(tmp_path) -> None:
    client = RemoteClient()
    paired = EdgeState(tmp_path / "paired")
    paired.save_agent("https://api.test", "agent-a", "nk_agent_t")
    hub = build_hub(paired, client)
    assert hub.account_key == "agent-a"

    incomplete = EdgeState(tmp_path / "incomplete")
    incomplete.save_agent("https://api.test", "", "nk_agent_t")
    with pytest.raises(SystemExit, match="agent_id"):
        build_hub(incomplete, client)


def test_runtime_supplies_nonempty_account_identity() -> None:
    root = Path(__file__).resolve().parents[1] / "nakagai_edge"
    for relative in ["edge/runtime.py", "edge/executor.py", "edge/brake.py", "edge/portfolio.py", "edge/fills.py"]:
        tree = ast.parse((root / relative).read_text())
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if isinstance(call.func, ast.Attribute) and call.func.attr in {"call", "get"}:
                if call.func.attr == "call" and not (
                    isinstance(call.func.value, ast.Name) and call.func.value.id == "hub"
                ) and not (
                    isinstance(call.func.value, ast.Attribute) and call.func.value.attr == "hub"
                ):
                    continue
                if call.func.attr == "get" and not (
                    isinstance(call.func.value, ast.Attribute) and call.func.value.attr == "approvals"
                ):
                    continue
                if call.func.attr == "call":
                    assert any(keyword.arg == "account_key" for keyword in call.keywords), (
                        relative, call.lineno
                    )
                else:
                    assert call.args and ast.unparse(call.args[0]) == "hub.account_key", (
                        relative, call.lineno
                    )
