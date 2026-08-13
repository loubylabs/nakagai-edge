from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from nakagai_edge.approvals import (
    APPROVED,
    DENIED,
    ERROR,
    EXECUTED,
    PENDING,
    ApprovalError,
    ApprovalQueue,
)


def enqueue(queue: ApprovalQueue, account_key: str, *, agent_id: str = ""):
    return queue.enqueue(
        account_key,
        "broker",
        "place_order",
        {"symbol": "SPY", "quantity": 1},
        ttl_s=60,
        requested_by="same requester",
        agent_id=agent_id,
        signal={"symbol": "SPY"},
        notional=100.0,
    )


def snapshot(queue: ApprovalQueue, account_key: str, approval_id: str) -> dict:
    record = queue.get(account_key, approval_id)
    assert record is not None
    return record.to_dict()


def test_file_get_and_list_never_return_a_foreign_known_id(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    a = enqueue(queue, "account-a")
    b = enqueue(queue, "account-b")

    assert queue.get("account-a", b.id) is None
    assert [row["id"] for row in queue.list("account-a")] == [a.id]
    assert [row["id"] for row in queue.list("account-b")] == [b.id]


def test_file_unclaimable_lookup_requires_nonempty_account_key(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    record = enqueue(queue, "account-a")

    with pytest.raises(ValueError, match="account_key"):
        queue._why_unclaimable("", record.id)


def test_file_rationale_and_symbol_history_are_account_scoped(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    a = enqueue(queue, "account-a")
    b = enqueue(queue, "account-b")
    before = snapshot(queue, "account-b", b.id)

    assert queue.set_rationale("account-a", b.id, {"summary": "foreign"}) is False
    assert snapshot(queue, "account-b", b.id) == before
    assert queue.set_rationale("account-a", a.id, {"summary": "own"}) is True
    assert queue.recent_for_symbol("account-a", "SPY") == [
        {
            "status": PENDING,
            "decided_by": "",
            "created_at": a.created_at,
            "notional": 100.0,
        }
    ]


@pytest.mark.parametrize("transition", ["deny", "claim", "finish", "resolve", "grant"])
def test_file_transitions_leave_foreign_record_byte_equivalent(tmp_path, transition: str) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    b = enqueue(queue, "account-b")
    if transition in {"finish", "resolve", "grant"}:
        queue._claim("account-b", b.id, "owner", "checked")
    if transition == "resolve":
        queue._finish("account-b", b, ERROR, error="timeout", outcome_unknown=True)
    before = snapshot(queue, "account-b", b.id)

    with pytest.raises(ApprovalError):
        if transition == "deny":
            queue.deny("account-a", b.id)
        elif transition == "claim":
            queue._claim("account-a", b.id, "owner", "foreign")
        elif transition == "finish":
            queue._finish("account-a", b, EXECUTED, result={"ok": True})
        elif transition == "resolve":
            queue._resolve("account-a", b, "foreign")
        else:
            queue._grant("account-a", b, {"signed": True})

    assert snapshot(queue, "account-b", b.id) == before


def test_file_public_approve_and_grant_cannot_claim_foreign_record(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    b = enqueue(queue, "account-b")
    before = snapshot(queue, "account-b", b.id)

    async def execute(connector_id: str, tool: str, args: dict) -> dict:
        raise AssertionError("foreign approval must not execute")

    with pytest.raises(ApprovalError):
        asyncio.run(queue.approve("account-a", b.id, execute))
    with pytest.raises(ApprovalError):
        asyncio.run(queue.grant("account-a", b.id, lambda _: {"signed": True}))

    assert snapshot(queue, "account-b", b.id) == before


def test_file_execution_report_requires_account_id_and_agent_together(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    b = enqueue(queue, "account-b", agent_id="agent-b")
    queue._claim("account-b", b.id, "owner", "checked")
    queue._grant("account-b", b, {"signed": True})
    before = snapshot(queue, "account-b", b.id)

    with pytest.raises(ApprovalError):
        queue.record_execution("account-a", b.id, "agent-b", ok=True, result={"filled": True})

    assert snapshot(queue, "account-b", b.id) == before
    result = queue.record_execution(
        "account-b", b.id, "agent-b", ok=True, result={"filled": True}
    )
    assert result.status == EXECUTED


def test_file_expiry_and_stale_reconciliation_touch_one_account(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    expired_a = queue.enqueue("account-a", "broker", "place", {}, ttl_s=-1)
    expired_b = queue.enqueue("account-b", "broker", "place", {}, ttl_s=-1)
    stale_a = enqueue(queue, "account-a")
    stale_b = enqueue(queue, "account-b")
    queue._claim("account-a", stale_a.id, "owner", "checked")
    queue._claim("account-b", stale_b.id, "owner", "checked")

    assert [record.id for record in queue.expire("account-a")] == [expired_a.id]
    assert queue._items[expired_b.id].status == PENDING
    assert [record.id for record in queue.reconcile_stale("account-a")] == [stale_a.id]
    assert queue._items[stale_a.id].status == ERROR
    assert queue._items[stale_a.id].outcome_unknown is True
    assert queue._items[stale_b.id].status == APPROVED


def test_file_clear_history_stamps_only_eligible_rows_for_one_account(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    a = enqueue(queue, "account-a")
    b = enqueue(queue, "account-b")
    queue.deny("account-a", a.id)
    queue.deny("account-b", b.id)

    assert queue.clear_history("account-a") == 1
    assert queue._items[a.id].cleared_at > 0
    assert queue._items[b.id].cleared_at == 0
    assert queue._items[b.id].status == DENIED


def test_file_pending_capacity_is_per_account(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")
    for _ in range(100):
        enqueue(queue, "account-a")
        enqueue(queue, "account-b")

    with pytest.raises(ApprovalError, match="limit 100"):
        enqueue(queue, "account-a")
    with pytest.raises(ApprovalError, match="limit 100"):
        enqueue(queue, "account-b")
    assert len(queue.list("account-a", status=PENDING, limit=200)) == 100
    assert len(queue.list("account-b", status=PENDING, limit=200)) == 100


def test_file_same_account_concurrency_never_exceeds_100(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    def attempt(_: int) -> bool:
        try:
            enqueue(queue, "account-a")
        except ApprovalError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=24) as executor:
        admitted = list(executor.map(attempt, range(140)))

    assert admitted.count(True) == 100
    assert len(queue.list("account-a", status=PENDING, limit=200)) == 100


def test_legacy_file_row_without_account_key_is_owner_invisible(tmp_path) -> None:
    path = tmp_path / "approvals.jsonl"
    path.write_text(
        json.dumps(
            {
                "id": "legacy-id",
                "connector_id": "broker",
                "tool": "place_order",
                "args": {"symbol": "SPY"},
                "status": PENDING,
            }
        )
        + "\n"
    )

    queue = ApprovalQueue(path)

    assert queue.get("account-a", "legacy-id") is None
    assert queue.list("account-a") == []
    assert queue.expire("account-a") == []
    assert queue.reconcile_stale("account-a") == []
