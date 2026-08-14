from __future__ import annotations

import inspect

import pytest

from nakagai_edge.approvals import ApprovalQueue, BaseApprovalQueue


RECORD_METHODS = (
    "decision_lock",
    "enqueue",
    "get",
    "list",
    "history",
    "attention",
    "clear_history",
    "recent_for_symbol",
    "set_rationale",
    "resolve",
    "approve",
    "deny",
    "grant",
    "record_execution",
    "expire",
    "reconcile_stale",
    "_claim",
    "_finish",
    "_resolve",
    "_grant",
    "_why_unclaimable",
)


def test_every_record_method_requires_leading_account_key() -> None:
    for method_name in RECORD_METHODS:
        owner = ApprovalQueue if hasattr(ApprovalQueue, method_name) else BaseApprovalQueue
        method = getattr(owner, method_name)
        parameters = list(inspect.signature(method).parameters.values())

        assert [parameter.name for parameter in parameters[:2]] == ["self", "account_key"], (
            method_name
        )
        assert parameters[1].default is inspect.Parameter.empty, method_name
        assert "workspace" not in inspect.signature(method).parameters, method_name


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("enqueue", ("broker", "place_order", {}), {"ttl_s": 60}),
        ("get", ("approval-id",), {}),
        ("list", (), {}),
        ("history", (), {"statuses": ("denied",)}),
        ("attention", (), {}),
        ("clear_history", (), {}),
        ("recent_for_symbol", ("SPY",), {}),
        ("set_rationale", ("approval-id", {}), {}),
        ("deny", ("approval-id",), {}),
        ("record_execution", ("approval-id", "agent-id"), {"ok": True}),
        ("expire", (), {}),
        ("reconcile_stale", (), {}),
    ],
)
def test_old_file_queue_call_shapes_are_rejected(
    tmp_path, method_name: str, args: tuple, kwargs: dict
) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    with pytest.raises(TypeError):
        getattr(queue, method_name)(*args, **kwargs)


@pytest.mark.parametrize("account_key", ["", "   "])
def test_new_file_records_require_nonempty_account_key(tmp_path, account_key: str) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    with pytest.raises(ValueError, match="account_key"):
        queue.enqueue(account_key, "broker", "place_order", {}, ttl_s=60)
