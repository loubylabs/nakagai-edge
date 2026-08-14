from __future__ import annotations

import asyncio
import contextlib
import inspect
from datetime import UTC, datetime, timedelta

import pytest

from nakagai_edge.approvals import (
    ERROR,
    EXECUTED,
    Approval,
    ApprovalError,
    PgApprovalQueue,
)


PG_RECORD_METHODS = (
    "decision_lock",
    "enqueue",
    "get",
    "list",
    "history",
    "attention",
    "clear_history",
    "recent_for_symbol",
    "set_rationale",
    "deny",
    "record_execution",
    "expire",
    "reconcile_stale",
    "_claim",
    "_finish",
    "_resolve",
    "_grant",
    "_why_unclaimable",
)


def pg_row(*, account_key: str = "account-a", status: str = "pending") -> tuple:
    now = datetime.now(UTC)
    values = {
        "id": "approval-a",
        "account_key": account_key,
        "connector_id": "broker",
        "tool": "place_order",
        "args": {"symbol": "SPY"},
        "status": status,
        "requested_by": "agent",
        "created_at": now,
        "expires_at": now + timedelta(minutes=5),
        "decided_at": None,
        "decided_by": "",
        "reason": "",
        "result": None,
        "error": "",
        "outcome_unknown": False,
        "agent_id": "agent-a",
        "artifact": None,
        "signal_id": "signal-a",
        "signal": {"symbol": "SPY"},
        "notional": 100.0,
        "rationale": None,
        "cleared_at": None,
    }
    return tuple(values[column] for column in PgApprovalQueue.COLUMNS)


class Result:
    def __init__(self, one=None, many=None) -> None:
        self.one = one
        self.many = many

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many if self.many is not None else ([] if self.one is None else [self.one])


class RecordingConnection:
    def __init__(self, statements: list[tuple[str, tuple]]) -> None:
        self.statements = statements

    @contextlib.contextmanager
    def transaction(self):
        self.statements.append(("BEGIN", ()))
        yield
        self.statements.append(("COMMIT", ()))

    def execute(self, sql: str, params=()) -> Result:
        normalized = " ".join(sql.split()).lower()
        params = tuple(params)
        self.statements.append((normalized, params))
        if "pg_try_advisory_xact_lock" in normalized:
            return Result((True,))
        if "select count(*)" in normalized:
            return Result((0,))
        if "select status, decided_by" in normalized:
            return Result(many=[("denied", "owner", datetime.now(UTC), 100.0)])
        if "returning id" in normalized and "returning id," not in normalized:
            if "foreign-id" in params and "workspace_id = %s" in normalized:
                return Result(None, [])
            return Result(("approval-a",), [("approval-a",)])
        if " returning " in normalized or normalized.startswith("select id,"):
            account = next((value for value in params if value in {"account-a", "account-b"}), "account-a")
            approval_id = next((value for value in params if value in {"approval-a", "foreign-id"}), "approval-a")
            if approval_id == "foreign-id" and "workspace_id = %s" in normalized:
                return Result(None, [])
            return Result(pg_row(account_key=account), [pg_row(account_key=account)])
        return Result((None,))


class RecordingPool:
    def __init__(self, statements: list[tuple[str, tuple]]) -> None:
        self.statements = statements

    @contextlib.contextmanager
    def connection(self, **kwargs):
        self.statements.append(("CONNECTION", tuple(sorted(kwargs.items()))))
        yield RecordingConnection(self.statements)


class RecordingDatabase:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.pool = RecordingPool(self.statements)


def approval(*, approval_id: str = "approval-a", status: str = "approved",
             unknown: bool = False) -> Approval:
    return Approval(
        id=approval_id,
        account_key="account-a",
        connector_id="broker",
        tool="place_order",
        args={"symbol": "SPY"},
        status=status,
        agent_id="agent-a",
        outcome_unknown=unknown,
    )


def approval_sql(database: RecordingDatabase) -> list[tuple[str, tuple]]:
    return [
        statement
        for statement in database.statements
        if " approvals" in statement[0] or "approval(" in statement[0]
    ]


def assert_account_scoped(database: RecordingDatabase, account_key: str = "account-a") -> None:
    statements = approval_sql(database)
    assert statements
    for sql, params in statements:
        if sql.startswith("insert into approvals"):
            assert "workspace_id" in sql
        else:
            assert "workspace_id = %s" in sql
        assert account_key in params


def test_pg_record_methods_require_leading_account_key_and_constructor_has_no_tenant() -> None:
    for method_name in PG_RECORD_METHODS:
        parameters = list(inspect.signature(getattr(PgApprovalQueue, method_name)).parameters.values())
        assert [parameter.name for parameter in parameters[:2]] == ["self", "account_key"], method_name
        assert parameters[1].default is inspect.Parameter.empty, method_name
        assert "workspace" not in inspect.signature(getattr(PgApprovalQueue, method_name)).parameters

    with pytest.raises(TypeError):
        PgApprovalQueue(RecordingDatabase(), "account-a")


@pytest.mark.parametrize(
    "operation",
    [
        lambda queue: queue.get("account-a", "approval-a"),
        lambda queue: queue.list("account-a", status="pending"),
        lambda queue: queue.clear_history("account-a"),
        lambda queue: queue.set_rationale("account-a", "approval-a", {"summary": "safe"}),
        lambda queue: queue.deny("account-a", "approval-a", reason="no"),
        lambda queue: queue._claim("account-a", "approval-a", "owner", "yes"),
        lambda queue: queue._finish("account-a", approval(), EXECUTED, result={"ok": True}),
        lambda queue: queue._resolve("account-a", approval(status=ERROR, unknown=True), "checked"),
        lambda queue: queue._grant("account-a", approval(), {"signed": True}),
        lambda queue: queue.record_execution("account-a", "approval-a", "agent-a", ok=True),
        lambda queue: queue.expire("account-a"),
        lambda queue: queue.reconcile_stale("account-a"),
        lambda queue: queue.recent_for_symbol("account-a", "SPY"),
    ],
)
def test_pg_every_record_query_carries_account_predicate(operation) -> None:
    database = RecordingDatabase()
    operation(PgApprovalQueue(database))
    assert_account_scoped(database)


def test_pg_decision_locks_are_distinct_per_account() -> None:
    database = RecordingDatabase()
    queue = PgApprovalQueue(database)

    with queue.decision_lock("account-a"):
        pass
    with queue.decision_lock("account-b"):
        pass

    locks = [entry for entry in database.statements if "pg_try_advisory_xact_lock" in entry[0]]
    assert [params[-1] for _, params in locks] == ["account-a", "account-b"]


def test_pg_admission_counts_inside_the_account_transaction() -> None:
    database = RecordingDatabase()
    queue = PgApprovalQueue(database)

    queue.enqueue("account-a", "broker", "place_order", {"symbol": "SPY"}, ttl_s=60)

    labels = [
        "begin" if sql == "BEGIN" else
        "expire" if sql.startswith("update approvals") and "expired" in sql else
        "count" if "select count(*)" in sql else
        "insert" if sql.startswith("insert into approvals") else
        "commit" if sql == "COMMIT" else "other"
        for sql, _ in database.statements
    ]
    assert labels.index("begin") < labels.index("expire") < labels.index("count") < labels.index("insert") < labels.index("commit")
    assert_account_scoped(database)


@pytest.mark.parametrize(
    "operation",
    [
        lambda queue: queue.deny("account-a", "foreign-id"),
        lambda queue: queue._claim("account-a", "foreign-id", "owner", "yes"),
        lambda queue: queue._finish(
            "account-a", approval(approval_id="foreign-id"), EXECUTED
        ),
        lambda queue: queue._resolve(
            "account-a", approval(approval_id="foreign-id", status=ERROR, unknown=True), "checked"
        ),
        lambda queue: queue._grant(
            "account-a", approval(approval_id="foreign-id"), {"signed": True}
        ),
        lambda queue: queue.record_execution("account-a", "foreign-id", "agent-a", ok=True),
    ],
)
def test_pg_foreign_id_transitions_are_not_found(operation) -> None:
    database = RecordingDatabase()
    queue = PgApprovalQueue(database)

    with pytest.raises(ApprovalError, match="no approval"):
        operation(queue)
    assert_account_scoped(database)


def test_pg_public_foreign_id_transitions_never_execute_or_sign() -> None:
    database = RecordingDatabase()
    queue = PgApprovalQueue(database)

    async def execute(*_args):
        raise AssertionError("foreign approval must not execute")

    def sign(_record):
        raise AssertionError("foreign approval must not be signed")

    with pytest.raises(ApprovalError, match="no approval"):
        asyncio.run(queue.approve("account-a", "foreign-id", execute))
    with pytest.raises(ApprovalError, match="no approval"):
        asyncio.run(queue.grant("account-a", "foreign-id", sign))
    with pytest.raises(ApprovalError, match="no approval"):
        queue.resolve("account-a", "foreign-id", placed=False)
    assert queue.set_rationale("account-a", "foreign-id", {"summary": "foreign"}) is False

    assert_account_scoped(database)


def test_pg_clear_history_excludes_in_flight_and_unknown_outcomes() -> None:
    database = RecordingDatabase()

    PgApprovalQueue(database).clear_history("account-a")

    clear_sql = next(sql for sql, _ in database.statements if "set cleared_at" in sql)
    assert "status in ('denied', 'expired', 'executed', 'error')" in clear_sql
    assert "not outcome_unknown" in clear_sql
    assert "status <> 'pending'" not in clear_sql
