from __future__ import annotations

import contextlib
import inspect
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from nakagai_edge.approvals import (
    APPROVED,
    DENIED,
    ERROR,
    EXECUTED,
    EXPIRED,
    GRANTED,
    PENDING,
    Approval,
    ApprovalQueue,
    BaseApprovalQueue,
    PgApprovalQueue,
)


ALL_STATUSES = (PENDING, APPROVED, GRANTED, DENIED, EXPIRED, EXECUTED, ERROR)


def record(
    approval_id: str,
    *,
    account_key: str = "account-a",
    status: str = DENIED,
    created_at: float = 1.0,
    expires_at: float = 9_999_999_999.0,
    decided_at: float = 0.0,
    outcome_unknown: bool = False,
    cleared_at: float = 0.0,
) -> dict:
    return Approval(
        id=approval_id,
        account_key=account_key,
        connector_id="broker",
        tool="place_order",
        args={"symbol": "SPY", "quantity": 1},
        status=status,
        requested_by="agent",
        created_at=created_at,
        expires_at=expires_at,
        decided_at=decided_at,
        outcome_unknown=outcome_unknown,
        cleared_at=cleared_at,
    ).to_dict()


def file_queue(path: Path, rows: list[dict]) -> ApprovalQueue:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return ApprovalQueue(path)


def ids(rows: list[dict]) -> list[str]:
    return [str(row["id"]) for row in rows]


def test_history_and_attention_publish_exact_account_first_signatures() -> None:
    expected_history = (
        "self",
        "account_key",
        "statuses",
        "limit",
        "before",
    )
    expected_attention = ("self", "account_key", "limit", "after")

    for owner in (BaseApprovalQueue, ApprovalQueue, PgApprovalQueue):
        history = inspect.signature(owner.history)
        attention = inspect.signature(owner.attention)
        assert tuple(history.parameters) == expected_history
        assert tuple(attention.parameters) == expected_attention
        assert history.parameters["account_key"].default is inspect.Parameter.empty
        assert attention.parameters["account_key"].default is inspect.Parameter.empty
        assert history.parameters["statuses"].kind is inspect.Parameter.KEYWORD_ONLY
        assert attention.parameters["limit"].kind is inspect.Parameter.KEYWORD_ONLY


def test_history_orders_by_terminal_occurrence_and_uses_expiry_time(tmp_path) -> None:
    queue = file_queue(
        tmp_path / "approvals.jsonl",
        [
            record("late-terminal", created_at=1, decided_at=70),
            record("newer-created", created_at=100, decided_at=60),
            record("expired", status=EXPIRED, created_at=200, expires_at=65, decided_at=999),
            record("approved", status=APPROVED, created_at=300, decided_at=55),
            record("no-occurrence", status=GRANTED, created_at=400, decided_at=0),
        ],
    )

    rows = queue.history(
        "account-a",
        statuses=(DENIED, EXPIRED, APPROVED, GRANTED),
        limit=10,
    )

    assert ids(rows) == ["late-terminal", "expired", "newer-created", "approved"]


def test_history_is_account_scoped_before_equal_time_cursor_and_limit(tmp_path) -> None:
    queue = file_queue(
        tmp_path / "approvals.jsonl",
        [
            record("own-b", decided_at=40),
            record("own-a", decided_at=40),
            record("foreign-z", account_key="account-b", decided_at=40),
            record("cleared-z", decided_at=50, cleared_at=51),
            record("no-time-z", decided_at=0),
            record("older", decided_at=30),
        ],
    )

    first = queue.history("account-a", statuses=(DENIED,), limit=1)
    second = queue.history(
        "account-a", statuses=(DENIED,), limit=1, before=(40, "own-b")
    )
    third = queue.history(
        "account-a", statuses=(DENIED,), limit=10, before=(40, "own-a")
    )

    assert ids(first) == ["own-b"]
    assert ids(second) == ["own-a"]
    assert ids(third) == ["older"]


def test_history_strict_time_cursor_excludes_every_equal_time_id(tmp_path) -> None:
    queue = file_queue(
        tmp_path / "approvals.jsonl",
        [
            record("same-z", decided_at=40),
            record("same-a", decided_at=40),
            record("older", decided_at=39),
        ],
    )

    tuple_rows = queue.history(
        "account-a", statuses=(DENIED,), before=(40, "same-z")
    )
    strict_rows = queue.history(
        "account-a", statuses=(DENIED,), before=(40, None)
    )

    assert ids(tuple_rows) == ["same-a", "older"]
    assert ids(strict_rows) == ["older"]


def test_history_expires_owed_pending_rows_before_reading(tmp_path) -> None:
    expired_at = time.time() - 5
    queue = file_queue(
        tmp_path / "approvals.jsonl",
        [record("owed", status=PENDING, created_at=1, expires_at=expired_at)],
    )

    rows = queue.history("account-a", statuses=(EXPIRED,), limit=1)

    assert ids(rows) == ["owed"]
    assert rows[0]["status"] == EXPIRED
    assert rows[0]["expires_at"] == expired_at


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"statuses": ()}, "statuses"),
        ({"statuses": ("unknown",)}, "statuses"),
        ({"statuses": (DENIED,), "limit": 0}, "limit"),
        ({"statuses": (DENIED,), "limit": 1001}, "limit"),
        ({"statuses": (DENIED,), "limit": True}, "limit"),
        ({"statuses": (DENIED,), "before": (-1, "x")}, "cursor time"),
        ({"statuses": (DENIED,), "before": (math.inf, "x")}, "cursor time"),
        ({"statuses": (DENIED,), "before": (1, "")}, "cursor id"),
        ({"statuses": (DENIED,), "before": (1, "   ")}, "cursor id"),
    ],
)
def test_history_invalid_input_fails_closed(tmp_path, kwargs: dict, message: str) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    with pytest.raises(ValueError, match=message):
        queue.history("account-a", **kwargs)


def test_history_rejects_old_unkeyed_call(tmp_path) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    with pytest.raises(TypeError):
        queue.history(statuses=(DENIED,))


def attention_rows() -> list[dict]:
    return [
        record("unknown-fallback", status=ERROR, created_at=10, outcome_unknown=True),
        record(
            "unknown-decided",
            status=ERROR,
            created_at=5,
            decided_at=20,
            outcome_unknown=True,
        ),
        record("pending-a", status=PENDING, created_at=15),
        record("pending-b", status=PENDING, created_at=25),
        record(
            "cleared-unknown",
            status=ERROR,
            created_at=1,
            decided_at=1,
            outcome_unknown=True,
            cleared_at=2,
        ),
        record("known-error", status=ERROR, created_at=2, decided_at=2),
        record(
            "foreign-equal",
            account_key="account-b",
            status=ERROR,
            created_at=10,
            outcome_unknown=True,
        ),
    ]


def test_attention_returns_exact_summary_and_deterministic_exclusive_pages(tmp_path) -> None:
    queue = file_queue(tmp_path / "approvals.jsonl", attention_rows())

    first = queue.attention("account-a", limit=2)
    second = queue.attention("account-a", limit=2, after=(0, 20, "unknown-decided"))
    empty = queue.attention("account-a", limit=2, after=(1, 25, "pending-b"))

    assert ids(first["records"]) == ["unknown-fallback", "unknown-decided"]
    assert ids(second["records"]) == ["pending-a", "pending-b"]
    assert empty["records"] == []
    for result in (first, second, empty):
        assert result["total"] == 4
        assert result["oldest_at"] == 10


def test_attention_equal_keys_use_id_and_never_cross_accounts(tmp_path) -> None:
    rows = [
        record("own-a", status=PENDING, created_at=10),
        record("own-b", status=PENDING, created_at=10),
        record("foreign-c", account_key="account-b", status=PENDING, created_at=10),
    ]
    queue = file_queue(tmp_path / "approvals.jsonl", rows)

    first = queue.attention("account-a", limit=1)
    second = queue.attention("account-a", limit=1, after=(1, 10, "own-a"))

    assert ids(first["records"]) == ["own-a"]
    assert ids(second["records"]) == ["own-b"]
    assert first["total"] == second["total"] == 2


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit": 0}, "limit"),
        ({"limit": 1001}, "limit"),
        ({"after": (-1, 1, "x")}, "group"),
        ({"after": (2, 1, "x")}, "group"),
        ({"after": (0, -1, "x")}, "cursor time"),
        ({"after": (0, math.nan, "x")}, "cursor time"),
        ({"after": (0, 1, "")}, "cursor id"),
        ({"after": (0, 1, " ")}, "cursor id"),
    ],
)
def test_attention_invalid_input_fails_closed(tmp_path, kwargs: dict, message: str) -> None:
    queue = ApprovalQueue(tmp_path / "approvals.jsonl")

    with pytest.raises(ValueError, match=message):
        queue.attention("account-a", **kwargs)


class QueryResult:
    def __init__(self, *, many: list[tuple] | None = None) -> None:
        self.many = [] if many is None else many

    def fetchall(self) -> list[tuple]:
        return self.many


class QueryConnection:
    def __init__(self, database: "QueryDatabase") -> None:
        self.database = database

    def execute(self, sql: str, params=()) -> QueryResult:
        normalized = " ".join(sql.split()).lower()
        params = tuple(params)
        self.database.statements.append((normalized, params))
        if normalized.startswith("update approvals set status = 'expired'"):
            return QueryResult()
        if normalized.startswith("with eligible as"):
            return QueryResult(many=self.database.attention_result)
        if normalized.startswith("select id,"):
            return QueryResult(many=self.database.history_result)
        raise AssertionError(f"unexpected SQL: {normalized}")


class QueryPool:
    def __init__(self, database: "QueryDatabase") -> None:
        self.database = database

    @contextlib.contextmanager
    def connection(self):
        yield QueryConnection(self.database)


class QueryDatabase:
    def __init__(
        self,
        *,
        history_result: list[tuple] | None = None,
        attention_result: list[tuple] | None = None,
    ) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.history_result = [] if history_result is None else history_result
        self.attention_result = [] if attention_result is None else attention_result
        self.pool = QueryPool(self)


def pg_row(row: dict) -> tuple:
    values = dict(row)
    for key in ("created_at", "expires_at", "decided_at", "cleared_at"):
        value = float(values[key])
        values[key] = datetime.fromtimestamp(value, UTC) if value else None
    return tuple(values[key] for key in Approval._FIELDS)


def test_pg_history_returns_rows_and_pushes_scope_order_cursor_and_limit_into_sql() -> None:
    expected = record("own-a", decided_at=40)
    database = QueryDatabase(history_result=[pg_row(expected)])
    queue = PgApprovalQueue(database)

    rows = queue.history(
        "account-a",
        statuses=(DENIED, EXPIRED),
        limit=7,
        before=(50, "cursor-id"),
    )

    assert ids(rows) == ["own-a"]
    sql, params = next(
        statement for statement in database.statements if statement[0].startswith("select id,")
    )
    assert "workspace_id = %s" in sql
    assert "status in (%s, %s)" in sql
    assert "cleared_at is null" in sql
    assert "case when status = 'expired' then expires_at else decided_at end is not null" in sql
    assert "(case when status = 'expired' then expires_at else decided_at end, id) < (to_timestamp(%s), %s)" in sql
    assert "order by case when status = 'expired' then expires_at else decided_at end desc, id desc" in sql
    assert sql.endswith("limit %s")
    assert params == ("account-a", DENIED, EXPIRED, 50, "cursor-id", 7)
    assert all("workspace_id = %s" in statement for statement, _ in database.statements)


def test_pg_history_strict_time_cursor_has_no_id_comparison() -> None:
    database = QueryDatabase()

    PgApprovalQueue(database).history(
        "account-a", statuses=(DENIED,), before=(50, None)
    )

    sql, params = next(
        statement for statement in database.statements if statement[0].startswith("select id,")
    )
    assert "case when status = 'expired' then expires_at else decided_at end < to_timestamp(%s)" in sql
    assert "end, id) <" not in sql
    assert params == ("account-a", DENIED, 50, 100)


def test_pg_attention_returns_exact_summary_and_pushes_bounded_page_into_sql() -> None:
    expected = record("pending-a", status=PENDING, created_at=15)
    width = len(Approval._FIELDS)
    database = QueryDatabase(attention_result=[(*pg_row(expected), 4, 10.0)])
    queue = PgApprovalQueue(database)

    result = queue.attention("account-a", limit=3, after=(0, 20, "unknown-decided"))

    assert ids(result["records"]) == ["pending-a"]
    assert result["total"] == 4
    assert result["oldest_at"] == 10
    sql, params = next(
        statement for statement in database.statements if statement[0].startswith("with eligible as")
    )
    assert "workspace_id = %s" in sql
    assert "cleared_at is null" in sql
    assert "status = 'pending'" in sql
    assert "status = 'error' and outcome_unknown" in sql
    assert "count(*)" in sql
    assert "min(owed_at)" in sql
    assert "(attention_group, owed_at, id) > (%s, to_timestamp(%s), %s)" in sql
    assert "order by attention_group, owed_at, id" in sql
    assert "limit %s" in sql
    assert params == ("account-a", 0, 20, "unknown-decided", 3)
    assert width + 2 == len(database.attention_result[0])
    assert all("workspace_id = %s" in statement for statement, _ in database.statements)


def test_pg_attention_empty_page_keeps_complete_summary() -> None:
    empty_page = (None,) * len(Approval._FIELDS)
    database = QueryDatabase(attention_result=[(*empty_page, 4, 10.0)])

    result = PgApprovalQueue(database).attention(
        "account-a", limit=2, after=(1, 25, "pending-b")
    )

    assert result == {"records": [], "total": 4, "oldest_at": 10.0}
