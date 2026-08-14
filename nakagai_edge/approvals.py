"""Human-in-the-loop approvals for state-changing downstream calls.

When a guardrail returns `approve`, the call does not execute. Nakagai records
what the agent *asked* for and hands back an `approval_id`. A human then
approves or denies it, and **Nakagai executes the stored request server-side**, so
the agent never gets a second chance to change the arguments.

Two backends, one contract:

* `ApprovalQueue`: in-memory dict + append-only jsonl journal, mirroring
  `nakagai.api.jobs.JobRegistry`. The compare-and-set is a `threading.Lock`, so
  it is atomic **within one process only**.
* `PgApprovalQueue`: the same contract with the CAS in Postgres
  (`UPDATE … WHERE status = 'pending' RETURNING *`). Atomic across workers and
  machines; this is what makes horizontal scaling safe.

Safety properties this file is responsible for:

* **Args integrity.** Execution uses the args captured at request time.
* **No double execution.** The pending→approved transition is a compare-and-set;
  a losing racer raises instead of placing a second order.
* **Re-validation at execute time.** The connector's guardrails are evaluated
  again against current config. A connector disabled (or `allow_writes` revoked,
  or the account allowlist tightened) after the request was made will refuse the
  execution even though a human clicked approve.
* **Never auto-execute on restart.** A process that died between `approve` and
  the downstream call leaves a record whose outcome at the broker is *unknown*.
  Guessing is how you place an order twice.
"""

from __future__ import annotations

import contextlib
import copy
import json
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger(__name__)

PENDING, APPROVED, GRANTED, DENIED, EXPIRED, EXECUTED, ERROR = (
    "pending", "approved", "granted", "denied", "expired", "executed", "error")

TERMINAL = {DENIED, EXPIRED, EXECUTED, ERROR}
APPROVAL_STATUSES = frozenset(
    {PENDING, APPROVED, GRANTED, DENIED, EXPIRED, EXECUTED, ERROR}
)

# A runaway agent must not be able to fill one account's disk or memory with
# pending approvals. Enqueue past this and that account's request is refused.
APPROVAL_MAX_PENDING_PER_ACCOUNT = 100

# The budget a worker gives the autopilot decision lock: how long it will wait for a
# pooled connection, and how long a statement inside the lock transaction may run.
# NOT a wait for the lock itself: that is a `try` (see PgApprovalQueue.
# decision_lock), so a contended lock declines at once rather than blocking. A
# database that has gone slow or away must make autopilot decline, never hang.
LOCK_TIMEOUT_S = 10.0


class ApprovalError(Exception):
    """The approval could not be recorded or acted on."""


class DecisionLockError(ApprovalError):
    """The autopilot decision lock could not be taken: a sibling worker is
    mid-decision, or the database is unreachable. The caller must DECLINE (leave
    the record pending for a human); it must never auto-execute without it."""


def _require_account_key(account_key: str) -> str:
    if not isinstance(account_key, str) or not account_key.strip():
        raise ValueError("account_key must be a nonempty string")
    return account_key


def _require_page_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer from 1 through 1000")
    return limit


def _require_cursor_time(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("cursor time must be finite and nonnegative")
    value = float(value)
    if not math.isfinite(value) or value < 0:
        raise ValueError("cursor time must be finite and nonnegative")
    return value


def _require_cursor_id(value: str | None, *, optional: bool) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("cursor id must be a nonempty string")
    return value


def _history_inputs(
    statuses: tuple[str, ...], limit: int,
    before: tuple[float, str | None] | None,
) -> tuple[tuple[str, ...], int, tuple[float, str | None] | None]:
    if (not isinstance(statuses, tuple) or not statuses
            or any(not isinstance(status, str) or status not in APPROVAL_STATUSES
                   for status in statuses)):
        raise ValueError("statuses must be a nonempty tuple of approval statuses")
    limit = _require_page_limit(limit)
    if before is not None:
        if not isinstance(before, tuple) or len(before) != 2:
            raise ValueError("before must be a two-part cursor")
        before = (
            _require_cursor_time(before[0]),
            _require_cursor_id(before[1], optional=True),
        )
    return statuses, limit, before


def _attention_inputs(
    limit: int, after: tuple[int, float, str] | None,
) -> tuple[int, tuple[int, float, str] | None]:
    limit = _require_page_limit(limit)
    if after is not None:
        if not isinstance(after, tuple) or len(after) != 3:
            raise ValueError("after must be a three-part cursor")
        group, occurred_at, approval_id = after
        if isinstance(group, bool) or not isinstance(group, int) or group not in (0, 1):
            raise ValueError("cursor group must be zero or one")
        after = (
            group,
            _require_cursor_time(occurred_at),
            _require_cursor_id(approval_id, optional=False),
        )
    return limit, after


def _terminal_occurrence(approval: Approval) -> float:
    value = approval.expires_at if approval.status == EXPIRED else approval.decided_at
    return float(value or 0.0)


def _attention_key(approval: Approval) -> tuple[int, float, str]:
    if approval.status == ERROR and approval.outcome_unknown:
        return (0, float(approval.decided_at or approval.created_at), approval.id)
    return (1, float(approval.created_at), approval.id)


@dataclass
class Approval:
    id: str
    account_key: str
    connector_id: str
    tool: str
    args: dict
    status: str = PENDING
    requested_by: str = ""
    created_at: float = 0.0
    expires_at: float = 0.0
    decided_at: float = 0.0
    decided_by: str = ""
    reason: str = ""
    result: Any = None
    error: str = ""
    # Set when a write was dispatched downstream but the outcome is unknown
    # (timeout, crash). The broker may hold an order Nakagai never saw.
    outcome_unknown: bool = False
    # Edge-origin approvals: which paired agent asked, and the signed artifact
    # the edge must present-and-verify before executing. Platform-origin
    # records leave both empty.
    agent_id: str = ""
    artifact: Any = None
    # What this order CLAIMS to be. `signal_id` is the id of a signal Nakagai
    # itself emitted; `signal` is the frozen record as it stood at request time,
    # so the approvals screen and the journal show the evidence a human is
    # actually deciding on. `notional` is the extracted order size, stored so the
    # day's autopilot budget is computable from this queue alone, with no second
    # ledger to drift out of sync. All empty for an order that cited nothing.
    signal_id: str = ""
    signal: dict | None = None
    notional: float = 0.0
    # Copilot read: an advisory LLM-generated second opinion, written AFTER
    # enqueue by nakagai/api/rationale.py. Success: {summary, concerns, lean,
    # model, generated_at}. Failure: {error, generated_at}. None: not generated
    # (still running, key unset, or lost to a restart). Display-only; nothing
    # in the mandate or autopilot path reads it.
    rationale: dict | None = None
    # Set when the owner cleared this decided record from the History view.
    # A hide, never a delete: the record stays in the journal and in list(),
    # which autoapprove.py and mandate.py read for budgets and reconciliation.
    cleared_at: float = 0.0

    _FIELDS = ("id", "account_key", "connector_id", "tool", "args", "status",
               "requested_by", "created_at", "expires_at", "decided_at", "decided_by",
               "reason", "result", "error", "outcome_unknown", "agent_id", "artifact",
               "signal_id", "signal", "notional", "rationale", "cleared_at")

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self._FIELDS}

    def public(self) -> dict:
        """What an agent may see: its own request and the outcome, never the
        approver's identity."""
        d = self.to_dict()
        d.pop("decided_by", None)
        # The copilot read is the human's second opinion on the agent's own
        # proposal; feeding it back to the proposer would let the agent tune
        # against its reviewer. The agent never sees it.
        d.pop("rationale", None)
        return d

    def is_expired(self, now: float | None = None) -> bool:
        now = now if now is not None else time.time()
        return self.status == PENDING and self.expires_at and now >= self.expires_at


class BaseApprovalQueue:
    """The orchestration every backend shares. Subclasses supply the atomic bits:
    `enqueue`, `get`, `list`, `deny`, `_claim` (the CAS), `_finish`, `_resolve`."""

    @contextlib.contextmanager
    def decision_lock(self, account_key: str, timeout_s: float = LOCK_TIMEOUT_S):
        """Serialize autopilot's read → check → claim for one account.

        The CAS below makes sure an approval is decided at most ONCE. It does not
        make the mandate's *budget* safe: `autoapprove.py` reads the day's usage
        (and the once-per-signal fence) from this queue and only then claims. Two
        workers can both read `orders == 4`, both pass `daily_order_max`, and both
        claim two different records, overshooting the day's cap by (workers - 1).
        The cap is the primary containment for a runaway loop, so it has to be
        atomic, not advisory. Hence: hold this across the whole sequence.

        The default is a NO-OP, which is correct for `ApprovalQueue`: it is
        file-backed and single-process by construction (its CAS is a
        `threading.Lock`), so there is no second reader to serialize against, and
        the auto-approver's read→claim contains no `await`. `PgApprovalQueue`, the
        backend that exists precisely so several API workers can run, overrides it
        with a Postgres advisory lock.

        Raises `DecisionLockError` if the lock cannot be taken. The caller declines.
        """
        _require_account_key(account_key)
        yield

    def _claim(self, account_key: str, approval_id: str, decided_by: str,
               reason: str) -> Approval:
        """Atomically move pending → approved. Raise if it is not claimable."""
        raise NotImplementedError

    def _finish(self, account_key: str, a: Approval, status: str, *, result=None,
                error: str = "",
                outcome_unknown: bool = False) -> Approval:
        """Record the terminal state of an approved record."""
        raise NotImplementedError

    def _resolve(self, account_key: str, a: Approval, note: str) -> Approval:
        """Clear `outcome_unknown` and append the human's finding to `error`."""
        raise NotImplementedError

    def set_rationale(self, account_key: str, approval_id: str,
                      payload: dict) -> bool:
        """Attach the copilot read. Advisory only, so unlike every decision
        method this never raises for a missing row: the generator runs in a
        fire-and-forget thread with nobody to catch."""
        raise NotImplementedError

    def clear_history(self, account_key: str) -> int:
        """Stamp `cleared_at` on denied, expired, executed, and known-outcome
        error records that lack one. Returns how many were stamped.

        Never touches pending, approved, granted, or `outcome_unknown` records.
        An unreconciled record is the only thing telling agents not to resubmit
        an order that may be live at the broker, so it stays visible until a
        human resolves it. A hide, not a delete: `list()` keeps returning
        cleared records because budgets and reconciliation scans read it."""
        raise NotImplementedError

    def history(
        self,
        account_key: str,
        *,
        statuses: tuple[str, ...],
        limit: int = 100,
        before: tuple[float, str | None] | None = None,
    ) -> list[dict]:
        """Return one account's uncleared approval history page."""
        raise NotImplementedError

    def attention(
        self,
        account_key: str,
        *,
        limit: int = 100,
        after: tuple[int, float, str] | None = None,
    ) -> dict:
        """Return one account's action-owed page and exact summary."""
        raise NotImplementedError

    def recent_for_symbol(self, account_key: str, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        """Recent approvals citing a signal for `symbol`, newest first. Feeds
        the copilot read only ("third NVDA proposal today, last two denied"),
        so the shape is a compact display dict, not an Approval.

        Every backend scopes this history inside the supplied account before
        observing a record."""
        raise NotImplementedError

    def resolve(self, account_key: str, approval_id: str, *, placed: bool,
                note: str = "",
                resolved_by: str = "") -> Approval:
        """Record what a human found at the broker for an `outcome_unknown` call.

        Nakagai cannot know whether a timed-out `place_*` reached the broker, so
        it never guesses. A person checks the account at the broker and records
        the answer here. Only then does the record stop warning
        agents off, and only a human's word clears it.

        `placed=True` means the order IS live at the broker; `placed=False` means
        it is not, so the action may be requested again.
        """
        a = self.get(account_key, approval_id)
        if a is None:
            raise ApprovalError(f"no approval {approval_id!r}")
        if not a.outcome_unknown:
            raise ApprovalError(
                f"approval {approval_id!r} is not awaiting reconciliation "
                f"(status={a.status!r}, outcome_unknown=False)")
        verdict = "REACHED the broker" if placed else "did NOT reach the broker"
        who = f" by {resolved_by}" if resolved_by else ""
        stamp = f"reconciled{who}: the call {verdict}."
        return self._resolve(account_key, a, f"{stamp} {note}".strip())

    async def approve(self, account_key: str, approval_id: str, execute, *,
                      decided_by: str = "", reason: str = "") -> Approval:
        """Approve and execute, once.

        `execute(connector_id, tool, args) -> dict` runs the real downstream call
        and must re-check the connector's guardrails against current config.
        Everything after `_claim` is on the far side of the compare-and-set, so a
        second concurrent approve raises instead of placing a second order.
        """
        a = self._claim(account_key, approval_id, decided_by, reason)
        try:
            # Deep copy: a shallow dict() still shares nested order payloads, and
            # what executes must be exactly what the human read on screen.
            result = await execute(a.connector_id, a.tool, copy.deepcopy(a.args))
        except BaseException as e:  # noqa: BLE001 (status must reflect reality)
            if isinstance(e, KeyboardInterrupt | SystemExit):
                raise
            # A guardrail refusal happened *before* anything left Nakagai.
            # Anything else may have reached the broker.
            unknown = type(e).__name__ != "GuardrailDenied"
            return self._finish(account_key, a, ERROR,
                                error=f"{type(e).__name__}: {e}",
                                outcome_unknown=unknown)
        try:
            return self._finish(account_key, a, EXECUTED, result=result)
        except Exception as e:  # noqa: BLE001 - the order already FILLED
            # The write left Nakagai and the broker took it. A failure to RECORD
            # that is not a failure to EXECUTE, and reporting it as one is the
            # worst lie this queue can tell: the agent sees a crash for an order
            # that filled, and an agent that retries on a crash doubles the
            # position.
            #
            # So we return the truth in memory. The stored record stays `approved`
            # and stale reconciliation sweeps it to outcome_unknown, where
            # a human resolves it against the broker, which is the correct
            # conservative end state: the owner is ASKED to check, never TOLD a
            # falsehood.
            #
            # That in-memory return is silent on its own, though: nobody else
            # learns this happened until the next process boot runs
            # stale reconciliation (see `_install_approval_queue()` in
            # `nakagai/api/app.py`, which only runs once, at startup, not on a
            # schedule). On a long-running API that could be days away, with the
            # stored record sitting inconsistent with reality the whole time. So
            # an operator needs to hear about this now, not at the next restart.
            #
            # try/except around the log call itself: a logging handler that
            # raises (a full disk under a file handler, a broken formatter) must
            # not turn this already-filled order back into a crash. Best effort,
            # same as the journal write this is standing in for.
            try:
                log.error(
                    "approval %s (%s.%s): order reached the broker and was "
                    "accepted, but recording that outcome failed: %r. The stored "
                    "record is now inconsistent with reality and needs "
                    "reconciling against the broker.",
                    a.id, a.connector_id, a.tool, e)
            except Exception:  # noqa: BLE001 - never let logging crash a fill
                pass
            a.status, a.result = EXECUTED, result
            return a

    async def grant(self, account_key: str, approval_id: str, build_artifact, *,
                    decided_by: str = "", reason: str = "") -> Approval:
        """Approve an EDGE-origin request: claim it (same CAS as approve), then
        record the signed artifact instead of executing. The platform holds no
        broker credentials, so execution happens at the edge, which reports
        back via record_execution()."""
        a = self._claim(account_key, approval_id, decided_by, reason)
        return self._grant(account_key, a, build_artifact(a))

    def _grant(self, account_key: str, a: Approval, artifact: dict) -> Approval:
        raise NotImplementedError

    def record_execution(self, account_key: str, approval_id: str, agent_id: str, *,
                         ok: bool,
                         result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
        raise NotImplementedError

    def expire(self, account_key: str) -> list[Approval]:
        raise NotImplementedError

    def reconcile_stale(self, account_key: str) -> list[Approval]:
        raise NotImplementedError

    def _why_unclaimable(self, account_key: str,
                         approval_id: str) -> ApprovalError:
        raise NotImplementedError


class ApprovalQueue(BaseApprovalQueue):
    """File-backed. The CAS is a process-local lock; see `assert_single_worker`."""

    def __init__(self, path: Path | None = None,
                 on_snapshot: Callable[[Approval], None] | None = None) -> None:
        self._items: dict[str, Approval] = {}
        self._lock = threading.Lock()
        self._path = path
        self._on_snapshot = on_snapshot
        if path is not None and path.exists():
            self._replay(path)

    def _replay(self, path: Path) -> None:
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # torn final line from a kill mid-append
            if (not isinstance(rec, dict) or "id" not in rec
                    or not isinstance(rec.get("account_key"), str)
                    or not rec["account_key"].strip()):
                continue
            self._items[rec["id"]] = Approval(
                **{k: rec[k] for k in Approval._FIELDS if k in rec})

    def _journal(self, a: Approval) -> None:
        if self._path is not None:
            line = json.dumps(a.to_dict(), default=str) + "\n"
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a") as f:
                f.write(line)
                f.flush()
        if self._on_snapshot is not None:
            self._on_snapshot(a)

    # ---- enqueue -------------------------------------------------------

    def enqueue(self, account_key: str, connector_id: str, tool: str, args: dict, *,
                ttl_s: int, requested_by: str = "", agent_id: str = "",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        _require_account_key(account_key)
        now = time.time()
        with self._lock:
            pending = sum(1 for a in self._items.values()
                          if a.account_key == account_key and a.status == PENDING
                          and not a.is_expired(now))
            if pending >= APPROVAL_MAX_PENDING_PER_ACCOUNT:
                raise ApprovalError(
                    f"{pending} approvals already await a human decision "
                    f"(limit {APPROVAL_MAX_PENDING_PER_ACCOUNT}); resolve those "
                    f"before requesting more")
            # Full 128 bits, not a short id: `get_approval(id)` is not scoped to
            # the requesting agent. The MCP tool never checks the caller's
            # identity (nakagai/identity.py) against agent_id below, so the id
            # is the capability that keeps one agent from reading another's
            # result. (Per-agent authorization is explicitly out of scope for
            # the identity work that gave /mcp callers an identity at all; see
            # todos/mcp-workspace-scoped-data.md.)
            #
            # Deep copy on the way in: a shallow dict() leaves nested payloads
            # (e.g. {"order": {...}}) aliased to the caller's dict, which would
            # let the requester change the order after a human read it.
            a = Approval(id=uuid.uuid4().hex, account_key=account_key,
                         connector_id=connector_id, tool=tool,
                         args=copy.deepcopy(args), requested_by=requested_by,
                         created_at=now, expires_at=now + ttl_s,
                         agent_id=agent_id, signal_id=signal_id,
                         signal=copy.deepcopy(signal), notional=notional)
            self._items[a.id] = a
        self._journal(a)
        return a

    # ---- read ----------------------------------------------------------

    def get(self, account_key: str, approval_id: str) -> Approval | None:
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                return None
            expired = self._expire_locked(a)
        if expired:
            self._journal(a)
        return a

    def list(self, account_key: str, *, status: str = "",
             limit: int = 100) -> list[dict]:
        _require_account_key(account_key)
        with self._lock:
            items = [a for a in self._items.values()
                     if a.account_key == account_key]
            newly = [a for a in items if self._expire_locked(a)]
        for a in newly:
            self._journal(a)
        if status:
            items = [a for a in items if a.status == status]
        items.sort(key=lambda a: a.created_at)
        return [a.to_dict() for a in items[-limit:]]

    def history(
        self,
        account_key: str,
        *,
        statuses: tuple[str, ...],
        limit: int = 100,
        before: tuple[float, str | None] | None = None,
    ) -> list[dict]:
        _require_account_key(account_key)
        statuses, limit, before = _history_inputs(statuses, limit, before)
        with self._lock:
            owned = [
                approval for approval in self._items.values()
                if approval.account_key == account_key
            ]
            newly = [approval for approval in owned if self._expire_locked(approval)]
            rows = []
            for approval in owned:
                occurred_at = _terminal_occurrence(approval)
                if (approval.status not in statuses or approval.cleared_at
                        or not math.isfinite(occurred_at) or occurred_at <= 0):
                    continue
                key = (occurred_at, approval.id)
                if before is not None:
                    cursor_time, cursor_id = before
                    if cursor_id is None:
                        if occurred_at >= cursor_time:
                            continue
                    elif key >= (cursor_time, cursor_id):
                        continue
                rows.append((key, approval.to_dict()))
        for approval in newly:
            self._journal(approval)
        rows.sort(key=lambda row: row[0], reverse=True)
        return [row for _, row in rows[:limit]]

    def attention(
        self,
        account_key: str,
        *,
        limit: int = 100,
        after: tuple[int, float, str] | None = None,
    ) -> dict:
        _require_account_key(account_key)
        limit, after = _attention_inputs(limit, after)
        with self._lock:
            owned = [
                approval for approval in self._items.values()
                if approval.account_key == account_key
            ]
            newly = [approval for approval in owned if self._expire_locked(approval)]
            eligible = [
                approval for approval in owned
                if not approval.cleared_at and (
                    approval.status == PENDING
                    or (approval.status == ERROR and approval.outcome_unknown)
                )
            ]
            keyed = [(_attention_key(approval), approval.to_dict()) for approval in eligible]
        for approval in newly:
            self._journal(approval)
        keyed.sort(key=lambda row: row[0])
        oldest_at = min((key[1] for key, _ in keyed), default=None)
        if after is not None:
            keyed = [row for row in keyed if row[0] > after]
        return {
            "records": [row for _, row in keyed[:limit]],
            "total": len(eligible),
            "oldest_at": oldest_at,
        }

    def _expire_locked(self, a: Approval) -> bool:
        """Caller holds the lock. Returns True if this call expired the record."""
        if a.is_expired():
            a.status = EXPIRED
            a.error = "expired before a human decided"
            return True
        return False

    def expire(self, account_key: str) -> list[Approval]:
        _require_account_key(account_key)
        with self._lock:
            newly = [a for a in self._items.values()
                     if a.account_key == account_key and self._expire_locked(a)]
        for a in newly:
            self._journal(a)
        return newly

    def reconcile_stale(self, account_key: str) -> list[Approval]:
        _require_account_key(account_key)
        with self._lock:
            stale = [a for a in self._items.values()
                     if a.account_key == account_key and a.status == APPROVED]
            for a in stale:
                a.status, a.error = ERROR, (
                    "the server restarted after approval but before the call "
                    "completed; check the broker before retrying")
                a.outcome_unknown = True
        for a in stale:
            self._journal(a)
        return stale

    def clear_history(self, account_key: str) -> int:
        _require_account_key(account_key)
        now = time.time()
        with self._lock:
            # Sweep expiry first so a record that lapsed since the last read
            # counts as history, matching what the owner sees on the page.
            owned = [a for a in self._items.values()
                     if a.account_key == account_key]
            newly = [a for a in owned if self._expire_locked(a)]
            cleared = [a for a in owned
                       if a.status in TERMINAL and not a.outcome_unknown
                       and not a.cleared_at]
            for a in cleared:
                a.cleared_at = now
        for a in {id(a): a for a in (*newly, *cleared)}.values():
            self._journal(a)
        return len(cleared)

    # ---- decide --------------------------------------------------------

    def _why_unclaimable(self, account_key: str,
                         approval_id: str) -> ApprovalError:
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                return ApprovalError(f"no approval {approval_id!r}")
            if a.status == EXPIRED:
                return ApprovalError(
                    f"approval {approval_id!r} expired before a human decided")
            return ApprovalError(
                f"approval {approval_id!r} is already {a.status!r}; "
                f"it cannot be decided twice")

    def _claim(self, account_key: str, approval_id: str, decided_by: str,
               reason: str) -> Approval:
        """Compare-and-set pending -> approved. The single point where a
        concurrent second approve loses."""
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                raise ApprovalError(f"no approval {approval_id!r}")
            if self._expire_locked(a):
                raise ApprovalError(f"approval {approval_id!r} expired at "
                                    f"{time.strftime('%H:%M:%S', time.localtime(a.expires_at))}")
            if a.status != PENDING:
                raise ApprovalError(
                    f"approval {approval_id!r} is already {a.status!r}; "
                    f"it cannot be decided twice")
            a.status = APPROVED
            a.decided_at, a.decided_by, a.reason = time.time(), decided_by, reason
        self._journal(a)
        return a

    def deny(self, account_key: str, approval_id: str, *, decided_by: str = "",
             reason: str = "") -> Approval:
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                raise ApprovalError(f"no approval {approval_id!r}")
            self._expire_locked(a)
            if a.status != PENDING:
                raise ApprovalError(f"approval {approval_id!r} is already {a.status!r}")
            a.status = DENIED
            a.decided_at, a.decided_by, a.reason = time.time(), decided_by, reason
        self._journal(a)
        return a

    def _finish(self, account_key: str, a: Approval, status: str, *, result=None,
                error: str = "",
                outcome_unknown: bool = False) -> Approval:
        _require_account_key(account_key)
        with self._lock:
            live = self._items.get(a.id)
            if live is None or live.account_key != account_key:
                raise ApprovalError(f"no approval {a.id!r}")
            live.status, live.result, live.error = status, result, error
            live.outcome_unknown = outcome_unknown
            a = live
        self._journal(a)
        return a

    def _resolve(self, account_key: str, a: Approval, note: str) -> Approval:
        _require_account_key(account_key)
        with self._lock:
            live = self._items.get(a.id)
            if live is None or live.account_key != account_key:
                raise ApprovalError(f"no approval {a.id!r}")
            if not live.outcome_unknown:
                raise ApprovalError(
                    f"approval {a.id!r} is no longer awaiting reconciliation")
            live.outcome_unknown = False
            live.error = f"{live.error}; {note}".strip(" ;")
            a = live
        self._journal(a)
        return a

    def _grant(self, account_key: str, a: Approval, artifact: dict) -> Approval:
        _require_account_key(account_key)
        with self._lock:
            live = self._items.get(a.id)
            if live is None or live.account_key != account_key:
                raise ApprovalError(f"no approval {a.id!r}")
            if live.status != APPROVED:
                raise ApprovalError(f"approval {a.id!r} could not be granted")
            live.status, live.artifact = GRANTED, artifact
            a = live
        self._journal(a)
        return a

    def record_execution(self, account_key: str, approval_id: str, agent_id: str, *,
                         ok: bool,
                         result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                raise ApprovalError(f"no approval {approval_id!r}")
            if a.status != GRANTED:
                raise ApprovalError(
                    f"approval {approval_id!r} is {a.status!r}, not granted; "
                    f"only a granted approval takes an execution report")
            if a.agent_id != agent_id:
                raise ApprovalError(
                    f"approval {approval_id!r} was granted to a different agent")
            a.status = EXECUTED if ok else ERROR
            a.result, a.error, a.outcome_unknown = result, error, outcome_unknown
        self._journal(a)
        return a

    def set_rationale(self, account_key: str, approval_id: str,
                      payload: dict) -> bool:
        _require_account_key(account_key)
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or a.account_key != account_key:
                return False
            a.rationale = copy.deepcopy(payload)
        self._journal(a)
        return True

    def recent_for_symbol(self, account_key: str, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        _require_account_key(account_key)
        if not symbol:
            return []
        with self._lock:
            items = [a for a in self._items.values()
                     if a.account_key == account_key and a.id != exclude_id
                     and (a.signal or {}).get("symbol") == symbol]
        items.sort(key=lambda a: a.created_at, reverse=True)
        return [{"status": a.status, "decided_by": a.decided_by,
                 "created_at": a.created_at, "notional": a.notional}
                for a in items[:limit]]


class PgApprovalQueue(BaseApprovalQueue):
    """Postgres-backed queue with account authority on every operation."""

    COLUMNS = ("id", "account_key", "connector_id", "tool", "args", "status",
               "requested_by", "created_at", "expires_at", "decided_at",
               "decided_by", "reason", "result", "error", "outcome_unknown",
               "agent_id", "artifact", "signal_id", "signal", "notional",
               "rationale", "cleared_at")
    DB_COLUMNS = ("id", "workspace_id", "connector_id", "tool", "args", "status",
                  "requested_by", "created_at", "expires_at", "decided_at",
                  "decided_by", "reason", "result", "error", "outcome_unknown",
                  "agent_id", "artifact", "signal_id", "signal", "notional",
                  "rationale", "cleared_at")

    def __init__(self, database) -> None:
        self.db = database

    @classmethod
    def _columns(cls) -> str:
        return ", ".join(cls.DB_COLUMNS)

    @classmethod
    def _select(cls) -> str:
        return f"select {cls._columns()} from approvals"

    @staticmethod
    def _row(row) -> Approval:
        values = dict(zip(PgApprovalQueue.COLUMNS, row))
        for key in ("created_at", "expires_at", "decided_at", "cleared_at"):
            values[key] = values[key].timestamp() if values[key] is not None else 0.0
        return Approval(**values)

    @contextlib.contextmanager
    def decision_lock(self, account_key: str, timeout_s: float = LOCK_TIMEOUT_S):
        """Serialize one account's autopilot decision across API workers."""
        _require_account_key(account_key)
        with contextlib.ExitStack() as stack:
            try:
                connection = stack.enter_context(
                    self.db.pool.connection(timeout=timeout_s)
                )
                stack.enter_context(connection.transaction())
                connection.execute(
                    "select set_config('statement_timeout', %s, true)",
                    (f"{max(int(timeout_s * 1000), 1)}ms",),
                )
                locked = connection.execute(
                    "select pg_try_advisory_xact_lock("
                    "hashtext('nakagai:autopilot'), hashtext(%s::text))",
                    (account_key,),
                ).fetchone()[0]
            except Exception as error:  # noqa: BLE001
                raise DecisionLockError(f"{type(error).__name__}: {error}") from error
            if not locked:
                raise DecisionLockError(
                    "another worker is deciding an autopilot order for this account"
                )
            yield

    @classmethod
    def _expire_with(cls, connection, account_key: str) -> list[Approval]:
        rows = connection.execute(
            f"update approvals set status = 'expired',"
            f" error = 'expired before a human decided'"
            f" where workspace_id = %s and status = 'pending'"
            f" and expires_at <= now() returning {cls._columns()}",
            (account_key,),
        ).fetchall()
        return [cls._row(row) for row in rows]

    def expire(self, account_key: str) -> list[Approval]:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            return self._expire_with(connection, account_key)

    def reconcile_stale(self, account_key: str) -> list[Approval]:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            rows = connection.execute(
                f"update approvals set status = 'error', outcome_unknown = true,"
                f" error = 'the server restarted after approval but before the call completed;"
                f" check the broker before retrying'"
                f" where workspace_id = %s and status = 'approved'"
                f" and decided_at < now() - interval '10 minutes'"
                f" returning {self._columns()}",
                (account_key,),
            ).fetchall()
        return [self._row(row) for row in rows]

    def clear_history(self, account_key: str) -> int:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            with connection.transaction():
                self._expire_with(connection, account_key)
                rows = connection.execute(
                    "update approvals set cleared_at = now()"
                    " where workspace_id = %s"
                    " and status in ('denied', 'expired', 'executed', 'error')"
                    " and not outcome_unknown and cleared_at is null returning id",
                    (account_key,),
                ).fetchall()
        return len(rows)

    def enqueue(self, account_key: str, connector_id: str, tool: str, args: dict, *,
                ttl_s: int, requested_by: str = "", agent_id: str = "",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    "select pg_advisory_xact_lock("
                    "hashtext('nakagai:approval-admission'), hashtext(%s::text))",
                    (account_key,),
                )
                self._expire_with(connection, account_key)
                pending = connection.execute(
                    "select count(*) from approvals"
                    " where workspace_id = %s and status = 'pending'",
                    (account_key,),
                ).fetchone()[0]
                if pending >= APPROVAL_MAX_PENDING_PER_ACCOUNT:
                    raise ApprovalError(
                        f"{pending} approvals already await a human decision "
                        f"(limit {APPROVAL_MAX_PENDING_PER_ACCOUNT}); resolve those "
                        f"before requesting more"
                    )
                row = connection.execute(
                    f"insert into approvals (id, workspace_id, connector_id, tool, args,"
                    f" status, requested_by, expires_at, agent_id, signal_id, signal, notional)"
                    f" values (%s, %s, %s, %s, %s, 'pending', %s,"
                    f" now() + make_interval(secs => %s), %s, %s, %s, %s)"
                    f" returning {self._columns()}",
                    (uuid.uuid4().hex, account_key, connector_id, tool,
                     json.dumps(copy.deepcopy(args)), requested_by, ttl_s, agent_id,
                     signal_id,
                     json.dumps(copy.deepcopy(signal)) if signal is not None else None,
                     notional),
                ).fetchone()
        return self._row(row)

    def get(self, account_key: str, approval_id: str) -> Approval | None:
        _require_account_key(account_key)
        self.expire(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"{self._select()} where workspace_id = %s and id = %s",
                (account_key, approval_id),
            ).fetchone()
        return self._row(row) if row else None

    def list(self, account_key: str, *, status: str = "",
             limit: int = 100) -> list[dict]:
        _require_account_key(account_key)
        self.expire(account_key)
        sql = f"{self._select()} where workspace_id = %s"
        params: tuple = (account_key,)
        if status:
            sql += " and status = %s"
            params += (status,)
        sql += " order by created_at desc limit %s"
        with self.db.pool.connection() as connection:
            rows = connection.execute(sql, (*params, limit)).fetchall()
        return [self._row(row).to_dict() for row in reversed(rows)]

    def history(
        self,
        account_key: str,
        *,
        statuses: tuple[str, ...],
        limit: int = 100,
        before: tuple[float, str | None] | None = None,
    ) -> list[dict]:
        _require_account_key(account_key)
        statuses, limit, before = _history_inputs(statuses, limit, before)
        self.expire(account_key)
        occurrence = "case when status = 'expired' then expires_at else decided_at end"
        placeholders = ", ".join("%s" for _ in statuses)
        sql = (
            f"{self._select()} where workspace_id = %s"
            f" and status in ({placeholders})"
            " and cleared_at is null"
            f" and {occurrence} is not null"
        )
        params: tuple = (account_key, *statuses)
        if before is not None:
            cursor_time, cursor_id = before
            if cursor_id is None:
                sql += f" and {occurrence} < to_timestamp(%s)"
                params += (cursor_time,)
            else:
                sql += f" and ({occurrence}, id) < (to_timestamp(%s), %s)"
                params += (cursor_time, cursor_id)
        sql += f" order by {occurrence} desc, id desc limit %s"
        params += (limit,)
        with self.db.pool.connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._row(row).to_dict() for row in rows]

    def attention(
        self,
        account_key: str,
        *,
        limit: int = 100,
        after: tuple[int, float, str] | None = None,
    ) -> dict:
        _require_account_key(account_key)
        limit, after = _attention_inputs(limit, after)
        self.expire(account_key)
        group = "case when status = 'error' then 0 else 1 end"
        owed_at = (
            "case when status = 'error' then coalesce(decided_at, created_at)"
            " else created_at end"
        )
        page_columns = ", ".join(f"page.{column}" for column in self.DB_COLUMNS)
        cursor = ""
        params: tuple = (account_key,)
        if after is not None:
            cursor = (
                " where (attention_group, owed_at, id)"
                " > (%s, to_timestamp(%s), %s)"
            )
            params += after
        sql = (
            "with eligible as ("
            f" select {self._columns()}, {group} as attention_group,"
            f" {owed_at} as owed_at from approvals"
            " where workspace_id = %s and cleared_at is null"
            " and (status = 'pending' or (status = 'error' and outcome_unknown))"
            "), summary as ("
            " select count(*) as total, min(owed_at) as oldest_at from eligible"
            ")"
            f" select {page_columns}, summary.total,"
            " extract(epoch from summary.oldest_at)"
            " from summary left join lateral ("
            f" select * from eligible{cursor}"
            " order by attention_group, owed_at, id limit %s"
            ") page on true"
            " order by page.attention_group, page.owed_at, page.id"
        )
        params += (limit,)
        with self.db.pool.connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        if not rows:
            return {"records": [], "total": 0, "oldest_at": None}
        width = len(self.COLUMNS)
        total, oldest_at = rows[0][width:width + 2]
        records = [
            self._row(row[:width]).to_dict()
            for row in rows if row[0] is not None
        ]
        return {
            "records": records,
            "total": int(total),
            "oldest_at": float(oldest_at) if oldest_at is not None else None,
        }

    def _why_unclaimable(self, account_key: str,
                         approval_id: str) -> ApprovalError:
        _require_account_key(account_key)
        record = self.get(account_key, approval_id)
        if record is None:
            return ApprovalError(f"no approval {approval_id!r}")
        if record.status == EXPIRED:
            return ApprovalError(
                f"approval {approval_id!r} expired before a human decided"
            )
        return ApprovalError(
            f"approval {approval_id!r} is already {record.status!r}; "
            f"it cannot be decided twice"
        )

    def _claim(self, account_key: str, approval_id: str, decided_by: str,
               reason: str) -> Approval:
        _require_account_key(account_key)
        self.expire(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set status = 'approved', decided_at = now(),"
                f" decided_by = %s, reason = %s"
                f" where workspace_id = %s and id = %s and status = 'pending'"
                f" and expires_at > now()"
                f" returning {self._columns()}",
                (decided_by, reason, account_key, approval_id),
            ).fetchone()
        if row is None:
            raise self._why_unclaimable(account_key, approval_id)
        return self._row(row)

    def deny(self, account_key: str, approval_id: str, *, decided_by: str = "",
             reason: str = "") -> Approval:
        _require_account_key(account_key)
        self.expire(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set status = 'denied', decided_at = now(),"
                f" decided_by = %s, reason = %s"
                f" where workspace_id = %s and id = %s and status = 'pending'"
                f" and expires_at > now()"
                f" returning {self._columns()}",
                (decided_by, reason, account_key, approval_id),
            ).fetchone()
        if row is None:
            raise self._why_unclaimable(account_key, approval_id)
        return self._row(row)

    def _finish(self, account_key: str, a: Approval, status: str, *, result=None,
                error: str = "", outcome_unknown: bool = False) -> Approval:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set status = %s, result = %s, error = %s,"
                f" outcome_unknown = %s where workspace_id = %s and id = %s"
                f" returning {self._columns()}",
                (status, json.dumps(result) if result is not None else None,
                 error, outcome_unknown, account_key, a.id),
            ).fetchone()
        if row is None:
            raise ApprovalError(f"no approval {a.id!r}")
        return self._row(row)

    def _resolve(self, account_key: str, a: Approval, note: str) -> Approval:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set outcome_unknown = false,"
                f" error = trim(both ' ;' from coalesce(error, '') || '; ' || %s)"
                f" where workspace_id = %s and id = %s and outcome_unknown"
                f" returning {self._columns()}",
                (note, account_key, a.id),
            ).fetchone()
        if row is None:
            if self.get(account_key, a.id) is None:
                raise ApprovalError(f"no approval {a.id!r}")
            raise ApprovalError(
                f"approval {a.id!r} is no longer awaiting reconciliation"
            )
        return self._row(row)

    def _grant(self, account_key: str, a: Approval, artifact: dict) -> Approval:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set status = 'granted', artifact = %s"
                f" where workspace_id = %s and id = %s and status = 'approved'"
                f" returning {self._columns()}",
                (json.dumps(artifact), account_key, a.id),
            ).fetchone()
        if row is None:
            if self.get(account_key, a.id) is None:
                raise ApprovalError(f"no approval {a.id!r}")
            raise ApprovalError(f"approval {a.id!r} could not be granted")
        return self._row(row)

    def record_execution(self, account_key: str, approval_id: str, agent_id: str, *,
                         ok: bool, result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                f"update approvals set status = %s, result = %s, error = %s,"
                f" outcome_unknown = %s where workspace_id = %s and id = %s"
                f" and status = 'granted' and agent_id = %s"
                f" returning {self._columns()}",
                (EXECUTED if ok else ERROR,
                 json.dumps(result) if result is not None else None,
                 error, outcome_unknown, account_key, approval_id, agent_id),
            ).fetchone()
        if row is None:
            record = self.get(account_key, approval_id)
            if record is None:
                raise ApprovalError(f"no approval {approval_id!r}")
            if record.status != GRANTED:
                raise ApprovalError(
                    f"approval {approval_id!r} is {record.status!r}, not granted; "
                    f"only a granted approval takes an execution report"
                )
            if record.agent_id != agent_id:
                raise ApprovalError(
                    f"approval {approval_id!r} was granted to a different agent"
                )
            raise ApprovalError(
                f"approval {approval_id!r} is {record.status!r}, not granted"
            )
        return self._row(row)

    def set_rationale(self, account_key: str, approval_id: str,
                      payload: dict) -> bool:
        _require_account_key(account_key)
        with self.db.pool.connection() as connection:
            row = connection.execute(
                "update approvals set rationale = %s"
                " where workspace_id = %s and id = %s returning id",
                (json.dumps(payload), account_key, approval_id),
            ).fetchone()
        return row is not None

    def recent_for_symbol(self, account_key: str, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        _require_account_key(account_key)
        if not symbol:
            return []
        with self.db.pool.connection() as connection:
            rows = connection.execute(
                "select status, decided_by, created_at, notional from approvals"
                " where workspace_id = %s and signal->>'symbol' = %s and id <> %s"
                " order by created_at desc limit %s",
                (account_key, symbol, exclude_id, limit),
            ).fetchall()
        return [
            {
                "status": row[0],
                "decided_by": row[1],
                "created_at": row[2].timestamp() if row[2] is not None else 0.0,
                "notional": row[3],
            }
            for row in rows
        ]
