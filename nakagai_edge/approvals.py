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

# A runaway agent must not be able to fill the disk or memory with pending
# approvals. Enqueue past this and the request is refused outright.
MAX_PENDING = 100

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


@dataclass
class Approval:
    id: str
    connector_id: str
    tool: str
    args: dict
    status: str = PENDING
    requested_by: str = ""
    workspace: str = "default"
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

    _FIELDS = ("id", "connector_id", "tool", "args", "status", "requested_by",
               "workspace", "created_at", "expires_at", "decided_at", "decided_by",
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
    def decision_lock(self, workspace: str, timeout_s: float = LOCK_TIMEOUT_S):
        """Serialize autopilot's read → check → claim for one workspace, in principle.
        See `PgApprovalQueue.decision_lock` for the caveat that matters in the
        deployment that actually overrides this: with the wiring `api/app.py`
        uses, "one workspace" does not describe production.

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
        yield

    def _claim(self, approval_id: str, decided_by: str, reason: str) -> Approval:
        """Atomically move pending → approved. Raise if it is not claimable."""
        raise NotImplementedError

    def _finish(self, a: Approval, status: str, *, result=None, error: str = "",
                outcome_unknown: bool = False) -> Approval:
        """Record the terminal state of an approved record."""
        raise NotImplementedError

    def _resolve(self, a: Approval, note: str) -> Approval:
        """Clear `outcome_unknown` and append the human's finding to `error`."""
        raise NotImplementedError

    def set_rationale(self, approval_id: str, payload: dict) -> bool:
        """Attach the copilot read. Advisory only, so unlike every decision
        method this never raises for a missing row: the generator runs in a
        fire-and-forget thread with nobody to catch."""
        raise NotImplementedError

    def clear_history(self) -> int:
        """Stamp `cleared_at` on every decided, reconciled record that lacks one,
        hiding it from the owner's History view. Returns how many were stamped.

        Never touches pending records, and never touches `outcome_unknown` ones:
        an unreconciled record is the only thing telling agents not to resubmit
        an order that may be live at the broker, so it stays visible until a
        human resolves it. A hide, not a delete: `list()` keeps returning
        cleared records because budgets and reconciliation scans read it."""
        raise NotImplementedError

    def recent_for_symbol(self, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        """Recent approvals citing a signal for `symbol`, newest first. Feeds
        the copilot read only ("third NVDA proposal today, last two denied"),
        so the shape is a compact display dict, not an Approval.

        Like `PgApprovalQueue.list()`, this reads with no workspace filter:
        safe under today's single-tenant wiring, but it is not per-tenant
        isolation, and it must be scoped in the same pass that scopes
        `list()` before multi-tenant is safe to open."""
        raise NotImplementedError

    def resolve(self, approval_id: str, *, placed: bool, note: str = "",
                resolved_by: str = "") -> Approval:
        """Record what a human found at the broker for an `outcome_unknown` call.

        Nakagai cannot know whether a timed-out `place_*` reached the broker, so
        it never guesses. A person checks the account (e.g. `get_equity_orders`)
        and records the answer here. Only then does the record stop warning
        agents off, and only a human's word clears it.

        `placed=True` means the order IS live at the broker; `placed=False` means
        it is not, so the action may be requested again.
        """
        a = self.get(approval_id)
        if a is None:
            raise ApprovalError(f"no approval {approval_id!r}")
        if not a.outcome_unknown:
            raise ApprovalError(
                f"approval {approval_id!r} is not awaiting reconciliation "
                f"(status={a.status!r}, outcome_unknown=False)")
        verdict = "REACHED the broker" if placed else "did NOT reach the broker"
        who = f" by {resolved_by}" if resolved_by else ""
        stamp = f"reconciled{who}: the call {verdict}."
        return self._resolve(a, f"{stamp} {note}".strip())

    async def approve(self, approval_id: str, execute, *, decided_by: str = "",
                      reason: str = "") -> Approval:
        """Approve and execute, once.

        `execute(connector_id, tool, args) -> dict` runs the real downstream call
        and must re-check the connector's guardrails against current config.
        Everything after `_claim` is on the far side of the compare-and-set, so a
        second concurrent approve raises instead of placing a second order.
        """
        a = self._claim(approval_id, decided_by, reason)
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
            return self._finish(a, ERROR, error=f"{type(e).__name__}: {e}",
                                outcome_unknown=unknown)
        try:
            return self._finish(a, EXECUTED, result=result)
        except Exception as e:  # noqa: BLE001 - the order already FILLED
            # The write left Nakagai and the broker took it. A failure to RECORD
            # that is not a failure to EXECUTE, and reporting it as one is the
            # worst lie this queue can tell: the agent sees a crash for an order
            # that filled, and an agent that retries on a crash doubles the
            # position.
            #
            # So we return the truth in memory. The stored record stays `approved`
            # and `reconcile_stale_approvals()` sweeps it to outcome_unknown, where
            # a human resolves it against the broker, which is the correct
            # conservative end state: the owner is ASKED to check, never TOLD a
            # falsehood.
            #
            # That in-memory return is silent on its own, though: nobody else
            # learns this happened until the next process boot runs
            # `reconcile_stale_approvals()` (see `_install_approval_queue()` in
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

    async def grant(self, approval_id: str, build_artifact, *,
                    decided_by: str = "", reason: str = "") -> Approval:
        """Approve an EDGE-origin request: claim it (same CAS as approve), then
        record the signed artifact instead of executing. The platform holds no
        broker credentials, so execution happens at the edge, which reports
        back via record_execution()."""
        a = self._claim(approval_id, decided_by, reason)
        return self._grant(a, build_artifact(a))

    def _grant(self, a: Approval, artifact: dict) -> Approval:
        raise NotImplementedError

    def record_execution(self, approval_id: str, agent_id: str, *, ok: bool,
                         result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
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
            if not isinstance(rec, dict) or "id" not in rec:
                continue
            self._items[rec["id"]] = Approval(
                **{k: rec[k] for k in Approval._FIELDS if k in rec})

        # A record left `approved` means we died between the human's click and
        # the downstream call. We cannot know whether the broker saw it. Do NOT
        # execute it now. Surface it for a human to reconcile.
        #
        # GRANTED rows are deliberately NOT swept: the edge holds the signed
        # artifact and reports the outcome via record_execution(). Only
        # `approved` (platform-executes, died mid-call) is outcome-unknown.
        for a in self._items.values():
            if a.status == APPROVED:
                a.status, a.error = ERROR, (
                    "the server restarted after approval but before the call "
                    "completed; check the broker before retrying")
                a.outcome_unknown = True
                self._journal(a)

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

    def enqueue(self, connector_id: str, tool: str, args: dict, *, ttl_s: int,
                requested_by: str = "", workspace: str = "default", agent_id: str = "",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        now = time.time()
        with self._lock:
            pending = sum(1 for a in self._items.values()
                          if a.status == PENDING and not a.is_expired(now))
            if pending >= MAX_PENDING:
                raise ApprovalError(
                    f"{pending} approvals already await a human decision "
                    f"(limit {MAX_PENDING}); resolve those before requesting more")
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
            a = Approval(id=uuid.uuid4().hex, connector_id=connector_id, tool=tool,
                         args=copy.deepcopy(args), requested_by=requested_by,
                         workspace=workspace, created_at=now, expires_at=now + ttl_s,
                         agent_id=agent_id, signal_id=signal_id,
                         signal=copy.deepcopy(signal), notional=notional)
            self._items[a.id] = a
        self._journal(a)
        return a

    # ---- read ----------------------------------------------------------

    def get(self, approval_id: str, workspace: str | None = None) -> Approval | None:
        with self._lock:
            a = self._items.get(approval_id)
            if a is None or (workspace is not None and a.workspace != workspace):
                return None
            expired = self._expire_locked(a)
        if expired:
            self._journal(a)
        return a

    def list(self, status: str = "", workspace: str | None = None,
             limit: int = 100) -> list[dict]:
        with self._lock:
            items = list(self._items.values())
            newly = [a for a in items if self._expire_locked(a)]
        for a in newly:
            self._journal(a)
        if workspace is not None:
            items = [a for a in items if a.workspace == workspace]
        if status:
            items = [a for a in items if a.status == status]
        items.sort(key=lambda a: a.created_at)
        return [a.to_dict() for a in items[-limit:]]

    def _expire_locked(self, a: Approval) -> bool:
        """Caller holds the lock. Returns True if this call expired the record."""
        if a.is_expired():
            a.status = EXPIRED
            a.error = "expired before a human decided"
            return True
        return False

    def clear_history(self) -> int:
        now = time.time()
        with self._lock:
            # Sweep expiry first so a record that lapsed since the last read
            # counts as history, matching what the owner sees on the page.
            newly = [a for a in self._items.values() if self._expire_locked(a)]
            cleared = [a for a in self._items.values()
                       if a.status != PENDING and not a.outcome_unknown
                       and not a.cleared_at]
            for a in cleared:
                a.cleared_at = now
        for a in {id(a): a for a in (*newly, *cleared)}.values():
            self._journal(a)
        return len(cleared)

    # ---- decide --------------------------------------------------------

    def _claim(self, approval_id: str, decided_by: str, reason: str) -> Approval:
        """Compare-and-set pending -> approved. The single point where a
        concurrent second approve loses."""
        with self._lock:
            a = self._items.get(approval_id)
            if a is None:
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

    def deny(self, approval_id: str, decided_by: str = "", reason: str = "") -> Approval:
        with self._lock:
            a = self._items.get(approval_id)
            if a is None:
                raise ApprovalError(f"no approval {approval_id!r}")
            self._expire_locked(a)
            if a.status != PENDING:
                raise ApprovalError(f"approval {approval_id!r} is already {a.status!r}")
            a.status = DENIED
            a.decided_at, a.decided_by, a.reason = time.time(), decided_by, reason
        self._journal(a)
        return a

    def _finish(self, a: Approval, status: str, *, result=None, error: str = "",
                outcome_unknown: bool = False) -> Approval:
        with self._lock:
            a.status, a.result, a.error = status, result, error
            a.outcome_unknown = outcome_unknown
        self._journal(a)
        return a

    def _resolve(self, a: Approval, note: str) -> Approval:
        with self._lock:
            live = self._items.get(a.id, a)
            live.outcome_unknown = False
            live.error = f"{live.error}; {note}".strip(" ;")
            a = live
        self._journal(a)
        return a

    def _grant(self, a: Approval, artifact: dict) -> Approval:
        with self._lock:
            a.status, a.artifact = GRANTED, artifact
        self._journal(a)
        return a

    def record_execution(self, approval_id: str, agent_id: str, *, ok: bool,
                         result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
        with self._lock:
            a = self._items.get(approval_id)
            if a is None:
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

    def set_rationale(self, approval_id: str, payload: dict) -> bool:
        with self._lock:
            a = self._items.get(approval_id)
            if a is None:
                return False
            a.rationale = copy.deepcopy(payload)
        self._journal(a)
        return True

    def recent_for_symbol(self, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        if not symbol:
            return []
        with self._lock:
            items = [a for a in self._items.values()
                     if a.id != exclude_id
                     and (a.signal or {}).get("symbol") == symbol]
        items.sort(key=lambda a: a.created_at, reverse=True)
        return [{"status": a.status, "decided_by": a.decided_by,
                 "created_at": a.created_at, "notional": a.notional}
                for a in items[:limit]]


class PgApprovalQueue(BaseApprovalQueue):
    """Postgres-backed. The compare-and-set is `claim_approval()` in
    `ops/db/0002_gateway.sql`, so two workers cannot both approve one record,
    which is what makes running more than one API worker safe.

    Durable across redeploys, unlike `results/approvals.jsonl` on ephemeral disk.
    """

    COLUMNS = ("id", "connector_id", "tool", "args", "status", "requested_by",
               "created_at", "expires_at", "decided_at", "decided_by", "reason",
               "result", "error", "outcome_unknown", "agent_id", "artifact",
               "signal_id", "signal", "notional", "rationale", "cleared_at")

    def __init__(self, database, workspace_id: str | None = None) -> None:
        self.db = database
        self.workspace_id = workspace_id

    # ---- the autopilot decision lock ---------------------------------------

    @contextlib.contextmanager
    def decision_lock(self, workspace: str, timeout_s: float = LOCK_TIMEOUT_S):
        """Serialize autopilot's read → check → claim across API workers (see the
        base class for why the CAS alone is not enough).

        **Read this first, before the mechanism below:** with the wiring
        `nakagai/api/app.py` actually uses (`PgApprovalQueue(database)`, no
        `workspace_id`), the key this locks on degenerates to a single global
        `"default"` (see `_lock_key`), so in production TODAY this is one global
        lock, not one lock per workspace. Every autopilot decision, in every
        workspace, serializes against every other one. That is safe (it can only
        make a decision decline to a human tap, never let two through) but it is
        not per-tenant isolation, whatever the per-workspace framing below
        suggests. Read `_lock_key`'s docstring in full before changing that: making
        the lock per-workspace while `PgApprovalQueue.list()` still reads the whole
        table with no workspace filter would let two workspaces take two different
        locks and both read the same global budget, reintroducing the exact cap
        overshoot this lock exists to prevent.

        **Transaction-scoped** (`pg_try_advisory_xact_lock`, never the session-scoped
        `pg_advisory_lock`): Postgres releases it at COMMIT/ROLLBACK, and if the
        worker dies, when the backend goes away. A session-scoped lock outliving a
        dying worker would wedge autopilot for that workspace permanently, a far
        worse failure than declining one order.

        **TRY, not wait.** A blocking `pg_advisory_xact_lock` would be a *synchronous*
        wait inside an async request: the lock-holder awaits the broker (seconds) with
        the lock held, so a second concurrent decision in the same process would block
        the event loop, and therefore block the very task it is waiting on. Trying
        and giving up cannot deadlock, and costs nothing real: autopilot decides at most
        `daily_order_max` times a day (default 5), so contention is vanishingly rare,
        and losing the race merely means that order waits for a human tap.

        The key is DERIVED from the workspace (`_lock_key`), but see that docstring
        and the caveat at the top of this one before reading "one owner's decision
        never blocks another's" as a description of production today: with the
        wiring `api/app.py` actually uses, it degenerates to one global key, and it
        must stay that way until `list()` is scoped to match. `hashtext` is
        Postgres's own string hash (int4); the two-key form namespaces it under
        `hashtext('nakagai:autopilot')`, so this cannot collide with an advisory lock
        anyone later takes on a bare workspace hash. A hash collision between two
        workspaces could only make one of them decline, never let two decisions
        through, which is the direction that must not fail.

        The lock is held on its own pooled connection for the body of the `with`. It
        does not have to be the connection the reads and the claim run on: an advisory
        lock serializes against other holders of the same key, and this key has
        exactly one taker: the auto-approver.

        Raises `DecisionLockError` if the lock is held elsewhere, or if the database
        cannot be reached. The caller declines; it must never execute without it.
        """
        with contextlib.ExitStack() as stack:
            # Only ACQUISITION failures become DecisionLockError. The body runs
            # outside this try on purpose: wrapping it too would rewrite a broker
            # error (or an ApprovalError from the CAS) into "could not take the
            # lock", and the caller reasons about those very differently.
            try:
                # `timeout` bounds the wait for a free pooled connection; an
                # exhausted pool or an unreachable database must decline, not hang.
                c = stack.enter_context(self.db.pool.connection(timeout=timeout_s))
                # The pool is autocommit; transaction() issues an explicit BEGIN, so
                # the *xact*-scoped lock survives until this block ends. Without it
                # each statement is its own transaction and the lock would be
                # released before the caller had done a thing.
                stack.enter_context(c.transaction())
                # set_config(_, _, is_local => true), not `SET LOCAL`: `SET` is a
                # utility statement and takes no bind parameters, so `set local
                # statement_timeout = %s` is a syntax error over the extended
                # protocol. set_config() is an ordinary function and does take them.
                # Transaction-local, so it reverts with this transaction.
                c.execute("select set_config('statement_timeout', %s, true)",
                          (f"{max(int(timeout_s * 1000), 1)}ms",))
                got = c.execute(
                    "select pg_try_advisory_xact_lock("
                    "hashtext('nakagai:autopilot'), hashtext(%s::text))",
                    (self._lock_key(workspace),)).fetchone()[0]
            except Exception as e:  # noqa: BLE001 (fail closed: the caller declines)
                # The CAUSE, not a sentence: the caller (autoapprove.py) owns the
                # owner-facing decline reason and wraps this.
                raise DecisionLockError(f"{type(e).__name__}: {e}") from e
            if not got:
                raise DecisionLockError(
                    "another worker is deciding an autopilot order for this workspace")
            yield
            # Exit: COMMIT (or ROLLBACK if the body raised) ends the transaction,
            # which is what releases the lock. It cannot leak past this block, and
            # cannot outlive the process.

    def _lock_key(self, workspace: str) -> str:
        """What this queue's autopilot decisions serialize against. `workspace_id` is
        authoritative when set (it is the uuid every row of this queue is scoped
        to), and the caller's workspace name is the fallback.

        Be clear about what that means TODAY: `nakagai/api/app.py` builds
        `PgApprovalQueue(database)` with no workspace_id, and `COLUMNS` does not
        select the row's `workspace_id`, so `Approval.workspace` is always its
        default. The key therefore degenerates to one global 'default': every
        workspace's autopilot decisions serialize against each other.

        That is SAFE: over-serializing can never let two decisions through, only
        make one of them decline to a human tap, and a decision holds the lock for a
        single broker round-trip a handful of times a day. It is not per-tenant
        isolation, and no comment here should pretend it is.

        Making this per-workspace is NOT a one-line change here. `list()`, the read
        that computes the day's cap usage and the once-per-signal fence in
        `autoapprove.py`, has no workspace filter (see `PgApprovalQueue.list()`
        below and `_select()`); it reads every row in the table regardless of
        `workspace`. Scoping `list()` to the workspace is a PREREQUISITE, not an
        afterthought: pass a `workspace_id` here without also scoping `list()` and
        two workspaces take two *different* locks while both still read the *same*
        global budget: both can pass `daily_order_max` and both claim, which is
        exactly the cap overshoot this lock was built to prevent. Only once `list()`
        (and the budget/fence read it feeds) is workspace-scoped does keying the
        lock per-workspace become safe to do on its own.
        """
        return self.workspace_id or workspace or "default"

    # ---- row <-> Approval ----------------------------------------------

    @staticmethod
    def _row(row) -> Approval:
        d = dict(zip(PgApprovalQueue.COLUMNS, row))
        # Postgres hands back datetimes; the dataclass and the JSON contract
        # both speak epoch seconds.
        for k in ("created_at", "expires_at", "decided_at", "cleared_at"):
            d[k] = d[k].timestamp() if d[k] is not None else 0.0
        return Approval(**d)

    def _select(self) -> str:
        return f"select {', '.join(self.COLUMNS)} from approvals"

    # ---- lifecycle -------------------------------------------------------

    def reconcile_stale(self) -> list[Approval]:
        """Turn abandoned `approved` rows into `error` + `outcome_unknown`.

        Only rows older than the interval in SQL (10 min), because a row approved
        seconds ago may be executing right now in a sibling worker.
        """
        with self.db.pool.connection() as c:
            # Name the columns: the function returns `setof approvals`, i.e. every
            # table column in table order (including workspace_id), which does not
            # match COLUMNS.
            rows = c.execute(
                f"select {', '.join(self.COLUMNS)} from reconcile_stale_approvals()"
            ).fetchall()
        return [self._row(r) for r in rows]

    def _expire(self) -> None:
        with self.db.pool.connection() as c:
            c.execute("select expire_approvals()")

    def clear_history(self) -> int:
        self._expire()
        with self.db.pool.connection() as c:
            rows = c.execute(
                "update approvals set cleared_at = now()"
                " where status <> 'pending' and not outcome_unknown"
                " and cleared_at is null returning id").fetchall()
        return len(rows)

    def enqueue(self, connector_id: str, tool: str, args: dict, *, ttl_s: int,
                requested_by: str = "", workspace: str = "default", agent_id: str = "",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        self._expire()
        with self.db.pool.connection() as c:
            pending = c.execute(
                "select count(*) from approvals where status = 'pending'").fetchone()[0]
            if pending >= MAX_PENDING:
                raise ApprovalError(
                    f"{pending} approvals already await a human decision "
                    f"(limit {MAX_PENDING}); resolve those before requesting more")
            row = c.execute(
                f"insert into approvals (id, workspace_id, connector_id, tool, args,"
                f" status, requested_by, expires_at, agent_id,"
                f" signal_id, signal, notional)"
                f" values (%s, %s, %s, %s, %s, 'pending', %s,"
                f" now() + make_interval(secs => %s), %s, %s, %s, %s)"
                f" returning {', '.join(self.COLUMNS)}",
                (uuid.uuid4().hex, self.workspace_id, connector_id, tool,
                 json.dumps(copy.deepcopy(args)), requested_by, ttl_s, agent_id,
                 signal_id,
                 json.dumps(copy.deepcopy(signal)) if signal is not None else None,
                 notional)).fetchone()
        return self._row(row)

    def get(self, approval_id: str, workspace: str | None = None) -> Approval | None:
        self._expire()
        with self.db.pool.connection() as c:
            row = c.execute(f"{self._select()} where id = %s", (approval_id,)).fetchone()
        return self._row(row) if row else None

    def list(self, status: str = "", workspace: str | None = None,
             limit: int = 100) -> list[dict]:
        self._expire()
        sql = self._select()
        params: tuple = ()
        if status:
            sql += " where status = %s"
            params = (status,)
        # NEWEST `limit`, returned oldest-first: the same contract as
        # ApprovalQueue.list (which sorts ascending and slices `[-limit:]`).
        # `asc limit N` would return the OLDEST N instead, which is not merely a
        # cosmetic difference for the UI: autoapprove.py computes the day's autopilot
        # budget and looks for unreconciled (`outcome_unknown`) calls from this very
        # list, so on a table with more than N rows it would see nothing from today.
        # The daily caps would silently stop binding, and only on Postgres.
        sql += " order by created_at desc limit %s"
        with self.db.pool.connection() as c:
            rows = c.execute(sql, (*params, limit)).fetchall()
        return [self._row(r).to_dict() for r in reversed(rows)]

    # ---- decisions -------------------------------------------------------

    def _why_unclaimable(self, approval_id: str) -> ApprovalError:
        """The claim returned no row. Say which of the three reasons it was."""
        a = self.get(approval_id)
        if a is None:
            return ApprovalError(f"no approval {approval_id!r}")
        if a.status == EXPIRED:
            return ApprovalError(f"approval {approval_id!r} expired before a human decided")
        return ApprovalError(f"approval {approval_id!r} is already {a.status!r}; "
                             f"it cannot be decided twice")

    def _claim(self, approval_id: str, decided_by: str, reason: str) -> Approval:
        self._expire()
        with self.db.pool.connection() as c:
            # Name the columns: claim_approval() returns `setof approvals`
            # (table order, workspace_id included), not this class's COLUMNS.
            row = c.execute(
                f"select {', '.join(self.COLUMNS)} from claim_approval(%s, %s, %s)",
                (approval_id, decided_by, reason)).fetchone()
        if row is None:
            raise self._why_unclaimable(approval_id)
        return self._row(row)

    def deny(self, approval_id: str, decided_by: str = "", reason: str = "") -> Approval:
        self._expire()
        with self.db.pool.connection() as c:
            row = c.execute(
                f"update approvals set status = 'denied', decided_at = now(),"
                f" decided_by = %s, reason = %s"
                f" where id = %s and status = 'pending'"
                f" returning {', '.join(self.COLUMNS)}",
                (decided_by, reason, approval_id)).fetchone()
        if row is None:
            raise self._why_unclaimable(approval_id)
        return self._row(row)

    def _finish(self, a: Approval, status: str, *, result=None, error: str = "",
                outcome_unknown: bool = False) -> Approval:
        with self.db.pool.connection() as c:
            row = c.execute(
                f"update approvals set status = %s, result = %s, error = %s,"
                f" outcome_unknown = %s where id = %s"
                f" returning {', '.join(self.COLUMNS)}",
                (status, json.dumps(result) if result is not None else None,
                 error, outcome_unknown, a.id)).fetchone()
        return self._row(row)

    def _resolve(self, a: Approval, note: str) -> Approval:
        with self.db.pool.connection() as c:
            row = c.execute(
                f"update approvals set outcome_unknown = false,"
                f" error = trim(both ' ;' from coalesce(error, '') || '; ' || %s)"
                f" where id = %s and outcome_unknown"
                f" returning {', '.join(self.COLUMNS)}",
                (note, a.id)).fetchone()
        if row is None:  # another approver reconciled it first
            raise ApprovalError(
                f"approval {a.id!r} is no longer awaiting reconciliation")
        return self._row(row)

    def _grant(self, a: Approval, artifact: dict) -> Approval:
        with self.db.pool.connection() as c:
            row = c.execute(
                f"update approvals set status = 'granted', artifact = %s"
                f" where id = %s and status = 'approved'"
                f" returning {', '.join(self.COLUMNS)}",
                (json.dumps(artifact), a.id)).fetchone()
        if row is None:  # claim raced with something that moved it off approved
            raise ApprovalError(f"approval {a.id!r} could not be granted")
        return self._row(row)

    def record_execution(self, approval_id: str, agent_id: str, *, ok: bool,
                         result=None, error: str = "",
                         outcome_unknown: bool = False) -> Approval:
        with self.db.pool.connection() as c:
            row = c.execute(
                f"update approvals set status = %s, result = %s, error = %s,"
                f" outcome_unknown = %s"
                f" where id = %s and status = 'granted' and agent_id = %s"
                f" returning {', '.join(self.COLUMNS)}",
                (EXECUTED if ok else ERROR,
                 json.dumps(result) if result is not None else None,
                 error, outcome_unknown, approval_id, agent_id)).fetchone()
        if row is None:
            a = self.get(approval_id)
            if a is None:
                raise ApprovalError(f"no approval {approval_id!r}")
            if a.status != GRANTED:
                raise ApprovalError(
                    f"approval {approval_id!r} is {a.status!r}, not granted; "
                    f"only a granted approval takes an execution report")
            if a.agent_id != agent_id:
                raise ApprovalError(
                    f"approval {approval_id!r} was granted to a different agent")
            raise ApprovalError(
                f"approval {approval_id!r} is {a.status!r}, not granted")
        return self._row(row)

    def set_rationale(self, approval_id: str, payload: dict) -> bool:
        with self.db.pool.connection() as c:
            row = c.execute(
                "update approvals set rationale = %s where id = %s returning id",
                (json.dumps(payload), approval_id)).fetchone()
        return row is not None

    def recent_for_symbol(self, symbol: str, *, exclude_id: str = "",
                          limit: int = 5) -> list[dict]:
        if not symbol:
            return []
        with self.db.pool.connection() as c:
            rows = c.execute(
                "select status, decided_by, created_at, notional from approvals"
                " where signal->>'symbol' = %s and id <> %s"
                " order by created_at desc limit %s",
                (symbol, exclude_id, limit)).fetchall()
        return [{"status": r[0], "decided_by": r[1],
                 "created_at": r[2].timestamp() if r[2] is not None else 0.0,
                 "notional": r[3]} for r in rows]
