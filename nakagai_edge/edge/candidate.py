"""Crash-safe local state for one bounded execution-candidate wake."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime

from nakagai_edge.edge.state import EdgeState

CANDIDATE_WAKE_MAX_S = 900.0


def _deadline(value) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


class CandidateWakeScope:
    """One listener-owned candidate write boundary shared with the daemon.

    The listener writes the scope before it starts the wake command and removes
    it after the command exits. The candidate deadline and a local maximum make
    a file left by a crashed listener expire without trusting cleanup.
    """

    def __init__(self, state: EdgeState) -> None:
        self.state = state

    def begin(self, event: dict) -> str | None:
        now = time.time()
        expires_at = _deadline(event.get("expires_at"))
        candidate_id = event.get("candidate_id")
        if (event.get("kind") != "execution_candidate"
                or event.get("response_required") is not True
                or not isinstance(candidate_id, str) or not candidate_id.strip()
                or expires_at is None or expires_at <= now):
            return None
        token = uuid.uuid4().hex
        self.state._write_private(self.state.candidate_wake_path, {
            "token": token,
            "candidate_id": candidate_id.strip(),
            "seq": event.get("seq"),
            "expires_at": min(expires_at, now + CANDIDATE_WAKE_MAX_S),
        })
        return token

    def current(self) -> dict | None:
        path = self.state.candidate_wake_path
        try:
            doc = json.loads(path.read_text())
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return self._corrupt_current(path)
        if (not isinstance(doc, dict)
                or not isinstance(doc.get("candidate_id"), str)
                or not doc["candidate_id"].strip()):
            return self._corrupt_current(path)
        try:
            expired = float(doc.get("expires_at", 0)) <= time.time()
        except (ValueError, TypeError):
            return self._corrupt_current(path)
        if expired:
            path.unlink(missing_ok=True)
            return None
        return {"candidate_id": doc["candidate_id"], "seq": doc.get("seq")}

    @staticmethod
    def _corrupt_current(path) -> dict | None:
        """Fail closed after torn state, with mtime as the bounded deadline."""
        try:
            expires_at = path.stat().st_mtime + CANDIDATE_WAKE_MAX_S
        except OSError:
            return {"candidate_id": None, "seq": None}
        if expires_at > time.time():
            return {"candidate_id": None, "seq": None}
        path.unlink(missing_ok=True)
        return None

    def finish(self, token: str | None) -> None:
        if not token:
            return
        try:
            doc = json.loads(self.state.candidate_wake_path.read_text())
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return
        if isinstance(doc, dict) and doc.get("token") == token:
            self.state.candidate_wake_path.unlink(missing_ok=True)


def pending_candidate_outcomes(state: EdgeState) -> dict:
    if not state.candidate_outcomes_path.exists():
        return {}
    try:
        doc = json.loads(state.candidate_outcomes_path.read_text())
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return doc if isinstance(doc, dict) else {}


def _save_candidate_outcomes(state: EdgeState, doc: dict) -> None:
    state._write_private(state.candidate_outcomes_path, doc)


def deliver_candidate_outcome(
        state: EdgeState, client, candidate_id: str, *,
        mechanical_status: str, mechanical_reason: str,
        approval_id: str = "", urgent: bool = False,
        outcome_unknown: bool = False) -> bool:
    payload = {
        "mechanical_status": mechanical_status,
        "mechanical_reason": str(mechanical_reason).strip()[:1000],
        "approval_id": approval_id,
        "urgent": bool(urgent),
        "outcome_unknown": bool(outcome_unknown),
    }
    pending = pending_candidate_outcomes(state)
    pending[candidate_id] = payload
    _save_candidate_outcomes(state, pending)
    try:
        acknowledged = client.report_candidate_outcome(candidate_id, **payload)
    except Exception:  # noqa: BLE001 (the durable row is retried by the executor)
        return False
    if acknowledged.get("ok") is not True:
        return False
    current = pending_candidate_outcomes(state)
    if current.get(candidate_id) == payload:
        del current[candidate_id]
        _save_candidate_outcomes(state, current)
    return True


def flush_candidate_outcomes(state: EdgeState, client) -> int:
    delivered = 0
    for candidate_id, payload in list(pending_candidate_outcomes(state).items()):
        if not isinstance(payload, dict):
            continue
        if deliver_candidate_outcome(state, client, candidate_id, **payload):
            delivered += 1
    return delivered


def candidate_entries_armed(state: EdgeState) -> bool:
    return not state.candidate_entries_off_path.exists()


def disarm_candidate_entries(
        state: EdgeState, *, candidate_id: str, approval_id: str,
        reason: str) -> None:
    state._write_private(state.candidate_entries_off_path, {
        "disarmed": True,
        "candidate_id": candidate_id,
        "approval_id": approval_id,
        "reason": str(reason).strip()[:1000],
        "at": time.time(),
    })


def rearm_candidate_entries(state: EdgeState) -> None:
    state.candidate_entries_off_path.unlink(missing_ok=True)
