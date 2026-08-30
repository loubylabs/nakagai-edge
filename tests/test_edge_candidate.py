import json

from nakagai_edge.edge.candidate import (
    candidate_entries_armed,
    deliver_candidate_outcome,
    disarm_candidate_entries,
    flush_candidate_outcomes,
    pending_candidate_outcomes,
)
from nakagai_edge.edge.state import EdgeState


class _Client:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def report_candidate_outcome(self, candidate_id, **payload):
        self.calls.append((candidate_id, payload))
        if self.fail:
            raise RuntimeError("platform down")
        return {
            "candidate_id": candidate_id,
            "mechanical_status": payload["mechanical_status"],
            "approval_id": payload["approval_id"],
        }


def test_candidate_outcome_stays_durable_until_platform_acknowledges_it(tmp_path):
    state = EdgeState(tmp_path)
    payload = {
        "mechanical_status": "blocked",
        "mechanical_reason": "local policy stale",
        "approval_id": "approval-1",
        "urgent": False,
        "outcome_unknown": False,
    }

    assert deliver_candidate_outcome(
        state, _Client(fail=True), "candidate-1", **payload) is False
    assert pending_candidate_outcomes(state) == {"candidate-1": payload}

    client = _Client()
    assert flush_candidate_outcomes(state, client) == 1
    assert client.calls == [("candidate-1", payload)]
    assert pending_candidate_outcomes(state) == {}


def test_terminal_candidate_response_acknowledges_the_durable_outcome(tmp_path):
    state = EdgeState(tmp_path)
    payload = {
        "mechanical_status": "submitted",
        "mechanical_reason": "broker fill reconciled and supervision verified",
        "approval_id": "approval-1",
        "urgent": False,
        "outcome_unknown": False,
    }

    class TerminalClient:
        def report_candidate_outcome(self, candidate_id, **reported):
            assert candidate_id == "candidate-1"
            assert reported == payload
            return {
                "candidate_id": candidate_id,
                "decision": "accepted",
                "mechanical_status": reported["mechanical_status"],
                "mechanical_reason": reported["mechanical_reason"],
                "approval_id": reported["approval_id"],
            }

    assert deliver_candidate_outcome(
        state, TerminalClient(), "candidate-1", **payload) is True
    assert pending_candidate_outcomes(state) == {}


def test_candidate_entry_disarm_is_persistent_and_fails_closed(tmp_path):
    state = EdgeState(tmp_path)
    assert candidate_entries_armed(state) is True

    disarm_candidate_entries(
        state, candidate_id="candidate-1", approval_id="approval-1",
        reason="supervision could not be verified",
    )

    assert candidate_entries_armed(state) is False
    doc = json.loads(state.candidate_entries_off_path.read_text())
    assert doc["candidate_id"] == "candidate-1"
    assert doc["approval_id"] == "approval-1"
    assert "supervision" in doc["reason"]

    state.candidate_entries_off_path.write_text("{not json")
    assert candidate_entries_armed(state) is False
