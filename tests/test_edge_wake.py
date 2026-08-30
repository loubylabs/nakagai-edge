import json
import os
import subprocess
import time

from nakagai_edge.edge.candidate import CandidateWakeScope
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.wake import WakeRunner


def test_wake_runner_serializes_response_required_events():
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    runner = WakeRunner(["agent", "run"], EdgeState("/tmp/unused-edge-wake"),
                        note=lambda _: None, run=run)
    runner.emit({"seq": 1, "response_required": False})
    runner.emit({"seq": 2, "response_required": True, "text": "hello"})
    runner.close()

    assert len(calls) == 1
    command, kwargs = calls[0]
    assert command == ("agent", "run")
    assert json.loads(kwargs["input"])["seq"] == 2
    assert kwargs["text"] is True
    assert kwargs["check"] is False


def test_wake_runner_reports_nonzero_exit():
    notes = []

    def run(command, **kwargs):
        return subprocess.CompletedProcess(command, 7)

    runner = WakeRunner(["agent"], EdgeState("/tmp/unused-edge-wake"),
                        note=notes.append, run=run)
    runner.emit({"seq": 9, "response_required": True})
    runner.close()

    assert notes == ["[wake] command exited 7 for seq 9"]


def test_candidate_wake_scope_exists_only_while_that_candidate_runs(tmp_path):
    state = EdgeState(tmp_path)
    seen = []

    def run(command, **kwargs):
        seen.append(CandidateWakeScope(state).current())
        return subprocess.CompletedProcess(command, 0)

    runner = WakeRunner(["agent"], state=state, note=lambda _: None, run=run)
    runner.emit({
        "seq": 12,
        "kind": "execution_candidate",
        "response_required": True,
        "candidate_id": "candidate-12",
        "expires_at": time.time() + 300,
    })
    runner.close()

    assert seen == [{"candidate_id": "candidate-12", "seq": 12}]
    assert CandidateWakeScope(state).current() is None


def test_candidate_wake_scope_is_bounded_by_candidate_expiry(tmp_path):
    state = EdgeState(tmp_path)
    seen = []

    def run(command, **kwargs):
        seen.append(CandidateWakeScope(state).current())
        return subprocess.CompletedProcess(command, 0)

    runner = WakeRunner(["agent"], state=state, note=lambda _: None, run=run)
    runner.emit({
        "seq": 13,
        "kind": "execution_candidate",
        "response_required": True,
        "candidate_id": "candidate-expired",
        "expires_at": time.time() - 1,
    })
    runner.close()

    assert seen == [None]
    assert CandidateWakeScope(state).current() is None


def test_a_crashed_listener_scope_expires_closed(tmp_path, monkeypatch):
    state = EdgeState(tmp_path)
    clock = [1_000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    scope = CandidateWakeScope(state)
    scope.begin({
        "seq": 14,
        "kind": "execution_candidate",
        "response_required": True,
        "candidate_id": "candidate-crash",
        "expires_at": 1_005.0,
    })
    assert scope.current() == {"candidate_id": "candidate-crash", "seq": 14}

    clock[0] = 1_006.0

    assert scope.current() is None
    assert not state.candidate_wake_path.exists()


def test_an_unreadable_scope_fails_closed_for_one_bounded_interval(
        tmp_path, monkeypatch):
    state = EdgeState(tmp_path)
    state.candidate_wake_path.parent.mkdir(parents=True)
    state.candidate_wake_path.write_text("{not json")
    os.utime(state.candidate_wake_path, (1_000.0, 1_000.0))
    clock = [1_001.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])

    assert CandidateWakeScope(state).current() == {
        "candidate_id": None, "seq": None}

    clock[0] = 1_901.0

    assert CandidateWakeScope(state).current() is None
    assert not state.candidate_wake_path.exists()


def test_an_older_wake_cannot_clear_a_newer_candidate_scope(tmp_path):
    state = EdgeState(tmp_path)
    scope = CandidateWakeScope(state)
    first = scope.begin({
        "seq": 15, "kind": "execution_candidate", "response_required": True,
        "candidate_id": "candidate-old", "expires_at": time.time() + 300,
    })
    second = scope.begin({
        "seq": 16, "kind": "execution_candidate", "response_required": True,
        "candidate_id": "candidate-new", "expires_at": time.time() + 300,
    })

    scope.finish(first)

    assert scope.current() == {"candidate_id": "candidate-new", "seq": 16}
    scope.finish(second)
    assert scope.current() is None
