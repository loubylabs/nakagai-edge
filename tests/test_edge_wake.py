import json
import subprocess

from nakagai_edge.edge.wake import WakeRunner


def test_wake_runner_serializes_response_required_events():
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    runner = WakeRunner(["agent", "run"], note=lambda _: None, run=run)
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

    runner = WakeRunner(["agent"], note=notes.append, run=run)
    runner.emit({"seq": 9, "response_required": True})
    runner.close()

    assert notes == ["[wake] command exited 7 for seq 9"]
