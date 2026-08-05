"""What `restart` is allowed to believe about the process on the port.

The whole point of this module is that a pid is not an identity. A pid read
from a stale file can have been recycled, and signalling a recycled pid kills
something that was never ours.
"""

import json
import socket
import subprocess
import sys

import pytest

from nakagai_edge.edge import daemon
from nakagai_edge.edge.daemon import (
    clear_pidfile, find_running, port_listening, read_pidfile, write_pidfile,
)
from nakagai_edge.edge.state import EdgeState


@pytest.fixture
def state(tmp_path):
    return EdgeState(tmp_path)


def _listener():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    return s, s.getsockname()[1]


def test_pidfile_roundtrips(state):
    write_pidfile(state, port=8330)
    doc = read_pidfile(state)
    assert doc["pid"] > 0
    assert doc["port"] == 8330
    assert doc["started_at"] > 0
    assert doc["version"]


def test_missing_pidfile_reads_as_none(state):
    assert read_pidfile(state) is None


def test_unreadable_pidfile_reads_as_none(state):
    """Half-written JSON is not a crash. A corrupt file means we do not know,
    and not knowing is exactly the case find_running must refuse on."""
    state.pid_path.parent.mkdir(parents=True, exist_ok=True)
    state.pid_path.write_text("{not json")
    assert read_pidfile(state) is None


def test_clear_is_idempotent(state):
    clear_pidfile(state)          # nothing there, must not raise
    write_pidfile(state, port=8330)
    clear_pidfile(state)
    assert read_pidfile(state) is None


def test_port_listening_is_true_only_while_something_listens():
    sock, port = _listener()
    try:
        assert port_listening(port) is True
    finally:
        sock.close()
    assert port_listening(port) is False


def test_nothing_listening_is_not_an_error(state, monkeypatch):
    """The empty reason is what tells restart 'just start one'.

    DEFAULT_PORT is pinned to the same unused port as the pidfile so this
    assertion holds regardless of whether a real edge happens to be serving
    on 8330 on the machine running the suite: the test must not depend on
    that ambient fact.
    """
    monkeypatch.setattr(daemon, "DEFAULT_PORT", 1)
    write_pidfile(state, port=1)  # port 1 is not ours and nothing listens
    edge, reason = find_running(state)
    assert edge is None
    assert reason == ""


def test_a_live_port_with_no_pidfile_refuses(state, monkeypatch):
    """The exact state of the owner's machine when this shipped: a daemon
    older than the pidfile. Refusing beats signalling a pid we cannot prove.

    DEFAULT_PORT is monkeypatched to this test's own listener rather than
    left at the real 8330: the fallback candidate has to be something this
    test controls, not whatever a real edge on the host happens to occupy.
    """
    sock, port = _listener()
    monkeypatch.setattr(daemon, "DEFAULT_PORT", port)
    try:
        state.pid_path.parent.mkdir(parents=True, exist_ok=True)
        state.pid_path.write_text(json.dumps(
            {"pid": 1, "port": port + 1, "started_at": 1.0, "version": "0.0.0"}))
        edge, reason = find_running(state)
        assert edge is None
        assert "8330" not in reason          # names the real port, not a default
        assert str(port) in reason
    finally:
        sock.close()


def test_identified_when_the_port_and_the_pidfile_agree(state):
    import os
    sock, port = _listener()
    try:
        state.pid_path.parent.mkdir(parents=True, exist_ok=True)
        state.pid_path.write_text(json.dumps(
            {"pid": os.getpid(), "port": port,
             "started_at": 1.0, "version": "0.2.2"}))
        edge, reason = find_running(state)
        assert reason == ""
        assert edge is not None
        assert (edge.pid, edge.port, edge.version) == (os.getpid(), port, "0.2.2")
    finally:
        sock.close()


def test_a_dead_pid_on_a_live_port_refuses(state):
    """The file says one thing and the OS says another, so we do not know who
    is on that port. Refuse rather than guess."""
    sock, port = _listener()
    try:
        state.pid_path.parent.mkdir(parents=True, exist_ok=True)
        state.pid_path.write_text(json.dumps(
            {"pid": 2**22, "port": port, "started_at": 1.0, "version": "0.2.2"}))
        edge, reason = find_running(state)
        assert edge is None
        assert reason
    finally:
        sock.close()


def _child_that_listens(*, ignore_sigterm: bool) -> tuple[subprocess.Popen, int]:
    """A real process this test owns start to finish, never a pid found lying
    around. It binds its own ephemeral port and prints it back so the test
    can build a RunningEdge without touching the daemon's pidfile machinery.

    The accept loop is not cosmetic: `stop()` polls `port_listening`, which
    completes a TCP handshake and closes its side, every ~0.2-0.35s. A child
    that never calls accept() leaves each of those in the kernel's backlog
    unclaimed, and a small backlog fills within a couple of probes, after
    which further connects time out even though the child, and its listening
    socket, are both still completely alive. That reads as "the port freed"
    to a timeout-based check and is a fake positive of exactly the kind
    `stop()` must not produce, so the child accepts and drops each connection
    the way a real server would, and the backlog never gets the chance.
    """
    src = (
        "import signal, socket, threading, time\n"
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_sigterm else "")
        + "s = socket.socket(); s.bind(('127.0.0.1', 0)); s.listen(5)\n"
        "print(s.getsockname()[1], flush=True)\n"
        "def _drain():\n"
        "    while True:\n"
        "        try:\n"
        "            conn, _ = s.accept()\n"
        "        except OSError:\n"
        "            return\n"
        "        conn.close()\n"
        "threading.Thread(target=_drain, daemon=True).start()\n"
        "time.sleep(30)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", src],
                            stdout=subprocess.PIPE, text=True)
    port = int(proc.stdout.readline())
    return proc, port


def test_stop_sigterms_a_real_child_and_confirms_the_port_freed():
    """SIGTERM is enough for a cooperative process: stop() must report True
    and the port must actually be free, not merely assumed free."""
    child, port = _child_that_listens(ignore_sigterm=False)
    try:
        edge = daemon.RunningEdge(pid=child.pid, port=port,
                                  started_at=0.0, version="test")
        assert daemon.stop(edge, timeout_s=5.0) is True
        assert child.wait(timeout=2) is not None
        assert port_listening(port) is False
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=2)


def test_stop_never_escalates_past_sigterm():
    """A child that ignores SIGTERM must come back False, and must still be
    ALIVE afterward: stop()'s own docstring says it never escalates to
    SIGKILL, and that is the one promise this test exists to hold it to."""
    child, port = _child_that_listens(ignore_sigterm=True)
    try:
        edge = daemon.RunningEdge(pid=child.pid, port=port,
                                  started_at=0.0, version="test")
        assert daemon.stop(edge, timeout_s=1.0) is False
        assert child.poll() is None          # still running: never SIGKILL'd
    finally:
        child.kill()
        child.wait(timeout=2)


def test_spawn_relaunches_through_argv0_into_the_log(state, monkeypatch):
    """argv[0] carries the install shape (uvx, a project venv, ...), and the
    child's stdout/stderr must land in state.log_path rather than a closed
    terminal stream. subprocess.Popen is faked so no real process is
    launched; only the call it would have made is inspected."""
    calls = []

    class _FakeProc:
        pid = 4242

    def _fake_popen(argv, **kwargs):
        calls.append((argv, kwargs))
        return _FakeProc()

    monkeypatch.setattr(subprocess, "Popen", _fake_popen)
    pid = daemon.spawn(state, port=9999)
    assert pid == 4242
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == [sys.argv[0], "run", "--port", "9999"]
    assert kwargs["start_new_session"] is True
    assert kwargs["stdin"] == subprocess.DEVNULL
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["cwd"] == str(state.root)
    assert kwargs["stdout"].name == str(state.log_path)


def test_spawn_reports_a_launch_failure_instead_of_raising(state, monkeypatch):
    """A restart calls spawn() only after the old daemon is already stopped:
    an exception escaping here would leave nothing serving and no chance for
    _cmd_restart to print its `edge.log` remedy. spawn() must catch it and
    hand the caller a value to check instead."""
    def _boom(argv, **kwargs):
        raise OSError("no such executable")

    monkeypatch.setattr(subprocess, "Popen", _boom)
    assert daemon.spawn(state, port=9999) == -1


def test_spawn_reports_a_log_open_failure_instead_of_raising(tmp_path):
    """The other half of spawn()'s try/except OSError: the failure can also
    come from mkdir/open on state.log_path, before Popen is ever called.
    Forced here by making the log's directory a plain file: mkdir(exist_ok=
    True) still refuses when the path exists and is not a directory."""
    root = tmp_path / "not_a_directory"
    root.write_text("occupied")
    state = EdgeState(root)
    assert daemon.spawn(state, port=9999) == -1


def test_wait_until_serving_is_false_when_nothing_answers():
    """A dead port: bind, then let go, so nothing else has had the chance to
    claim it in between."""
    sock, port = _listener()
    sock.close()
    assert daemon.wait_until_serving(port, timeout_s=0.1) is False


def test_wait_until_serving_is_true_once_something_answers():
    sock, port = _listener()
    try:
        assert daemon.wait_until_serving(port, timeout_s=1.0) is True
    finally:
        sock.close()
