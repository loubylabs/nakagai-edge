"""The gates in front of a restart, and the proof after it.

A restart takes the brake off the tape for a few seconds. That is cheap at
20:00 and expensive at 10:30, and this command cannot tell the time, so it
asks the ledger what is armed instead.
"""

import json
import socket

import pytest

from nakagai_edge.cli import main
from nakagai_edge.edge.state import EdgeState


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    return tmp_path


def _listener():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    s.listen(1)
    return s, s.getsockname()[1]


def _pidfile(state, *, pid, port):
    state.pid_path.parent.mkdir(parents=True, exist_ok=True)
    state.pid_path.write_text(json.dumps(
        {"pid": pid, "port": port, "started_at": 1.0, "version": "0.2.1"}))


def _supervised(state, records):
    """The real ledger shape: a flat dict keyed by position_id, the same
    document `supervision.record` writes and `supervision.load` reads back.
    Not a `{"positions": [...]}` wrapper; nothing in the ledger uses one."""
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    doc = {rec["position_id"]: rec for rec in records}
    state.supervised_path.write_text(json.dumps(doc))


def test_refuses_on_a_live_port_it_cannot_claim(root, capsys):
    """Tonight's machine: a daemon older than the pidfile."""
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=2**22, port=port)
        assert main(["restart"]) != 0
        out = capsys.readouterr()
        assert "by hand" in (out.out + out.err)
    finally:
        sock.close()


def test_refuses_while_a_position_is_armed(root, capsys, monkeypatch):
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        _supervised(state, [
            {"position_id": "p1", "symbol": "AAPL", "quantity": 120,
             "stop_price": 224.10, "state": "armed", "blocked": ""},
        ])
        assert main(["restart"]) != 0
        err = capsys.readouterr().err
        assert "AAPL" in err          # names what is in flight, not just a count
        assert "--force" in err       # and how to override
    finally:
        sock.close()


def test_force_restarts_past_an_armed_position(root, monkeypatch):
    """The control case for the gate above. Without this, a gate that refused
    unconditionally would pass the previous test and be useless."""
    import os
    sock, port = _listener()
    stopped, spawned = [], []
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        _supervised(state, [
            {"position_id": "p1", "symbol": "AAPL", "quantity": 120,
             "stop_price": 224.10, "state": "armed", "blocked": ""},
        ])
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: stopped.append(edge) or True)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
        monkeypatch.setattr(d, "port_listening", lambda p, host="127.0.0.1": True)
        assert main(["restart", "--force"]) == 0
        assert spawned == [port]
    finally:
        sock.close()


def test_restarts_on_the_port_the_old_daemon_served(root, monkeypatch):
    import os
    sock, port = _listener()
    spawned = []
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: True)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
        monkeypatch.setattr(d, "port_listening", lambda p, host="127.0.0.1": True)
        assert main(["restart"]) == 0
        assert spawned == [port]        # not 8330
    finally:
        sock.close()


def test_a_stop_that_never_frees_the_port_fails_loudly(root, monkeypatch):
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: False)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: pytest.fail(
            "spawned a second daemon onto a port the first still holds"))
        assert main(["restart"]) != 0
    finally:
        sock.close()


def test_a_daemon_that_never_comes_up_is_reported_not_claimed(root, monkeypatch, capsys):
    """The theater test. Printing success without checking the port is the
    failure this asserts against."""
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: True)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: 4242)
        monkeypatch.setattr(d, "port_listening", lambda p, host="127.0.0.1": False)
        assert main(["restart"]) != 0
        assert "edge.log" in capsys.readouterr().err   # points at where to look
    finally:
        sock.close()


def test_starts_one_when_nothing_is_running(root, monkeypatch):
    """No pidfile, nothing bound: find_running should read this as "go ahead".

    This machine has a real edge listening on daemon.DEFAULT_PORT (8330) that
    this suite must never touch, so DEFAULT_PORT is repointed at a dead value
    this test owns, and port_listening is faked to answer False only for that
    dead value: everything else, including the port `restart` actually spawns
    onto, reads back as listening. That keeps the assertion honest in both
    directions instead of faking "the port always answers", which would hide
    the real daemon behind a blanket True and make this pass for the wrong
    reason.
    """
    dead_port = 1
    spawned = []
    import nakagai_edge.edge.daemon as d
    monkeypatch.setattr(d, "DEFAULT_PORT", dead_port)
    monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
    monkeypatch.setattr(d, "port_listening",
                        lambda p, host="127.0.0.1": p != dead_port)
    assert main(["restart"]) == 0
    assert spawned == [8330]
