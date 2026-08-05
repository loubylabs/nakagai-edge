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
    """The gate itself.

    `d.stop` and `d.spawn` are patched to fail the test loudly rather than
    left real: `pid` here is `os.getpid()`, this very pytest process, because
    the pidfile has to name a pid `find_running` can prove alive. If the gate
    above them ever regresses and lets control reach `d.stop`, the previous
    version of this test called the REAL `stop()`, which sends a real
    SIGTERM to `os.getpid()`, meaning the gate's own test killed the test
    runner (RC=143, no output, every later test silently never ran). A
    failing assertion beats that outcome by a wide margin.
    """
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        _supervised(state, [
            {"position_id": "p1", "symbol": "AAPL", "quantity": 120,
             "stop_price": 224.10, "state": "armed", "blocked": ""},
        ])
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: pytest.fail(
            "the gate let a restart reach stop() on a live pid: this would "
            "have sent a real SIGTERM to the pytest runner"))
        monkeypatch.setattr(d, "spawn", lambda st, *, port: pytest.fail(
            "the gate let a restart spawn past an armed position"))
        assert main(["restart"]) != 0
        err = capsys.readouterr().err
        assert "AAPL" in err          # names what is in flight, not just a count
        assert "--force" in err       # and how to override
    finally:
        sock.close()


def test_refuses_on_a_firing_position_without_force(root, capsys, monkeypatch):
    """The worse moment than "armed": an exit order is in flight to the
    broker right now. This is the hazard `stop()`'s own docstring names, and
    `recover_interrupted` can only mark it `outcome_unknown` afterward, it
    cannot undo a restart that already happened.

    `d.stop` and `d.spawn` are patched to fail loudly for the same reason as
    the armed-position gate test above: pid is `os.getpid()`, this pytest
    process, and a regressed gate that reached the real `stop()` would send
    it a real SIGTERM rather than raise an assertion. Proven the hard way
    while writing this test: an earlier version without this patch measured
    RC=143 and no output the moment the gate was (deliberately, temporarily)
    broken to check that this test would catch it.
    """
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        _supervised(state, [
            {"position_id": "p1", "symbol": "AAPL", "quantity": 120,
             "stop_price": 224.10, "state": "firing", "blocked": ""},
        ])
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: pytest.fail(
            "the gate let a restart reach stop() on a live pid: this would "
            "have sent a real SIGTERM to the pytest runner"))
        monkeypatch.setattr(d, "spawn", lambda st, *, port: pytest.fail(
            "the gate let a restart spawn past a firing position"))
        assert main(["restart"]) != 0
        err = capsys.readouterr().err
        assert "AAPL" in err
        assert "--force" in err
        assert "exit in flight" in err   # labelled distinctly from a plain armed row
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
        assert len(stopped) == 1 and stopped[0].pid == os.getpid()
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
    failure this asserts against.

    `wait_until_serving` is patched directly rather than faking
    `port_listening` False: faking `port_listening` would also blind
    `find_running` to the real listener this test binds below, so it would
    read "nothing running" and exercise the cold-start branch on the literal
    default port instead of the restart branch this test is named for, and it
    would run `wait_until_serving`'s real 20s default timeout for real, since
    nothing would ever free a port that was never actually held.
    """
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: True)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: 4242)
        monkeypatch.setattr(d, "wait_until_serving", lambda p, **kw: False)
        assert main(["restart"]) != 0
        assert "edge.log" in capsys.readouterr().err   # points at where to look
    finally:
        sock.close()


def test_a_spawn_that_cannot_launch_is_reported_not_raised(root, monkeypatch, capsys):
    """spawn() returning -1 (Popen itself failed) must be reported, not left
    to surface as a bare traceback after the old daemon is already stopped."""
    import os
    sock, port = _listener()
    try:
        state = EdgeState(root)
        _pidfile(state, pid=os.getpid(), port=port)
        import nakagai_edge.edge.daemon as d
        monkeypatch.setattr(d, "stop", lambda edge, **kw: True)
        monkeypatch.setattr(d, "spawn", lambda st, *, port: -1)
        assert main(["restart"]) != 0
        assert "edge.log" in capsys.readouterr().err
    finally:
        sock.close()


def test_starts_one_on_the_default_port_when_there_is_no_record_at_all(
        root, monkeypatch):
    """A first-ever restart: no pidfile, nothing bound. find_running reads
    that as "go ahead", and with nothing to inherit the port is DEFAULT_PORT.

    This machine has a real edge listening on the real DEFAULT_PORT (8330)
    which this suite must never touch, so DEFAULT_PORT is repointed at a dead
    value this test owns and `port_listening` answers False for everything:
    nothing here may reach out to a port it did not create. `wait_until_serving`
    is patched separately rather than leaning on `port_listening`, so the two
    facts stay independent.
    """
    dead_port = 1
    spawned = []
    import nakagai_edge.edge.daemon as d
    monkeypatch.setattr(d, "DEFAULT_PORT", dead_port)
    monkeypatch.setattr(d, "port_listening", lambda p, host="127.0.0.1": False)
    monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
    monkeypatch.setattr(d, "wait_until_serving", lambda p, **kw: True)
    assert main(["restart"]) == 0
    assert spawned == [dead_port]


def test_a_restart_after_a_clean_stop_comes_back_on_the_same_port(
        root, monkeypatch):
    """An edge started on a non-default port, stopped, then restarted.

    The daemon that knew the port is gone by the time `restart` runs, so the
    record is what remembers: `release_pidfile` gives up the claim and keeps
    the port. Without that, this comes back on 8330 and the owner's agent,
    pointed at the port they chose, finds nothing there. The pidfile is
    written and released through the real functions rather than hand-rolled,
    so this fails if either end of that contract moves.
    """
    spawned = []
    import nakagai_edge.edge.daemon as d
    state = EdgeState(root)
    d.write_pidfile(state, port=9317)
    d.release_pidfile(state)                 # what run()'s `finally` does
    monkeypatch.setattr(d, "DEFAULT_PORT", 1)
    monkeypatch.setattr(d, "port_listening", lambda p, host="127.0.0.1": False)
    monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
    monkeypatch.setattr(d, "wait_until_serving", lambda p, **kw: True)
    # A released record names pid 0 and nothing is listening, so there is
    # nothing to stop. Reaching stop() here would mean the code believed a
    # record it must never believe.
    monkeypatch.setattr(d, "stop", lambda edge, **kw: pytest.fail(
        "tried to stop a daemon on the strength of a released record"))
    assert main(["restart"]) == 0
    assert spawned == [9317]                 # not 8330, and not the patched 1


def test_restart_takes_no_port_flag():
    """Choosing a port is starting a different daemon, and `run --port`
    already does that. A `--port` here would silently move an edge that a
    restart is supposed to leave exactly where it was.

    Parses only: nothing calls `args.func`, so this touches no port at all.
    """
    from nakagai_edge.cli import _build_parser
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["restart", "--port", "9000"])
