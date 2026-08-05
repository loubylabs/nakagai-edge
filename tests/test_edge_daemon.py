"""What `restart` is allowed to believe about the process on the port.

The whole point of this module is that a pid is not an identity. A pid read
from a stale file can have been recycled, and signalling a recycled pid kills
something that was never ours.
"""

import json
import socket

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
