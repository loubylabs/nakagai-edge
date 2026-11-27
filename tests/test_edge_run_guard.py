"""What `run()` does before it holds the port.

Under a login service a daemon is started at every login and after every
failed exit. So the deliberate refusals (an edge already serves the port, the
edge is not paired) must happen first, touch nothing, and exit 0, which the
service reads as "stay down". A second daemon that got as far as the pidfile
would overwrite the live one's record, and one that reached
`recover_interrupted` would mark the live brake's in-flight exit
`outcome_unknown`.
"""

import json
import socket

import pytest

from nakagai_edge.edge import runtime
from nakagai_edge.edge.state import EdgeState


@pytest.fixture
def untouchable(monkeypatch):
    def boom(*a, **k):
        pytest.fail("run() got past its refusal")
    monkeypatch.setattr(runtime, "sync_once", boom)
    monkeypatch.setattr(runtime, "recover_interrupted", boom)
    monkeypatch.setattr(runtime, "build_hub", boom)


def test_an_unpaired_edge_exits_0_with_one_line(tmp_path, capsys, untouchable):
    assert runtime.run(tmp_path, port=8331) is None
    err = capsys.readouterr().err
    assert "not paired" in err and len(err.strip().splitlines()) == 1
    assert not EdgeState(tmp_path).pid_path.exists()


def test_a_held_port_exits_0_and_leaves_the_live_record_alone(
        tmp_path, capsys, untouchable):
    state = EdgeState(tmp_path)
    state.save_agent("http://127.0.0.1:1", "ag1", "nk_agent_t")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(8)
    port = sock.getsockname()[1]
    live = {"pid": 999999, "port": port, "started_at": 1.0, "version": "x"}
    state.pid_path.write_text(json.dumps(live))
    try:
        assert runtime.run(tmp_path, port=port) is None
    finally:
        sock.close()
    err = capsys.readouterr().err
    assert f"127.0.0.1:{port}" in err and len(err.strip().splitlines()) == 1
    assert json.loads(state.pid_path.read_text()) == live


def test_the_cli_exits_0_on_a_refusal(tmp_path, monkeypatch, untouchable):
    from nakagai_edge.cli import main
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    assert main(["run", "--port", "8331"]) == 0
