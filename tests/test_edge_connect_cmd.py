"""`nakagai-edge connect`: the endpoint first, the wiring second.

Every test here patches `nakagai_edge.edge.clients` rather than letting the real
table run. `_claude_register` shells out to `claude mcp add --scope user`, and
`install_skills` writes into `~/.claude/skills`, so a test that reached the real
implementations would edit the machine running the suite.
"""

import json

import yaml

from nakagai_edge import cli
from nakagai_edge.edge import clients


class _FakeClient:
    """Stands in for an AgentClient without the dataclass's callable fields, so
    a test can see what it was asked to do."""

    def __init__(self, tmp_path, *, ok=True):
        self.key, self.label, self.ok = "fake", "Fake Client", ok
        self.registered = []
        self._skills = tmp_path / "fake-skills"

    def register(self, server_url):
        self.registered.append(server_url)
        return self.ok

    def skills_dir(self):
        return self._skills


def test_connect_prints_the_url_and_snippet(capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    rc = cli.main(["connect", "--no-register"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "127.0.0.1:8330/mcp/" in out
    assert "mcpServers" in out


def test_no_register_touches_nothing(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    called = []
    monkeypatch.setattr(clients, "detected", lambda: called.append("detect") or [])
    cli.main(["connect", "--no-register"])
    assert called == []


def test_the_port_carries_into_both_the_url_and_the_snippet(capsys, monkeypatch,
                                                            tmp_path):
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    cli.main(["connect", "--no-register", "--port", "9999"])
    out = capsys.readouterr().out
    assert "127.0.0.1:9999/mcp/" in out
    assert "8330" not in out


def test_an_unknown_client_still_leaves_with_the_snippet(capsys, monkeypatch,
                                                         tmp_path):
    """The whole point of printing first: someone on a client nobody has written
    an entry for gets everything they need anyway."""
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    monkeypatch.setattr(clients, "detected", lambda: [])
    rc = cli.main(["connect"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "mcpServers" in out
    assert "Paste the snippet" in out


def test_a_detected_client_gets_registered_and_the_skills_installed(
        capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    fake = _FakeClient(tmp_path)
    monkeypatch.setattr(clients, "detected", lambda: [fake])

    rc = cli.main(["connect"])
    out = capsys.readouterr().out

    assert rc == 0
    assert fake.registered == ["http://127.0.0.1:8330/mcp/"]
    assert "registered" in out
    assert (tmp_path / "fake-skills" / "halt" / "SKILL.md").is_file()
    assert json.loads((tmp_path / "skills-manifest.json").read_text())


def test_a_failed_registration_says_so_and_still_installs(capsys, monkeypatch,
                                                          tmp_path):
    """Registration is the part that can fail. It must not take the skills down
    with it, and it must name the fallback rather than going quiet."""
    monkeypatch.setattr(cli, "default_root", lambda: tmp_path)
    fake = _FakeClient(tmp_path, ok=False)
    monkeypatch.setattr(clients, "detected", lambda: [fake])

    rc = cli.main(["connect"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "NOT registered" in out
    assert (tmp_path / "fake-skills" / "halt" / "SKILL.md").is_file()


# --- setup connects too, and does it on the near side of the blocking serve ---

def _paired_and_ready(root):
    """An edge that has pairing and broker tokens already, so `setup` walks
    straight through to the serve."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    tokens = root / "secrets" / "tokens"
    tokens.mkdir(parents=True)
    (tokens / "robinhood-trading.json").write_text(
        json.dumps({"tokens": {"access_token": "x"}}))
    return state


def _synced(state):
    """What a successful sync lands: a registry, and the stamp that says the
    platform answered."""
    from nakagai_edge.edge.sync import _stamp

    (state.root / "config").mkdir(parents=True, exist_ok=True)
    (state.root / "config" / "connectors.yaml").write_text(
        yaml.safe_dump({"connectors": [
            {"id": "robinhood-trading", "enabled": True,
             "auth": {"mode": "oauth"}}]}))
    _stamp(state, "v1")


def test_setup_connects_before_it_serves(monkeypatch, tmp_path, capsys):
    """Serving blocks and never returns, so connect on the far side of it would
    simply never run. This pins which side of the call it sits on."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _paired_and_ready(tmp_path)
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda state, client: _synced(state))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    order = []
    monkeypatch.setattr(clients, "detected", lambda: order.append("connect") or [])
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: order.append("serve"))

    assert cli.main(["setup", "--platform", "http://platform.test"]) == 0
    assert order == ["connect", "serve"]
    assert "mcpServers" in capsys.readouterr().out


def test_setup_no_register_prints_the_endpoint_and_touches_no_config(
        monkeypatch, tmp_path, capsys):
    """The flag withholds the wiring, never the information."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _paired_and_ready(tmp_path)
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda state, client: _synced(state))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    called = []
    monkeypatch.setattr(clients, "detected", lambda: called.append("detect") or [])
    monkeypatch.setattr("nakagai_edge.edge.runtime.run", lambda root, port: None)

    assert cli.main(["setup", "--platform", "http://platform.test",
                     "--no-register"]) == 0
    assert called == []
    assert "127.0.0.1:8330/mcp/" in capsys.readouterr().out
