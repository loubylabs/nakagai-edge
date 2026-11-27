"""`edge sync` pulls the registry down on its own, with no server running."""

import json
import time

import pytest
import yaml

from nakagai_edge.cli import main as edge_main


@pytest.fixture
def edge_root(tmp_path, monkeypatch):
    root = tmp_path / "edge"
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(root))
    return root


@pytest.fixture(autouse=True)
def no_real_client_wiring(monkeypatch):
    """`setup` now ends by wiring up whatever agent client it detects, and that
    means shelling out to `claude mcp add` and writing into ~/.claude/skills.
    No test in this file is about the wiring, and none of them may edit the
    machine running the suite, so nothing is ever detected here. The wiring
    itself is covered against fakes in tests/test_edge_connect_cmd.py."""
    monkeypatch.setattr("nakagai_edge.edge.clients.detected", lambda: [])


@pytest.fixture(autouse=True)
def never_asks(monkeypatch):
    """`setup` asks nothing. Any prompt is a step someone can't be bothered
    with, so reaching input() fails the test."""
    def ask(*a, **k):
        raise AssertionError("setup prompted")
    monkeypatch.setattr("builtins.input", ask)


def _platform_answered(state, etag="v1"):
    """What the real sync_once does whenever the platform answers us, on both a
    200 and a 304: stamp the fetch. Neither failure path writes it, which is
    what lets the CLI tell a failed pull from an unchanged policy, since both
    return False."""
    from nakagai_edge.edge.sync import _stamp
    _stamp(state, etag)


def _prior_sync(state, entries):
    """An edge that has synced before: a registry on disk and a fetch stamp,
    both left by an earlier successful sync (an hour ago, so a fresh stamp is
    unmistakably newer)."""
    (state.root / "config").mkdir(parents=True, exist_ok=True)
    (state.root / "config" / "connectors.yaml").write_text(
        yaml.safe_dump({"connectors": entries}))
    state.meta_path.parent.mkdir(parents=True, exist_ok=True)
    state.meta_path.write_text(
        json.dumps({"etag": "v0", "fetched_at": time.time() - 3600}))


def test_edge_sync_writes_the_registry(edge_root, monkeypatch, capsys):
    from nakagai_edge.edge.state import EdgeState

    EdgeState(edge_root).save_agent("http://platform.test", "ag1", "nk_agent_x")

    def fake_sync_once(state, client):
        (state.root / "config").mkdir(parents=True, exist_ok=True)
        (state.root / "config" / "connectors.yaml").write_text(
            yaml.safe_dump({"connectors": [{"id": "robinhood-trading"}]}))
        _platform_answered(state)
        return True

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", fake_sync_once)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    assert edge_main(["sync"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["connectors"] == 1
    assert (edge_root / "config" / "connectors.yaml").exists()


def test_edge_sync_unpaired_refuses(edge_root, capsys):
    assert edge_main(["sync"]) == 1
    assert "not paired" in capsys.readouterr().out


def test_edge_sync_failed_pull_reports_failure(edge_root, monkeypatch, capsys):
    """An edge that has never synced, whose first pull fails: nothing on disk."""
    from nakagai_edge.edge.state import EdgeState

    EdgeState(edge_root).save_agent("http://platform.test", "ag1", "nk_agent_x")

    def fake_sync_once(state, client):
        # Mirrors sync_once's real contract: swallow the failure and return
        # False, writing nothing to disk.
        return False

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", fake_sync_once)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    assert edge_main(["sync"]) != 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False


def test_edge_sync_failed_pull_on_a_previously_synced_edge_reports_failure(
        edge_root, monkeypatch, capsys):
    """The edge synced yesterday, so the registry is already on disk. The
    platform is unreachable now (or the token was revoked). The pull reached
    nobody, so there is nothing to say about the registry except that it is as
    stale as it was a second ago, and saying `ok: true` with the old connector
    count would be a lie told at the exact moment the operator is trying to
    repair this."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    _prior_sync(state, [{"id": "stripe"}, {"id": "shopify"}])

    def fake_sync_once_down(state, client):
        # The real contract on a dead platform: swallow, write nothing, False.
        return False

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", fake_sync_once_down)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    assert edge_main(["sync"]) != 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "connectors" not in out          # never the stale count as a success
    assert "did not answer" in out["error"]


def test_edge_sync_unchanged_policy_is_not_a_failure(edge_root, monkeypatch, capsys):
    """A 304 (the platform answered, the policy is unchanged) on an edge whose
    registry is already on disk. sync_once returns False here too, exactly as a
    failed pull does, so the only thing telling them apart is the fetch stamp
    the 304 path writes and the failure paths do not. This must stay a success:
    the registry on disk is current, and the freshness clock was just reset."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    _prior_sync(state, [{"id": "stripe"}, {"id": "shopify"}, {"id": "quickbooks"}])

    def fake_sync_once_unchanged(state, client):
        # Simulate a 304: stamp the fetch (the platform answered), leave the
        # cached registry alone, return False.
        _platform_answered(state, "v0")
        return False

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", fake_sync_once_unchanged)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    assert edge_main(["sync"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["connectors"] == 3


def test_sync_step_unpaired_guard_raises_error(edge_root):
    """_sync_step raises EdgeClientError when called directly with unpaired
    state. This guards against Task 4's direct call without going through
    _cmd_sync (which has its own unpaired check)."""
    from nakagai_edge.cli import _sync_step
    from nakagai_edge.edge.client import EdgeClientError
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    # Explicitly do NOT pair; state.agent() returns None

    with pytest.raises(EdgeClientError, match="not paired"):
        _sync_step(state)


# --- edge setup: pair -> sync -> login -> run --------------------------------

def _registry(state, *, enabled=True):
    """What a successful sync lands on disk: a registry with the broker in it,
    and the fetch stamp that says the platform answered."""
    (state.root / "config").mkdir(parents=True, exist_ok=True)
    (state.root / "config" / "connectors.yaml").write_text(
        yaml.safe_dump({"connectors": [
            {"id": "robinhood-trading", "enabled": enabled,
             "auth": {"mode": "oauth"}}]}))
    _platform_answered(state)


def test_setup_no_run_pairs_syncs_and_reports(edge_root, monkeypatch, capsys):
    calls = []

    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform",
                        lambda url, **kw: calls.append(("preflight", url)))
    monkeypatch.setattr("nakagai_edge.edge.client.pair",
                        lambda url, code, **kw: {"agent_id": "ag1",
                                                 "token": "nk_agent_x"})

    def fake_sync_once(state, client):
        calls.append(("sync", None))
        _registry(state)

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", fake_sync_once)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def fake_login(root, connector_id):
        calls.append(("login", connector_id, root))
        return {"ok": True, "connector": connector_id, "tool_count": 47}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)

    rc = edge_main(["setup", "CODE1", "--platform", "http://platform.test",
               "--no-run"])
    assert rc == 0

    out = capsys.readouterr().out
    assert "paired" in out and "synced" in out and "robinhood-trading" in out
    assert ("preflight", "http://platform.test") in calls
    assert ("sync", None) in calls
    # the login ran without asking, pointed at the EDGE root: broker tokens
    # live nowhere else
    assert ("login", "robinhood-trading", edge_root) in calls
    assert (edge_root / "agent.json").exists()
    assert "needs a one-time browser login" in out


def test_setup_no_login_skips_it_and_prints_the_command(edge_root, monkeypatch, capsys):
    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", lambda url, **kw: None)
    monkeypatch.setattr("nakagai_edge.edge.client.pair",
                        lambda url, code, **kw: {"agent_id": "ag1", "token": "nk_agent_x"})
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda state, client: _registry(state))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def must_not_login(root, connector_id):
        raise AssertionError("logged in under --no-login")

    monkeypatch.setattr("nakagai_edge.oauth_login.login", must_not_login)

    assert edge_main(["setup", "CODE1", "--platform", "http://platform.test",
                      "--no-login", "--no-run"]) == 0
    out = capsys.readouterr().out
    assert "skipped (--no-login)" in out
    assert "nakagai-edge login robinhood-trading" in out


def test_setup_failed_login_stops_with_the_command_to_finish_it(
        edge_root, monkeypatch, capsys):
    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", lambda url, **kw: None)
    monkeypatch.setattr("nakagai_edge.edge.client.pair",
                        lambda url, code, **kw: {"agent_id": "ag1", "token": "nk_agent_x"})
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda state, client: _registry(state))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def boom_login(root, connector_id):
        raise RuntimeError("the browser never came back")

    monkeypatch.setattr("nakagai_edge.oauth_login.login", boom_login)

    assert edge_main(["setup", "CODE1", "--platform", "http://platform.test",
                 "--no-run"]) == 1
    assert "nakagai-edge login robinhood-trading" in capsys.readouterr().out


def test_setup_without_a_code_skips_pairing_and_login_when_both_are_done(
        edge_root, monkeypatch, capsys):
    """The second run of setup: paired, and the broker tokens are already here.
    Both decisions come from the planner reading disk, not from a code."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    tokens = edge_root / "secrets" / "tokens"
    tokens.mkdir(parents=True)
    (tokens / "robinhood-trading.json").write_text(
        json.dumps({"tokens": {"access_token": "x"}}))

    def must_not_pair(*a, **k):
        raise AssertionError("pairing was attempted without a code")

    monkeypatch.setattr("nakagai_edge.edge.client.pair", must_not_pair)
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda st, client: _registry(st))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def must_not_login(root, connector_id):
        raise AssertionError("logged in despite existing tokens")

    monkeypatch.setattr("nakagai_edge.oauth_login.login", must_not_login)

    assert edge_main(["setup", "--platform", "http://platform.test",
                 "--no-run"]) == 0
    out = capsys.readouterr().out
    assert "already paired" in out
    assert "already has robinhood-trading tokens" in out


def test_setup_unpaired_without_a_code_explains_itself(edge_root, capsys):
    assert edge_main(["setup", "--platform", "http://platform.test"]) == 1
    assert "no pairing code" in capsys.readouterr().out


def test_setup_bad_platform_stops_before_pairing(edge_root, monkeypatch, capsys):
    from nakagai_edge.edge.client import EdgeClientError

    def boom(url, **kw):
        raise EdgeClientError("http://localhost:3100 does not look like the nakagai API")

    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", boom)

    def must_not_pair(*a, **k):
        raise AssertionError("pairing was attempted despite a failed preflight")

    monkeypatch.setattr("nakagai_edge.edge.client.pair", must_not_pair)

    assert edge_main(["setup", "CODE1", "--platform", "http://localhost:3100"]) == 1
    assert "does not look like the nakagai API" in capsys.readouterr().out


def test_setup_failed_sync_stops_before_login(edge_root, monkeypatch, capsys):
    """A failed registry pull must not drop the user into a login prompt or a
    server with no policy. sync_once's real contract on failure: swallow the
    error, return False, and write nothing to disk, which is exactly what
    _sync_step reads as failure."""
    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", lambda url, **kw: None)
    monkeypatch.setattr("nakagai_edge.edge.client.pair",
                        lambda url, code, **kw: {"agent_id": "ag1", "token": "nk_agent_x"})
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", lambda state, client: False)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def must_not_login(root, connector_id):
        raise AssertionError("logged in despite a failed sync")

    monkeypatch.setattr("nakagai_edge.oauth_login.login", must_not_login)

    def must_not_run(root, port):
        raise AssertionError("served despite a failed sync")

    monkeypatch.setattr("nakagai_edge.edge.runtime.run", must_not_run)

    rc = edge_main(["setup", "CODE1", "--platform", "http://platform.test"])
    assert rc == 1
    assert "sync failed" in capsys.readouterr().out
    assert not (edge_root / "config" / "connectors.yaml").exists()


def test_setup_failed_sync_on_a_previously_synced_edge_stops_before_login(
        edge_root, monkeypatch, capsys):
    """docs/internal/EDGE.md sells `edge setup` as the repair path for a stale
    registry, so this is the run where the operator most needs the truth. The
    registry is already on disk from an earlier sync; the platform is dead now.
    Setup must stop here, not march on to login and serve against a registry it
    could not confirm."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    _prior_sync(state, [{"id": "robinhood-trading", "enabled": True,
                         "auth": {"mode": "oauth"}}])

    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once", lambda state, client: False)
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)

    async def must_not_login(root, connector_id):
        raise AssertionError("logged in despite a failed sync")

    monkeypatch.setattr("nakagai_edge.oauth_login.login", must_not_login)

    def must_not_run(root, port):
        raise AssertionError("served despite a failed sync")

    monkeypatch.setattr("nakagai_edge.edge.runtime.run", must_not_run)

    rc = edge_main(["setup", "--platform", "http://platform.test"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "sync failed" in out
    assert "did not answer" in out
    assert "synced     1 connectors" not in out    # never the stale count


def _ready_edge(edge_root, monkeypatch):
    """Paired, logged in, and a sync that answers: setup's last step is all
    that is left to decide."""
    from nakagai_edge.edge.state import EdgeState

    state = EdgeState(edge_root)
    state.save_agent("http://platform.test", "ag1", "nk_agent_x")
    tokens = edge_root / "secrets" / "tokens"
    tokens.mkdir(parents=True)
    (tokens / "robinhood-trading.json").write_text(
        json.dumps({"tokens": {"access_token": "x"}}))
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda st, client: _registry(st))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)
    return state


def _service_here(tmp_path, monkeypatch, platform="darwin", **rc_by_verb):
    """A Service on a tmp home whose manager is a recording fake."""
    from nakagai_edge.edge import service
    calls = []

    def runner(cmd):
        calls.append(cmd)
        answer = rc_by_verb.get(cmd[1], 0)
        return answer if isinstance(answer, tuple) else (answer, "")

    svc = service.Service(platform=platform, home=tmp_path / "home", uid=501,
                          user="x", runner=runner, sleep=lambda s: None)
    monkeypatch.setattr(service.Service, "here", classmethod(lambda cls: svc))
    return svc, calls


def _acts(calls):
    """The commands that change something; `print` is only a question."""
    return [c[:2] for c in calls if c[1] != "print"]


def _nothing_running(monkeypatch):
    import nakagai_edge.edge.daemon as d
    monkeypatch.setattr(d, "find_running", lambda state: (None, ""))


def _must_not_serve_here(monkeypatch):
    def must_not_run(root, port):
        raise AssertionError("served in the foreground")
    monkeypatch.setattr("nakagai_edge.edge.runtime.run", must_not_run)


def test_setup_hands_the_edge_to_the_login_service_and_returns(
        edge_root, tmp_path, monkeypatch, capsys):
    """The default: nothing blocks the terminal, and the edge comes back by
    itself at login. Proof, not assertion: the port is waited on."""
    import nakagai_edge.edge.daemon as d
    _ready_edge(edge_root, monkeypatch)
    svc, calls = _service_here(tmp_path, monkeypatch)
    _nothing_running(monkeypatch)
    _must_not_serve_here(monkeypatch)
    waited = []
    monkeypatch.setattr(d, "wait_until_serving",
                        lambda port, **kw: waited.append(port) or True)

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    out = capsys.readouterr().out
    assert "running in the background" in out
    assert "starts by itself at login" in out
    assert waited == [8330]
    assert svc.installed()
    assert _acts(calls) == [["launchctl", "bootout"],
                                      ["launchctl", "bootstrap"]]


def test_setup_service_carries_the_port(edge_root, tmp_path, monkeypatch, capsys):
    import plistlib

    import nakagai_edge.edge.daemon as d
    _ready_edge(edge_root, monkeypatch)
    svc, _ = _service_here(tmp_path, monkeypatch)
    _nothing_running(monkeypatch)
    _must_not_serve_here(monkeypatch)
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: port == 9999)

    assert edge_main(["setup", "--platform", "http://platform.test",
                      "--port", "9999"]) == 0
    body = plistlib.loads(svc.path.read_bytes())
    assert body["ProgramArguments"][1:] == ["run", "--port", "9999"]
    assert body["EnvironmentVariables"]["NAKAGAI_EDGE_ROOT"] == str(edge_root)


def test_setup_stops_a_detached_daemon_before_the_service_starts(
        edge_root, tmp_path, monkeypatch, capsys):
    """Two daemons on one port is the failure: the old one must be gone
    before the manager starts the new one."""
    import nakagai_edge.edge.daemon as d
    _ready_edge(edge_root, monkeypatch)
    svc, calls = _service_here(tmp_path, monkeypatch)
    _must_not_serve_here(monkeypatch)
    old = d.RunningEdge(pid=4242, port=8330, started_at=1.0, version="0.9.0")
    running = [old]
    monkeypatch.setattr(d, "find_running",
                        lambda state: (running[0] if running else None, ""))

    def stop(edge, **kw):
        calls.append(["stop", edge.pid])
        running.clear()
        return True

    monkeypatch.setattr(d, "stop", stop)
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: True)

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    assert _acts(calls) == [["launchctl", "bootout"], ["stop", 4242],
                                      ["launchctl", "bootstrap"]]


def test_setup_reports_a_service_that_never_answers(
        edge_root, tmp_path, monkeypatch, capsys):
    import nakagai_edge.edge.daemon as d
    _ready_edge(edge_root, monkeypatch)
    _service_here(tmp_path, monkeypatch)
    _nothing_running(monkeypatch)
    _must_not_serve_here(monkeypatch)
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: False)

    assert edge_main(["setup", "--platform", "http://platform.test"]) != 0
    assert "edge.log" in capsys.readouterr().out


def test_setup_reports_a_manager_that_refuses_the_unit_and_keeps_serving(
        edge_root, tmp_path, monkeypatch, capsys):
    """By the time the manager refuses, any old daemon is stopped: the unit
    is removed and the edge relaunched detached, so something still serves."""
    import nakagai_edge.edge.daemon as d
    _ready_edge(edge_root, monkeypatch)
    svc, _ = _service_here(tmp_path, monkeypatch, bootstrap=5)
    _nothing_running(monkeypatch)
    _must_not_serve_here(monkeypatch)
    spawned = []
    monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: True)

    assert edge_main(["setup", "--platform", "http://platform.test"]) != 0
    out = capsys.readouterr().out
    assert "serving detached" in out
    assert spawned == [8330] and not svc.path.exists()


def test_setup_with_an_unreachable_manager_serves_in_the_foreground(
        edge_root, tmp_path, monkeypatch, capsys):
    """Over SSH there is no gui domain; in a container no user bus."""
    _ready_edge(edge_root, monkeypatch)
    svc, calls = _service_here(tmp_path, monkeypatch, print=(125, ""))
    served = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: served.append((root, port)))

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    assert served == [(edge_root, 8330)]
    assert _acts(calls) == [] and not svc.path.exists()


def test_setup_leaves_a_healthy_service_alone(
        edge_root, tmp_path, monkeypatch, capsys):
    """A re-run must not bounce the edge (and the brake) for nothing."""
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.edge.service import unit_text
    _ready_edge(edge_root, monkeypatch)
    svc, calls = _service_here(tmp_path, monkeypatch,
                               print=(0, "\tstate = running\n\tpid = 99\n"))
    svc.path.parent.mkdir(parents=True)
    svc.path.write_text(unit_text("darwin", d.relaunch_argv0(), 8330,
                                  str(edge_root), path="/bin"))
    _must_not_serve_here(monkeypatch)
    monkeypatch.setattr(d, "find_running", lambda state: pytest.fail(
        "touched the running edge"))

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    assert "service    already running" in capsys.readouterr().out
    assert _acts(calls) == []


def test_setup_brake_refusal_names_a_flag_setup_has(
        edge_root, tmp_path, monkeypatch, capsys):
    import shlex

    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import _build_parser
    state = _ready_edge(edge_root, monkeypatch)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text(json.dumps({"p1": {
        "position_id": "p1", "symbol": "AAPL", "quantity": 1,
        "stop_price": 1.0, "state": "armed", "blocked": ""}}))
    _, calls = _service_here(tmp_path, monkeypatch)
    _must_not_serve_here(monkeypatch)
    live = d.RunningEdge(pid=4242, port=8330, started_at=1.0, version="x")
    monkeypatch.setattr(d, "find_running", lambda st: (live, ""))

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 3
    hint = capsys.readouterr().err.strip().splitlines()[-1].strip()
    assert hint == "nakagai-edge setup --force"
    assert _build_parser().parse_args(shlex.split(hint)[1:]).force is True
    assert _acts(calls) == []


def test_setup_foreground_serves_here_and_installs_nothing(
        edge_root, tmp_path, monkeypatch, capsys):
    _ready_edge(edge_root, monkeypatch)
    svc, calls = _service_here(tmp_path, monkeypatch)
    served = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: served.append((root, port)))

    assert edge_main(["setup", "--platform", "http://platform.test",
                      "--foreground", "--port", "9999"]) == 0
    assert served == [(edge_root, 9999)]
    assert calls == [] and not svc.installed()


def test_setup_with_no_service_here_serves_in_the_foreground(
        edge_root, tmp_path, monkeypatch, capsys):
    _ready_edge(edge_root, monkeypatch)
    _, calls = _service_here(tmp_path, monkeypatch, platform="win32")
    served = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: served.append((root, port)))

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    assert served == [(edge_root, 8330)]
    assert calls == []


def test_setup_from_a_uvx_cache_installs_no_service(
        edge_root, tmp_path, monkeypatch, capsys):
    """A unit pointed into uv's cache breaks the day uv cleans it, so a uvx
    run serves in the foreground and says how to get the durable install."""
    import sys
    _ready_edge(edge_root, monkeypatch)
    _, calls = _service_here(tmp_path, monkeypatch)
    monkeypatch.setattr(sys, "prefix", "/Users/x/.cache/uv/archive-v0/abc")
    served = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: served.append((root, port)))

    assert edge_main(["setup", "--platform", "http://platform.test"]) == 0
    assert served and calls == []
    assert "uv tool install" in capsys.readouterr().out


def test_setup_no_run_neither_installs_nor_serves(
        edge_root, tmp_path, monkeypatch, capsys):
    _ready_edge(edge_root, monkeypatch)
    _, calls = _service_here(tmp_path, monkeypatch)
    _must_not_serve_here(monkeypatch)

    assert edge_main(["setup", "--platform", "http://platform.test", "--no-run"]) == 0
    assert calls == []
    assert "nakagai-edge service install" in capsys.readouterr().out


# --- the "no tokens" error names the login that will actually work ----------

def _oauth_spec(connector_id, role):
    from nakagai_edge.config import ConnectorSpec
    return ConnectorSpec(id=connector_id, kind="mcp-http", role=role,
                         url="https://example.test/mcp", enabled=True,
                         auth={"mode": "oauth"})


@pytest.mark.parametrize("connector_id, role",
                         [("robinhood-trading", "broker"), ("notion-docs", "data")])
def test_no_tokens_message_points_at_the_edge_login(tmp_path, connector_id, role):
    """This string surfaces to agents and is displayed in the web UI, so it
    must name a command that exists. The edge login is the only one: the
    platform has no connector login, so OAuth tokens (a broker's above all)
    are only ever written on the edge, never on the platform."""
    from nakagai_edge.auth import build_http_client

    with pytest.raises(ValueError) as e:
        build_http_client(_oauth_spec(connector_id, role), tmp_path)

    msg = str(e.value)
    assert f"nakagai-edge login {connector_id}" in msg
    assert "connectors login" not in msg
