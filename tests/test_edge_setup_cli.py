"""`edge sync` pulls the registry down on its own, with no server running."""

import json
import time

import pytest
import yaml

pytest.importorskip("nakagai")

from nakagai.cli import main as platform_main  # noqa: E402
from nakagai_edge.cli import main as edge_main  # noqa: E402


@pytest.fixture
def edge_root(tmp_path, monkeypatch):
    root = tmp_path / "edge"
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(root))
    return root


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
    monkeypatch.setattr("builtins.input", lambda *_: "y")

    rc = edge_main(["setup", "CODE1", "--platform", "http://platform.test",
               "--no-run"])
    assert rc == 0

    out = capsys.readouterr().out
    assert "paired" in out and "synced" in out and "robinhood-trading" in out
    assert ("preflight", "http://platform.test") in calls
    assert ("sync", None) in calls
    # the login was pointed at the EDGE root: broker tokens live nowhere else
    assert ("login", "robinhood-trading", edge_root) in calls
    assert (edge_root / "agent.json").exists()
    # the planner's login reason is printed on its own line, not buried in
    # input()'s prompt argument where no monkeypatched input can see it
    assert "needs a one-time browser login" in out


def test_setup_declining_login_warns_and_continues(edge_root, monkeypatch, capsys):
    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", lambda url, **kw: None)
    monkeypatch.setattr("nakagai_edge.edge.client.pair",
                        lambda url, code, **kw: {"agent_id": "ag1", "token": "nk_agent_x"})
    monkeypatch.setattr("nakagai_edge.edge.sync.sync_once",
                        lambda state, client: _registry(state))
    monkeypatch.setattr("nakagai_edge.edge.client.PlatformClient.__init__",
                        lambda self, *a, **k: None)
    monkeypatch.setattr("builtins.input", lambda *_: "n")

    assert edge_main(["setup", "CODE1", "--platform", "http://platform.test",
                 "--no-run"]) == 0
    out = capsys.readouterr().out
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
    monkeypatch.setattr("builtins.input", lambda *_: "y")

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

    def must_not_ask(*a, **k):
        raise AssertionError("asked to log in despite existing tokens")

    monkeypatch.setattr("builtins.input", must_not_ask)

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

    def must_not_input(*a, **k):
        raise AssertionError("prompted for login despite a failed sync")

    monkeypatch.setattr("builtins.input", must_not_input)

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

    def must_not_input(*a, **k):
        raise AssertionError("prompted for login despite a failed sync")

    monkeypatch.setattr("builtins.input", must_not_input)

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


def test_setup_default_mode_serves_the_edge_root_on_the_default_port(
        edge_root, monkeypatch, capsys):
    """Every other setup test passes --no-run, so the actual `run(...)` call
    at the end of the command is otherwise never exercised."""
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

    calls = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: calls.append((root, port)))

    rc = edge_main(["setup", "--platform", "http://platform.test"])
    assert rc == 0
    assert calls == [(edge_root, 8330)]


def test_setup_default_mode_honors_an_explicit_port(edge_root, monkeypatch, capsys):
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

    calls = []
    monkeypatch.setattr("nakagai_edge.edge.runtime.run",
                        lambda root, port: calls.append((root, port)))

    rc = edge_main(["setup", "--platform", "http://platform.test", "--port", "9999"])
    assert rc == 0
    assert calls == [(edge_root, 9999)]


# --- connectors login: roots correctly, refuses brokers ---------------------

def _write_registry(root, entries):
    """A minimal config/connectors.yaml under `root`, the shape
    `_connector_role` (nakagai/cli.py) reads."""
    path = root / "config" / "connectors.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"connectors": entries}))


def test_connectors_login_roots_at_settings_not_cwd(tmp_path, monkeypatch, capsys):
    seen = {}
    workspace = tmp_path / "workspace"
    _write_registry(workspace, [{"id": "yfinance-data", "role": "data"}])

    async def fake_login(root, connector_id):
        seen["root"] = root
        return {"ok": True, "connector": connector_id, "tool_count": 1}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)
    monkeypatch.setenv("NAKAGAI_ROOT", str(workspace))
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path / "edge"))

    rc = platform_main(["connectors", "login", "yfinance-data"])
    assert rc == 0
    assert seen["root"] == workspace


def test_connectors_login_refuses_a_broker_connector(tmp_path, monkeypatch, capsys):
    """The load-bearing assertion: broker credentials never touch the
    platform, whether or not this machine happens to have a paired edge.

    `fake_login` records instead of raising: `_gateway_run` catches every
    exception and turns it into rc=1, so a raising fake would pass this test
    even if the guard were deleted. The `calls == []` assertion is the one
    that actually enforces "never invoked".
    """
    workspace = tmp_path / "workspace"
    _write_registry(workspace, [{"id": "robinhood-trading", "role": "broker"}])
    monkeypatch.setenv("NAKAGAI_ROOT", str(workspace))
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path / "edge"))

    calls = []

    async def fake_login(root, connector_id):
        calls.append((root, connector_id))
        return {"ok": True, "connector": connector_id, "tool_count": 1}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)

    rc = platform_main(["connectors", "login", "robinhood-trading"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "nakagai-edge login robinhood-trading" in out
    assert calls == []


def test_connectors_login_refuses_a_broker_even_when_edge_is_paired(
        tmp_path, monkeypatch, capsys):
    """The refusal must not depend on whether an edge is paired: a broker
    login on the platform is wrong either way."""
    from nakagai_edge.edge.state import EdgeState

    workspace = tmp_path / "workspace"
    edge = tmp_path / "edge"
    _write_registry(workspace, [{"id": "robinhood-trading", "role": "broker"}])
    EdgeState(edge).save_agent("http://platform.test", "ag1", "nk_agent_x")
    monkeypatch.setenv("NAKAGAI_ROOT", str(workspace))
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(edge))

    calls = []

    async def fake_login(root, connector_id):
        calls.append((root, connector_id))
        return {"ok": True, "connector": connector_id, "tool_count": 1}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)

    rc = platform_main(["connectors", "login", "robinhood-trading"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "nakagai-edge login robinhood-trading" in out
    assert calls == []


def test_connectors_login_refuses_an_unknown_connector_id(
        tmp_path, monkeypatch, capsys):
    """An id the registry does not list might be a broker we cannot see, so
    it must refuse rather than fall through to a platform login."""
    workspace = tmp_path / "workspace"
    _write_registry(workspace, [{"id": "yfinance-data", "role": "data"}])
    monkeypatch.setenv("NAKAGAI_ROOT", str(workspace))
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path / "edge"))

    calls = []

    async def fake_login(root, connector_id):
        calls.append((root, connector_id))
        return {"ok": True, "connector": connector_id, "tool_count": 1}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)

    rc = platform_main(["connectors", "login", "not-a-real-connector"])
    assert rc == 1
    assert calls == []


def test_connectors_login_refuses_when_the_registry_cannot_be_read(
        tmp_path, monkeypatch, capsys):
    """A registry that fails to parse must refuse too, same fail-closed
    posture as an unknown id: we cannot confirm the role isn't broker."""
    workspace = tmp_path / "workspace"
    path = workspace / "config" / "connectors.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("connectors: [this is not: valid: yaml: at all")
    monkeypatch.setenv("NAKAGAI_ROOT", str(workspace))
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path / "edge"))

    calls = []

    async def fake_login(root, connector_id):
        calls.append((root, connector_id))
        return {"ok": True, "connector": connector_id, "tool_count": 1}

    monkeypatch.setattr("nakagai_edge.oauth_login.login", fake_login)

    rc = platform_main(["connectors", "login", "robinhood-trading"])
    assert rc == 1
    assert calls == []


# --- the "no tokens" error names the login that will actually work ----------

def _oauth_spec(connector_id, role):
    from nakagai_edge.config import ConnectorSpec
    return ConnectorSpec(id=connector_id, kind="mcp-http", role=role,
                         url="https://example.test/mcp", enabled=True,
                         auth={"mode": "oauth"})


def test_no_tokens_message_for_a_broker_points_at_the_edge_login(tmp_path):
    """This string surfaces to agents and is displayed in the web UI. Sending
    the user to `connectors login` for a broker is a dead end: that command
    hard-refuses brokers, because broker credentials live only on the edge."""
    from nakagai_edge.auth import build_http_client

    with pytest.raises(ValueError) as e:
        build_http_client(_oauth_spec("robinhood-trading", "broker"), tmp_path)

    msg = str(e.value)
    assert "nakagai-edge login robinhood-trading" in msg
    assert "connectors login" not in msg


def test_no_tokens_message_for_a_non_broker_keeps_the_connectors_login(tmp_path):
    """For everything that is not a broker, `connectors login` is still the
    right command, and still the one that works."""
    from nakagai_edge.auth import build_http_client

    with pytest.raises(ValueError) as e:
        build_http_client(_oauth_spec("notion-docs", "data"), tmp_path)

    msg = str(e.value)
    assert "nakagai connectors login notion-docs" in msg
    assert "edge login" not in msg
