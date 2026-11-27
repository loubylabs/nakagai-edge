"""The login service: the edge starts by itself, and comes back after a crash.

Nothing here touches the real `launchctl` or `systemctl`. Every runner is a
recording fake, and the unit file lands under a tmp home, so the suite can run
on a machine whose own edge is a loaded service without disturbing it.
"""

import plistlib
import shlex

import pytest

from nakagai_edge.config import ConnectorSpec
from nakagai_edge.edge.service import (LABEL, UNIT, Service, env_warnings,
                                       service_path, unit_text)

ARGV0 = "/Users/x/.local/bin/nakagai-edge"
ROOT = "/Users/x/.nakagai/edge"
PATH = "/Users/x/.local/bin:/usr/bin:/bin"

LAUNCHCTL_PRINT_RUNNING = """gui/501/ai.nakag.edge = {
\tactive count = 1
\tpath = /Users/x/Library/LaunchAgents/ai.nakag.edge.plist
\tstate = running
\tpid = 4321
}"""
LAUNCHCTL_PRINT_LOADED = """gui/501/ai.nakag.edge = {
\tactive count = 0
\tstate = not running
}"""


class Runner:
    """Records every command. Answers (rc, output): `rc` by default, or a
    per-verb override given as an int or an (rc, output) pair."""

    def __init__(self, rc=0, **by_verb):
        self.calls: list[list[str]] = []
        self.rc = rc
        self.by_verb = by_verb

    def __call__(self, cmd):
        self.calls.append(list(cmd))
        if cmd[:2] == ["systemctl", "--user"]:
            verb = cmd[2]
        elif cmd[0] == "launchctl" and cmd[1] == "print" and cmd[2].count("/") == 1:
            verb = "print_domain"
        else:
            verb = cmd[1]
        answer = self.by_verb.get(verb.replace("-", "_"), self.rc)
        return answer if isinstance(answer, tuple) else (answer, "")


def _svc(tmp_path, platform="darwin", runner=None, sleep=lambda s: None):
    return Service(platform=platform, home=tmp_path, uid=501, user="x",
                   runner=runner or Runner(), sleep=sleep)


@pytest.fixture(autouse=True)
def default_root(monkeypatch):
    """The exact bodies below are the default-root case, on a known PATH."""
    monkeypatch.delenv("NAKAGAI_EDGE_ROOT", raising=False)
    monkeypatch.setenv("PATH", PATH)
    monkeypatch.setattr("nakagai_edge.edge.service.service_path",
                        lambda raw=None: PATH)


# ---- unit_text: the exact file bodies --------------------------------------

MAC_PLIST = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		<string>{PATH}</string>
	</dict>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>Label</key>
	<string>ai.nakag.edge</string>
	<key>ProgramArguments</key>
	<array>
		<string>{ARGV0}</string>
		<string>run</string>
		<string>--port</string>
		<string>8330</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>StandardErrorPath</key>
	<string>{ROOT}/edge.log</string>
	<key>StandardOutPath</key>
	<string>{ROOT}/edge.log</string>
	<key>ThrottleInterval</key>
	<integer>30</integer>
	<key>WorkingDirectory</key>
	<string>{ROOT}</string>
</dict>
</plist>
"""


def test_the_macos_launch_agent_body():
    """KeepAlive only on a failed exit: a daemon that exits 0 on purpose
    (another edge holds the port, the edge is unpaired) stays down instead of
    respawning forever, and a crash loop waits 30 seconds between tries."""
    assert unit_text("darwin", ARGV0, 8330, ROOT, path=PATH) == MAC_PLIST


def test_the_launch_agent_carries_the_edge_root_only_when_it_was_set():
    body = plistlib.loads(unit_text("darwin", ARGV0, 9000, "/tmp/e", path=PATH,
                                    env_root="/tmp/e").encode())
    assert body["EnvironmentVariables"] == {"NAKAGAI_EDGE_ROOT": "/tmp/e",
                                            "PATH": PATH}
    assert body["ProgramArguments"] == [ARGV0, "run", "--port", "9000"]
    assert plistlib.loads(unit_text("darwin", ARGV0, 9000, "/tmp/e", path=PATH)
                          .encode())["EnvironmentVariables"] == {"PATH": PATH}


LINUX_UNIT = """\
[Unit]
Description=Nakagai edge: the broker connector

[Service]
ExecStart="/home/x/.local/bin/nakagai-edge" run --port 8330
WorkingDirectory=/home/x/.nakagai/edge
Environment="PATH=/home/x/.local/bin:/usr/bin:/bin"
Restart=on-failure
RestartSec=30
StandardOutput=append:/home/x/.nakagai/edge/edge.log
StandardError=append:/home/x/.nakagai/edge/edge.log

[Install]
WantedBy=default.target
"""


def test_the_linux_systemd_user_unit_body():
    assert unit_text("linux", "/home/x/.local/bin/nakagai-edge", 8330,
                     "/home/x/.nakagai/edge",
                     path="/home/x/.local/bin:/usr/bin:/bin") == LINUX_UNIT


def test_the_systemd_unit_carries_the_edge_root_only_when_it_was_set():
    body = unit_text("linux", "/b/nakagai-edge", 8330, "/tmp/e", path="/bin",
                     env_root="/tmp/e")
    assert 'Environment="NAKAGAI_EDGE_ROOT=/tmp/e"\n' in body
    assert "NAKAGAI_EDGE_ROOT" not in unit_text("linux", "/b/nakagai-edge", 8330,
                                                "/tmp/e", path="/bin")


def test_systemd_escapes_specifiers_dollars_quotes_and_backslashes():
    """`%` is a specifier in every one of these settings and `$` expands only
    in ExecStart, so a crafted path must reach the process byte for byte."""
    weird = '/home/x/50% "off"/$HOME\\bin'
    body = unit_text("linux", f"{weird}/nakagai-edge", 8330, weird, path=weird,
                     env_root=weird)
    exec_line = next(l for l in body.splitlines() if l.startswith("ExecStart="))
    assert exec_line == ('ExecStart="/home/x/50%% \\"off\\"/$$HOME\\\\bin/'
                         'nakagai-edge" run --port 8330')
    assert "WorkingDirectory=/home/x/50%% \"off\"/$HOME\\bin\n" in body
    assert 'Environment="NAKAGAI_EDGE_ROOT=/home/x/50%% \\"off\\"/$HOME\\\\bin"\n' in body
    assert "StandardOutput=append:/home/x/50%% \"off\"/$HOME\\bin/edge.log\n" in body
    # and it reads back as the same root
    svc_root = Service._systemd_value(body, "WorkingDirectory")
    assert svc_root == weird


def test_no_unit_for_a_platform_with_no_service():
    with pytest.raises(ValueError, match="win32"):
        unit_text("win32", ARGV0, 8330, ROOT, path=PATH)


def test_service_path_keeps_existing_absolute_dirs_once(tmp_path):
    # `service_path` here is the module-top import, bound before the autouse
    # fixture replaced the module attribute.
    (tmp_path / "a").mkdir()
    raw = f"{tmp_path}/a:relative:{tmp_path}/missing:{tmp_path}/a:/bin"
    assert service_path(raw) == f"{tmp_path}/a:/bin"


# ---- what the service will not have ---------------------------------------

def test_env_warnings_name_what_the_service_cannot_see():
    on = {"enabled": True}
    specs = [
        ConnectorSpec(id="bear", kind="mcp-http", role="data", **on,
                      url="https://b.test/mcp/",
                      auth={"mode": "bearer", "token_env": "BEAR_TOKEN"}),
        ConnectorSpec(id="hdr", kind="mcp-http", role="data", **on,
                      url="https://h.test/mcp/",
                      auth={"mode": "headers", "headers": {"X-Key": "HDR_KEY"}}),
        ConnectorSpec(id="npx", kind="mcp-stdio", role="data", command="npx", **on,
                      env={"API_KEY": "${NPX_KEY}", "MODE": "live"}),
        ConnectorSpec(id="abs", kind="mcp-stdio", role="data", **on,
                      command="/usr/local/bin/srv"),
        ConnectorSpec(id="rh", kind="mcp-http", role="broker", **on,
                      url="https://rh.test/mcp/", auth={"mode": "oauth"}),
        ConnectorSpec(id="off", kind="mcp-stdio", role="data", command="npx",
                      enabled=False),
    ]
    lines = env_warnings(specs)
    assert len(lines) == 3
    assert "bear" in lines[0] and "BEAR_TOKEN" in lines[0]
    assert "hdr" in lines[1] and "HDR_KEY" in lines[1]
    assert "npx" in lines[2] and "NPX_KEY" in lines[2] and "not an absolute" in lines[2]


# ---- where the unit lives, and whether the manager has it ------------------

def test_unit_paths(tmp_path):
    assert _svc(tmp_path).path == tmp_path / "Library" / "LaunchAgents" / f"{LABEL}.plist"
    assert _svc(tmp_path, "linux").path == (
        tmp_path / ".config" / "systemd" / "user" / UNIT)


@pytest.mark.parametrize("platform, supported", [
    ("darwin", True), ("linux", True), ("win32", False), ("freebsd14", False)])
def test_which_platforms_have_a_service(tmp_path, platform, supported):
    svc = _svc(tmp_path, platform)
    assert svc.supported is supported
    assert svc.state() == ("not installed" if supported else "unsupported")


def test_installed_needs_the_manager_to_know_the_unit_not_just_the_file(tmp_path):
    svc = _svc(tmp_path, runner=Runner(print=113))
    svc.path.parent.mkdir(parents=True)
    svc.path.write_text(MAC_PLIST)
    assert svc.unit_exists() is True
    assert svc.installed() is False
    assert svc.state() == "not installed"
    assert _svc(tmp_path, runner=Runner()).state() == "installed"


def test_linux_installed_asks_is_enabled(tmp_path):
    r = Runner(is_enabled=1)
    svc = _svc(tmp_path, "linux", runner=r)
    svc.path.parent.mkdir(parents=True)
    svc.path.write_text(LINUX_UNIT)
    assert svc.installed() is False
    assert r.calls == [["systemctl", "--user", "is-enabled", "--quiet", UNIT]]


def test_reachable_asks_the_session_domain(tmp_path):
    assert _svc(tmp_path, runner=Runner()).reachable() is True
    r = Runner(print_domain=(125, "Bad request"))
    assert _svc(tmp_path, runner=r).reachable() is False
    assert r.calls == [["launchctl", "print", "gui/501"]]
    r = Runner(show_environment=1)
    assert _svc(tmp_path, "linux", runner=r).reachable() is False
    assert r.calls == [["systemctl", "--user", "show-environment"]]


def test_the_installed_config_reads_back_off_the_unit(tmp_path):
    svc = _svc(tmp_path)
    assert svc.installed_config() is None
    svc.path.parent.mkdir(parents=True)
    svc.path.write_text(MAC_PLIST)
    assert svc.installed_config() == {"argv0": ARGV0, "port": 8330, "root": ROOT}
    lsvc = _svc(tmp_path, "linux")
    lsvc.path.parent.mkdir(parents=True)
    lsvc.path.write_text(LINUX_UNIT)
    assert lsvc.installed_config() == {
        "argv0": "/home/x/.local/bin/nakagai-edge", "port": 8330,
        "root": "/home/x/.nakagai/edge"}


# ---- the runners, against a fake command runner ----------------------------

def test_macos_install_writes_the_plist_and_bootstraps_it(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, runner=r)
    assert svc.install(argv0=ARGV0, port=8330, root=ROOT) is True
    assert svc.path.read_text() == MAC_PLIST
    assert r.calls == [["launchctl", "bootstrap", "gui/501", str(svc.path)]]


def test_macos_install_retries_a_bootstrap_that_raced_the_bootout(tmp_path):
    """`Bootstrap failed: 5: Input/output error` right after a bootout is
    launchd still tearing the old job down; one short wait clears it."""
    answers = iter([(5, "Input/output error"), (0, "")])
    calls, slept = [], []

    def runner(cmd):
        calls.append(cmd)
        return next(answers)

    svc = _svc(tmp_path, runner=runner, sleep=slept.append)
    assert svc.install(argv0=ARGV0, port=8330, root=ROOT) is True
    assert [c[1] for c in calls] == ["bootstrap", "bootstrap"]
    assert slept and slept[0] > 0


def test_macos_install_reports_a_refused_bootstrap(tmp_path):
    svc = _svc(tmp_path, runner=Runner(bootstrap=5))
    assert svc.install(argv0=ARGV0, port=8330, root=ROOT) is False


def test_macos_unload_boots_out_and_ignores_a_unit_that_was_not_loaded(tmp_path):
    r = Runner(bootout=3)          # "No such process": nothing was loaded
    _svc(tmp_path, runner=r).unload()
    assert r.calls == [["launchctl", "bootout", f"gui/501/{LABEL}"]]


def test_discard_removes_the_unit_after_a_failed_load(tmp_path):
    r = Runner(bootstrap=5)
    svc = _svc(tmp_path, runner=r)
    svc.install(argv0=ARGV0, port=8330, root=ROOT)
    svc.discard()
    assert not svc.path.exists()


def test_macos_uninstall_boots_out_and_removes_the_plist(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, runner=r)
    svc.install(argv0=ARGV0, port=8330, root=ROOT)
    r.calls.clear()
    assert svc.uninstall() is True
    assert not svc.path.exists()
    assert r.calls == [["launchctl", "bootout", f"gui/501/{LABEL}"]]


def test_macos_restart_kickstarts_the_loaded_agent(tmp_path):
    r = Runner()
    assert _svc(tmp_path, runner=r).restart() is True
    assert r.calls == [["launchctl", "kickstart", "-k", f"gui/501/{LABEL}"]]


def test_macos_running_reads_the_state_not_the_exit_code(tmp_path):
    """`launchctl print` exits 0 for a loaded job that is not running."""
    up = _svc(tmp_path, runner=Runner(print=(0, LAUNCHCTL_PRINT_RUNNING)))
    assert up.running() is True and up.pid() == 4321
    loaded = _svc(tmp_path, runner=Runner(print=(0, LAUNCHCTL_PRINT_LOADED)))
    assert loaded.running() is False and loaded.pid() == 0
    gone = _svc(tmp_path, runner=Runner(print=(113, "")))
    assert gone.running() is False and gone.pid() == 0


def test_linux_install_reloads_enables_and_starts(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, "linux", runner=r)
    assert svc.install(argv0="/b/nakagai-edge", port=8330, root="/tmp/e") is True
    assert svc.path.read_text().startswith("[Unit]\n")
    # `restart`, not `start`: a re-install must pick up a rewritten unit even
    # when the old one is still running.
    assert r.calls == [["systemctl", "--user", "daemon-reload"],
                       ["systemctl", "--user", "enable", UNIT],
                       ["systemctl", "--user", "restart", UNIT]]


def test_linux_install_reports_a_failed_start(tmp_path):
    svc = _svc(tmp_path, "linux", runner=Runner(restart=1))
    assert svc.install(argv0="/b/nakagai-edge", port=8330, root="/tmp/e") is False


def test_linux_unload_stops_the_unit(tmp_path):
    r = Runner(stop=5)
    _svc(tmp_path, "linux", runner=r).unload()
    assert r.calls == [["systemctl", "--user", "stop", UNIT]]


def test_linux_uninstall_stops_disables_removes_and_reloads(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, "linux", runner=r)
    svc.install(argv0="/b/nakagai-edge", port=8330, root="/tmp/e")
    r.calls.clear()
    assert svc.uninstall() is True
    assert not svc.path.exists()
    assert r.calls == [["systemctl", "--user", "stop", UNIT],
                       ["systemctl", "--user", "disable", UNIT],
                       ["systemctl", "--user", "daemon-reload"]]


def test_linux_restart_running_and_pid(tmp_path):
    r = Runner(show=(0, "777\n"))
    svc = _svc(tmp_path, "linux", runner=r)
    assert svc.restart() is True
    assert svc.running() is True
    assert svc.pid() == 777
    assert r.calls == [["systemctl", "--user", "restart", UNIT],
                       ["systemctl", "--user", "is-active", "--quiet", UNIT],
                       ["systemctl", "--user", "show", "-p", "MainPID",
                        "--value", UNIT]]


def test_linux_linger(tmp_path):
    off = _svc(tmp_path, "linux", runner=Runner(show_user=(0, "Linger=no\n")))
    assert off.linger_off() is True
    assert off.runner.calls == [["loginctl", "show-user", "x", "-p", "Linger"]]
    on = _svc(tmp_path, "linux", runner=Runner(show_user=(0, "Linger=yes\n")))
    assert on.linger_off() is False
    # An answer we cannot read is not "off": no nag on a guess.
    assert _svc(tmp_path, "linux", runner=Runner(show_user=1)).linger_off() is False


def test_an_unsupported_platform_runs_nothing(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, "win32", runner=r)
    with pytest.raises(ValueError):
        svc.install(argv0=ARGV0, port=8330, root=ROOT)
    assert r.calls == []


# ---- `nakagai-edge service install | uninstall | status` -------------------

@pytest.fixture
def cli(tmp_path, monkeypatch):
    """The CLI against a fake service on a tmp home, a paired edge, and
    nothing serving."""
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.edge import service
    from nakagai_edge.edge.state import EdgeState

    root = tmp_path / "edge"
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(root))
    EdgeState(root).save_agent("http://platform.test", "ag1", "nk_agent_x")
    r = Runner(print=(0, LAUNCHCTL_PRINT_RUNNING))
    holder = {"svc": _svc(tmp_path / "home", runner=r)}
    monkeypatch.setattr(service.Service, "here",
                        classmethod(lambda cls: holder["svc"]))
    monkeypatch.setattr(d, "find_running", lambda state: (None, ""))
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: True)
    monkeypatch.setattr(d, "spawn", lambda st, *, port: pytest.fail(
        "spawned a detached daemon"))
    monkeypatch.setattr(d, "DEFAULT_PORT", 8330)
    holder["root"] = root
    return holder, r


def test_service_install_writes_loads_and_waits_for_the_port(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    assert main(["service", "install"]) == 0
    assert holder["svc"].installed()
    assert [c[1] for c in r.calls[:3]] == ["print", "bootout", "bootstrap"]
    assert "starts by itself at login" in capsys.readouterr().out


def test_service_install_twice_replaces_the_unit(cli):
    """Idempotent: the second install boots the first out before loading."""
    from nakagai_edge.cli import main
    holder, r = cli
    assert main(["service", "install"]) == 0
    assert main(["service", "install", "--port", "9100"]) == 0
    verbs = [c[1] for c in r.calls if c[1] in ("bootout", "bootstrap")]
    assert verbs == ["bootout", "bootstrap", "bootout", "bootstrap"]
    body = plistlib.loads(holder["svc"].path.read_bytes())
    assert body["ProgramArguments"][-1] == "9100"


def test_service_install_refuses_an_unpaired_edge(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    (holder["root"] / "agent.json").unlink()
    assert main(["service", "install"]) == 1
    assert not holder["svc"].path.exists()
    assert "not paired" in capsys.readouterr().err


def test_service_install_refuses_a_port_something_else_holds(cli, monkeypatch, capsys):
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import main
    holder, r = cli
    monkeypatch.setattr(d, "find_running",
                        lambda state: (None, "something is serving 127.0.0.1:8330"))
    assert main(["service", "install"]) != 0
    assert not holder["svc"].path.exists()
    assert "something is serving" in capsys.readouterr().err


def test_a_failed_load_removes_the_unit_and_keeps_the_edge_serving(
        cli, monkeypatch, capsys):
    """The old daemon is already stopped when the manager refuses, so the
    edge must not be left with nothing serving."""
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import main
    holder, r = cli
    r.by_verb["bootstrap"] = 5
    spawned = []
    monkeypatch.setattr(d, "spawn", lambda st, *, port: spawned.append(port) or 4242)
    assert main(["service", "install"]) != 0
    assert spawned == [8330]
    assert not holder["svc"].path.exists()
    err = capsys.readouterr().err
    assert "serving detached" in err and len(err.strip().splitlines()) == 1


def test_service_install_refuses_when_the_manager_is_unreachable(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    r.by_verb["print_domain"] = (125, "")
    assert main(["service", "install"]) == 1
    assert not holder["svc"].path.exists()
    assert "not reachable" in capsys.readouterr().err


def test_service_install_from_a_uvx_cache_refuses(cli, monkeypatch, capsys):
    import sys
    from nakagai_edge.cli import main
    holder, r = cli
    monkeypatch.setattr(sys, "prefix", "/Users/x/.cache/uv/archive-v0/abc")
    assert main(["service", "install"]) == 1
    assert r.calls == [] and not holder["svc"].path.exists()
    assert "uv tool install" in capsys.readouterr().err


def test_service_install_refuses_another_roots_unit(cli, capsys):
    """One label per user: a unit written for another edge root is not this
    root's to replace, and its brake ledger is not the one this root reads."""
    from nakagai_edge.cli import main
    holder, r = cli
    svc = holder["svc"]
    svc.path.parent.mkdir(parents=True)
    svc.path.write_text(unit_text("darwin", ARGV0, 8330, "/elsewhere/edge",
                                  path=PATH))
    for action in ("install", "uninstall"):
        assert main(["service", action]) == 1
        err = capsys.readouterr().err
        assert "/elsewhere/edge" in err and len(err.strip().splitlines()) == 1
    assert svc.path.exists()


def test_service_install_warns_about_what_the_service_will_not_see(cli, capsys):
    import yaml
    from nakagai_edge.cli import main
    holder, _ = cli
    (holder["root"] / "config").mkdir(parents=True)
    (holder["root"] / "config" / "connectors.yaml").write_text(yaml.safe_dump(
        {"connectors": [{"id": "npx", "kind": "mcp-stdio", "role": "data",
                         "command": "npx", "enabled": True}]}))
    assert main(["service", "install"]) == 0
    out = capsys.readouterr().out
    assert "npx" in out and "not an absolute" in out


def test_service_install_on_linux_names_lingering_when_it_is_off(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    r.by_verb["show_user"] = (0, "Linger=no\n")
    holder["svc"] = _svc(holder["svc"].home, "linux", runner=r)
    assert main(["service", "install"]) == 0
    out = capsys.readouterr().out
    assert "loginctl enable-linger" in out
    assert not any(c[:2] == ["loginctl", "enable-linger"] for c in r.calls)


def _armed(root):
    import json
    from nakagai_edge.edge.state import EdgeState
    state = EdgeState(root)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text(json.dumps({"p1": {
        "position_id": "p1", "symbol": "AAPL", "quantity": 1,
        "stop_price": 1.0, "state": "armed", "blocked": ""}}))


def test_service_uninstall_refuses_while_the_brake_watches(cli, monkeypatch, capsys):
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import _build_parser, main
    holder, r = cli
    main(["service", "install"])
    _armed(holder["root"])
    live = d.RunningEdge(pid=4321, port=8330, started_at=1.0, version="x")
    monkeypatch.setattr(d, "find_running", lambda state: (live, ""))
    capsys.readouterr()
    assert main(["service", "uninstall"]) == 3
    assert holder["svc"].path.exists()
    hint = capsys.readouterr().err.strip().splitlines()[-1].strip()
    _build_parser().parse_args(shlex.split(hint)[1:])     # the hint runs
    assert main(["service", "uninstall", "--force"]) == 0
    assert not holder["svc"].path.exists()


def test_uninstall_judges_the_brake_by_the_daemon_it_would_stop(
        cli, monkeypatch, capsys):
    """A hand-started edge keeps watching through an uninstall, which never
    touches it: refusing over its positions would be refusing for nothing,
    and "the edge is stopped" would be false."""
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import main
    holder, r = cli
    main(["service", "install"])
    _armed(holder["root"])
    r.by_verb["print"] = (0, LAUNCHCTL_PRINT_LOADED)      # service runs no pid
    detached = d.RunningEdge(pid=999, port=8330, started_at=1.0, version="x")
    monkeypatch.setattr(d, "find_running", lambda state: (detached, ""))
    capsys.readouterr()
    assert main(["service", "uninstall"]) == 0
    out = capsys.readouterr().out
    assert "pid 999" in out and "still serves" in out
    assert "the edge is stopped" not in out


def test_service_uninstall_removes_it(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    main(["service", "install"])
    assert main(["service", "uninstall"]) == 0
    assert not holder["svc"].path.exists()
    assert r.calls[-1][1] == "bootout"


def test_service_status_is_json_and_truthful(cli, capsys):
    import json
    from nakagai_edge.cli import main
    holder, r = cli
    assert main(["service", "status"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc == {"service": "not installed", "unit": str(holder["svc"].path),
                   "root": None, "running": False, "pid": 0}
    main(["service", "install"])
    capsys.readouterr()
    assert main(["service", "status"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["service"] == "installed" and doc["running"] is True
    assert doc["pid"] == 4321 and doc["root"] == str(holder["root"])
    r.by_verb["print"] = (0, LAUNCHCTL_PRINT_LOADED)
    assert main(["service", "status"]) == 0
    assert json.loads(capsys.readouterr().out)["running"] is False


@pytest.mark.parametrize("action", ["install", "uninstall"])
def test_service_on_an_unsupported_platform_exits_1_with_one_line(
        cli, capsys, action):
    from nakagai_edge.cli import main
    holder, r = cli
    holder["svc"] = _svc(holder["svc"].home, "win32", runner=r)
    assert main(["service", action]) == 1
    err = capsys.readouterr().err
    assert "win32" in err and len(err.strip().splitlines()) == 1
    assert r.calls == []


def test_service_status_on_an_unsupported_platform(cli, capsys):
    import json
    from nakagai_edge.cli import main
    holder, r = cli
    holder["svc"] = _svc(holder["svc"].home, "win32", runner=r)
    assert main(["service", "status"]) == 0
    assert json.loads(capsys.readouterr().out)["service"] == "unsupported"
