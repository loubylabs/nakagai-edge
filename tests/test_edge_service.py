"""The login service: the edge starts by itself, and comes back after a crash.

Nothing here touches the real `launchctl` or `systemctl`. Every runner is a
recording fake, and the unit file lands under a tmp home, so the suite can run
on a machine whose own edge is a loaded service without disturbing it.
"""

import plistlib

import pytest

from nakagai_edge.edge.service import LABEL, UNIT, Service, unit_text

ARGV0 = "/Users/x/.local/bin/nakagai-edge"
ROOT = "/Users/x/.nakagai/edge"


class Runner:
    """Records every command; answers each with `rc` (or a per-verb override)."""

    def __init__(self, rc=0, **by_verb):
        self.calls: list[list[str]] = []
        self.rc = rc
        self.by_verb = by_verb

    def __call__(self, cmd):
        self.calls.append(list(cmd))
        verb = cmd[2] if cmd[:2] == ["systemctl", "--user"] else cmd[1]
        return self.by_verb.get(verb.replace("-", "_"), self.rc)


@pytest.fixture(autouse=True)
def default_root(monkeypatch):
    """The exact bodies below are the default-root case."""
    monkeypatch.delenv("NAKAGAI_EDGE_ROOT", raising=False)


def _svc(tmp_path, platform="darwin", runner=None):
    return Service(platform=platform, home=tmp_path, uid=501,
                   runner=runner or Runner())


# ---- unit_text: the exact file bodies --------------------------------------

MAC_PLIST = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>KeepAlive</key>
	<true/>
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
	<key>WorkingDirectory</key>
	<string>{ROOT}</string>
</dict>
</plist>
"""


def test_the_macos_launch_agent_body():
    assert unit_text("darwin", ARGV0, 8330, ROOT) == MAC_PLIST


def test_the_launch_agent_carries_the_edge_root_only_when_it_was_set():
    body = plistlib.loads(unit_text("darwin", ARGV0, 9000, "/tmp/e",
                                    env_root="/tmp/e").encode())
    assert body["EnvironmentVariables"] == {"NAKAGAI_EDGE_ROOT": "/tmp/e"}
    assert body["ProgramArguments"] == [ARGV0, "run", "--port", "9000"]
    assert "EnvironmentVariables" not in plistlib.loads(
        unit_text("darwin", ARGV0, 9000, "/tmp/e").encode())


LINUX_UNIT = """\
[Unit]
Description=Nakagai edge: the broker connector

[Service]
ExecStart="/home/x/.local/bin/nakagai-edge" run --port 8330
WorkingDirectory=/home/x/.nakagai/edge
Restart=always
RestartSec=5
StandardOutput=append:/home/x/.nakagai/edge/edge.log
StandardError=append:/home/x/.nakagai/edge/edge.log

[Install]
WantedBy=default.target
"""


def test_the_linux_systemd_user_unit_body():
    assert unit_text("linux", "/home/x/.local/bin/nakagai-edge", 8330,
                     "/home/x/.nakagai/edge") == LINUX_UNIT


def test_the_systemd_unit_carries_the_edge_root_only_when_it_was_set():
    body = unit_text("linux", "/b/nakagai-edge", 8330, "/tmp/e", env_root="/tmp/e")
    assert 'Environment="NAKAGAI_EDGE_ROOT=/tmp/e"\n' in body
    assert "Environment=" not in unit_text("linux", "/b/nakagai-edge", 8330, "/tmp/e")


def test_no_unit_for_a_platform_with_no_service():
    with pytest.raises(ValueError, match="win32"):
        unit_text("win32", ARGV0, 8330, ROOT)


# ---- where the unit lives, and whether it is there -------------------------

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


# ---- the runners, against a fake command runner ----------------------------

def test_macos_install_writes_the_plist_and_bootstraps_it(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, runner=r)
    assert svc.install(argv0=ARGV0, port=8330, root=ROOT) is True
    assert svc.path.read_text() == MAC_PLIST
    assert svc.state() == "installed"
    assert r.calls == [["launchctl", "bootstrap", "gui/501", str(svc.path)]]


def test_macos_install_reports_a_refused_bootstrap(tmp_path):
    svc = _svc(tmp_path, runner=Runner(bootstrap=5))
    assert svc.install(argv0=ARGV0, port=8330, root=ROOT) is False


def test_macos_unload_boots_out_and_ignores_a_unit_that_was_not_loaded(tmp_path):
    r = Runner(bootout=3)          # "No such process": nothing was loaded
    _svc(tmp_path, runner=r).unload()
    assert r.calls == [["launchctl", "bootout", f"gui/501/{LABEL}"]]


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


def test_macos_running_asks_launchctl_print(tmp_path):
    assert _svc(tmp_path, runner=Runner()).running() is True
    assert _svc(tmp_path, runner=Runner(print=113)).running() is False


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


def test_linux_restart_and_running(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, "linux", runner=r)
    assert svc.restart() is True
    assert svc.running() is True
    assert r.calls == [["systemctl", "--user", "restart", UNIT],
                       ["systemctl", "--user", "is-active", "--quiet", UNIT]]


def test_an_unsupported_platform_runs_nothing(tmp_path):
    r = Runner()
    svc = _svc(tmp_path, "win32", runner=r)
    with pytest.raises(ValueError):
        svc.install(argv0=ARGV0, port=8330, root=ROOT)
    assert r.calls == []


# ---- `nakagai-edge service install | uninstall | status` -------------------

@pytest.fixture
def cli(tmp_path, monkeypatch):
    """The CLI against a fake service on a tmp home, with nothing serving."""
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.edge import service

    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path / "edge"))
    r = Runner()
    holder = {"svc": Service(platform="darwin", home=tmp_path / "home", uid=501,
                             runner=r)}
    monkeypatch.setattr(service.Service, "here",
                        classmethod(lambda cls: holder["svc"]))
    monkeypatch.setattr(d, "find_running", lambda state: (None, ""))
    monkeypatch.setattr(d, "wait_until_serving", lambda port, **kw: True)
    monkeypatch.setattr(d, "DEFAULT_PORT", 8330)
    return holder, r


def test_service_install_writes_loads_and_waits_for_the_port(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    assert main(["service", "install"]) == 0
    assert holder["svc"].installed()
    assert [c[1] for c in r.calls] == ["bootout", "bootstrap"]
    assert "starts by itself at login" in capsys.readouterr().out


def test_service_install_twice_replaces_the_unit(cli):
    """Idempotent: the second install boots the first out before loading."""
    from nakagai_edge.cli import main
    holder, r = cli
    assert main(["service", "install"]) == 0
    assert main(["service", "install", "--port", "9100"]) == 0
    assert [c[1] for c in r.calls] == ["bootout", "bootstrap",
                                       "bootout", "bootstrap"]
    body = plistlib.loads(holder["svc"].path.read_bytes())
    assert body["ProgramArguments"][-1] == "9100"


def test_service_install_refuses_a_port_something_else_holds(cli, monkeypatch, capsys):
    import nakagai_edge.edge.daemon as d
    from nakagai_edge.cli import main
    holder, r = cli
    monkeypatch.setattr(d, "find_running",
                        lambda state: (None, "something is serving 127.0.0.1:8330"))
    assert main(["service", "install"]) != 0
    assert r.calls == [] and not holder["svc"].installed()
    assert "something is serving" in capsys.readouterr().err


def test_service_install_from_a_uvx_cache_refuses(cli, monkeypatch, capsys):
    import sys
    from nakagai_edge.cli import main
    holder, r = cli
    monkeypatch.setattr(sys, "prefix", "/Users/x/.cache/uv/archive-v0/abc")
    assert main(["service", "install"]) == 1
    assert r.calls == [] and not holder["svc"].installed()
    assert "uv tool install" in capsys.readouterr().err


def test_service_uninstall_removes_it(cli, capsys):
    from nakagai_edge.cli import main
    holder, r = cli
    main(["service", "install"])
    assert main(["service", "uninstall"]) == 0
    assert not holder["svc"].installed()
    assert r.calls[-1][1] == "bootout"


def test_service_status_is_json(cli, capsys):
    import json
    from nakagai_edge.cli import main
    holder, _ = cli
    assert main(["service", "status"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc == {"service": "not installed", "unit": str(holder["svc"].path),
                   "running": False}
    main(["service", "install"])
    capsys.readouterr()
    assert main(["service", "status"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["service"] == "installed" and doc["running"] is True


@pytest.mark.parametrize("action", ["install", "uninstall"])
def test_service_on_an_unsupported_platform_exits_1_with_one_line(
        cli, capsys, action):
    from nakagai_edge.cli import main
    holder, r = cli
    holder["svc"] = Service(platform="win32", home=holder["svc"].home, uid=0,
                            runner=r)
    assert main(["service", action]) == 1
    err = capsys.readouterr().err
    assert "win32" in err and len(err.strip().splitlines()) == 1
    assert r.calls == []


def test_service_status_on_an_unsupported_platform(cli, capsys):
    import json
    from nakagai_edge.cli import main
    holder, r = cli
    holder["svc"] = Service(platform="win32", home=holder["svc"].home, uid=0,
                            runner=r)
    assert main(["service", "status"]) == 0
    assert json.loads(capsys.readouterr().out)["service"] == "unsupported"
