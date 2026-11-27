"""The edge as a login service: it starts by itself, and comes back after a crash.

An edge that lives in a terminal stops when the terminal closes or the machine
restarts, and nothing says so. So `setup` ends by handing the daemon to the
operating system's own per-user service manager: a LaunchAgent on macOS, a
systemd user unit on Linux. No other platform has one here (the daemon already
leans on POSIX calls), and there the edge serves in the foreground as before.

`unit_text` is pure: it takes the facts and returns the file body, so both
bodies are pinned byte for byte in a test. `Service` is the thin part that
writes the file and asks the manager to load it, and it takes its command
runner as an argument, so a test never reaches the real `launchctl` or
`systemctl`.

Stopping a detached daemon that already holds the port is the caller's job
(cli.py), between `unload` and `install`: this module never signals a
process, and two daemons on one port is exactly what that ordering prevents.
"""

import os
import plistlib
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

LABEL = "ai.nakag.edge"
UNIT = "nakagai-edge.service"

Runner = Callable[[list[str]], int]


def _manager(platform: str) -> str:
    """"launchd", "systemd", or "" for a platform with no service here."""
    if platform == "darwin":
        return "launchd"
    if platform.startswith("linux"):
        return "systemd"
    return ""


def unit_text(platform: str, argv0: str, port: int, root: str | Path, *,
              env_root: str = "") -> str:
    """The unit file body. `env_root` is NAKAGAI_EDGE_ROOT when the owner set
    one, and the unit carries it only then: a default root needs no variable,
    and writing one would pin a path the default would have found anyway."""
    log = str(Path(root) / "edge.log")
    manager = _manager(platform)
    if manager == "launchd":
        doc = {"Label": LABEL,
               "ProgramArguments": [argv0, "run", "--port", str(port)],
               "RunAtLoad": True, "KeepAlive": True,
               "WorkingDirectory": str(root),
               "StandardOutPath": log, "StandardErrorPath": log}
        if env_root:
            doc["EnvironmentVariables"] = {"NAKAGAI_EDGE_ROOT": env_root}
        return plistlib.dumps(doc, sort_keys=True).decode()
    if manager == "systemd":
        env = f'Environment="NAKAGAI_EDGE_ROOT={env_root}"\n' if env_root else ""
        return ("[Unit]\n"
                "Description=Nakagai edge: the broker connector\n"
                "\n"
                "[Service]\n"
                f'ExecStart="{argv0}" run --port {port}\n'
                f"WorkingDirectory={root}\n"
                f"{env}"
                "Restart=always\n"
                "RestartSec=5\n"
                f"StandardOutput=append:{log}\n"
                f"StandardError=append:{log}\n"
                "\n"
                "[Install]\n"
                "WantedBy=default.target\n")
    raise ValueError(f"no login service on {platform}")


def run_command(cmd: list[str]) -> int:
    """The real runner: the exit code, with the manager's chatter discarded.
    A missing binary reads as failure (127), never as a traceback."""
    try:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode
    except OSError:
        return 127


def _home() -> Path:
    return Path.home()


@dataclass(frozen=True)
class Service:
    platform: str
    home: Path
    uid: int
    runner: Runner

    @classmethod
    def here(cls) -> "Service":
        """This machine's service, driving the real manager."""
        return cls(platform=sys.platform, home=_home(), uid=os.getuid(),
                   runner=run_command)

    @property
    def manager(self) -> str:
        return _manager(self.platform)

    @property
    def supported(self) -> bool:
        return bool(self.manager)

    @property
    def path(self) -> Path:
        if self.manager == "launchd":
            return self.home / "Library" / "LaunchAgents" / f"{LABEL}.plist"
        return self.home / ".config" / "systemd" / "user" / UNIT

    def _require(self) -> None:
        if not self.supported:
            raise ValueError(f"no login service on {self.platform}")

    def state(self) -> str:
        """What `status` reports: installed | not installed | unsupported."""
        if not self.supported:
            return "unsupported"
        return "installed" if self.path.exists() else "not installed"

    def installed(self) -> bool:
        return self.state() == "installed"

    def _target(self) -> str:
        return f"gui/{self.uid}/{LABEL}"

    def unload(self) -> None:
        """Stop the unit if the manager is running it. Not loaded is fine:
        this is the step that makes a re-install replace rather than collide."""
        self._require()
        if self.manager == "launchd":
            self.runner(["launchctl", "bootout", self._target()])
        else:
            self.runner(["systemctl", "--user", "stop", UNIT])

    def install(self, *, argv0: str, port: int, root: str | Path) -> bool:
        """Write the unit and start it. True when the manager accepted it.

        Expects `unload` (and the caller's stop of any detached daemon) to
        have run first: launchd refuses to bootstrap a label it already has.
        """
        self._require()
        body = unit_text(self.platform, argv0, port, root,
                         env_root=os.environ.get("NAKAGAI_EDGE_ROOT", ""))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(body)
        if self.manager == "launchd":
            return self.runner(["launchctl", "bootstrap", f"gui/{self.uid}",
                                str(self.path)]) == 0
        return all(self.runner(cmd) == 0 for cmd in (
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", UNIT],
            # restart, not start: it starts a stopped unit and re-reads a
            # rewritten one that is still running.
            ["systemctl", "--user", "restart", UNIT]))

    def uninstall(self) -> bool:
        """Stop the unit and remove it. The edge is not running afterwards."""
        self.unload()
        if self.manager == "systemd":
            self.runner(["systemctl", "--user", "disable", UNIT])
        self.path.unlink(missing_ok=True)
        if self.manager == "systemd":
            self.runner(["systemctl", "--user", "daemon-reload"])
        return not self.path.exists()

    def restart(self) -> bool:
        self._require()
        if self.manager == "launchd":
            return self.runner(["launchctl", "kickstart", "-k", self._target()]) == 0
        return self.runner(["systemctl", "--user", "restart", UNIT]) == 0

    def running(self) -> bool:
        """Whether the manager has the unit loaded and running."""
        self._require()
        if self.manager == "launchd":
            return self.runner(["launchctl", "print", self._target()]) == 0
        return self.runner(["systemctl", "--user", "is-active", "--quiet",
                            UNIT]) == 0
