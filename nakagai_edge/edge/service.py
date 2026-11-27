"""The edge as a login service: it starts by itself, and comes back after a crash.

An edge that lives in a terminal stops when the terminal closes or the machine
restarts, and nothing says so. So `setup` ends by handing the daemon to the
operating system's own per-user service manager: a LaunchAgent on macOS, a
systemd user unit on Linux. No other platform has one here (the daemon already
leans on POSIX calls), and there the edge serves in the foreground as before.

The manager restarts the daemon only after a FAILED exit. A daemon that exits
0 on purpose (another edge already holds the port, the edge is not paired)
stays down, rather than respawning into the same refusal forever, and a real
crash loop waits 30 seconds between tries.

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
import re
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

LABEL = "ai.nakag.edge"
UNIT = "nakagai-edge.service"
THROTTLE_S = 30
BOOTSTRAP_RETRY_S = 1.0

# (exit code, stdout). Output matters: `launchctl print` exits 0 for a job
# that is loaded but not running, so the state has to be read off the text.
Runner = Callable[[list[str]], tuple[int, str]]


def _manager(platform: str) -> str:
    """"launchd", "systemd", or "" for a platform with no service here."""
    if platform == "darwin":
        return "launchd"
    if platform.startswith("linux"):
        return "systemd"
    return ""


def service_path(raw: str | None = None) -> str:
    """The PATH the unit carries: this shell's, kept to absolute directories
    that exist, each once. launchd would otherwise hand the daemon
    /usr/bin:/bin:/usr/sbin:/sbin, and a stdio connector named by bare
    command would never resolve."""
    raw = os.environ.get("PATH", "") if raw is None else raw
    kept: list[str] = []
    for entry in raw.split(os.pathsep):
        if entry and os.path.isabs(entry) and os.path.isdir(entry) \
                and entry not in kept:
            kept.append(entry)
    return os.pathsep.join(kept)


# systemd: `%` is a specifier in every setting written here; `$` expands only
# in ExecStart; inside a double-quoted value `\` and `"` are escapes.
def _sd_pct(value: str) -> str:
    return value.replace("%", "%%")


def _sd_quoted(value: str, *, dollars: bool) -> str:
    value = _sd_pct(value.replace("\\", "\\\\").replace('"', '\\"'))
    return value.replace("$", "$$") if dollars else value


def unit_text(platform: str, argv0: str, port: int, root: str | Path, *,
              path: str, env_root: str = "") -> str:
    """The unit file body. `env_root` is NAKAGAI_EDGE_ROOT when the owner set
    one, and the unit carries it only then. `path` is the PATH the daemon
    runs with (see `service_path`). No other variable is copied: a secret in
    the owner's shell stays out of a file on disk."""
    root = str(root)
    log = str(Path(root) / "edge.log")
    manager = _manager(platform)
    if manager == "launchd":
        env = {"PATH": path}
        if env_root:
            env["NAKAGAI_EDGE_ROOT"] = env_root
        doc = {"Label": LABEL,
               "ProgramArguments": [argv0, "run", "--port", str(port)],
               "RunAtLoad": True,
               "KeepAlive": {"SuccessfulExit": False},
               "ThrottleInterval": THROTTLE_S,
               "WorkingDirectory": root,
               "EnvironmentVariables": env,
               "StandardOutPath": log, "StandardErrorPath": log}
        return plistlib.dumps(doc, sort_keys=True).decode()
    if manager == "systemd":
        env = ""
        if env_root:
            env += (f'Environment="NAKAGAI_EDGE_ROOT='
                    f'{_sd_quoted(env_root, dollars=False)}"\n')
        env += f'Environment="PATH={_sd_quoted(path, dollars=False)}"\n'
        return ("[Unit]\n"
                "Description=Nakagai edge: the broker connector\n"
                "\n"
                "[Service]\n"
                f'ExecStart="{_sd_quoted(argv0, dollars=True)}" run --port {port}\n'
                f"WorkingDirectory={_sd_pct(root)}\n"
                f"{env}"
                "Restart=on-failure\n"
                f"RestartSec={THROTTLE_S}\n"
                f"StandardOutput=append:{_sd_pct(log)}\n"
                f"StandardError=append:{_sd_pct(log)}\n"
                "\n"
                "[Install]\n"
                "WantedBy=default.target\n")
    raise ValueError(f"no login service on {platform}")


_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def env_warnings(specs: Iterable) -> list[str]:
    """One line per enabled connector that needs something a login service
    will not have: a shell variable (bearer token, header, `${VAR}`), or a
    stdio command found only through the shell's PATH. Never the values."""
    lines = []
    for spec in specs:
        if not spec.enabled:
            continue
        needs: list[str] = []
        if spec.auth.token_env:
            needs.append(f"${spec.auth.token_env}")
        needs += [f"${name}" for name in spec.auth.headers.values()]
        for value in [*spec.env.values(), *spec.args, spec.url or ""]:
            needs += [f"${m}" for m in _ENV_REF.findall(value)]
        command = ""
        if spec.command and not os.path.isabs(spec.command):
            command = f"command {spec.command!r} is not an absolute path"
        if not needs and not command:
            continue
        parts = []
        if needs:
            parts.append(f"reads {', '.join(dict.fromkeys(needs))} from your "
                         "shell, which the service does not have")
        if command:
            parts.append(command)
        lines.append(f"connector {spec.id!r}: {'; '.join(parts)}")
    return lines


def run_command(cmd: list[str]) -> tuple[int, str]:
    """The real runner: exit code and stdout, stderr discarded. A missing
    binary reads as failure (127), never as a traceback."""
    try:
        done = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL, text=True)
    except OSError:
        return 127, ""
    return done.returncode, done.stdout or ""


def _home() -> Path:
    return Path.home()


def _user() -> str:
    import getpass
    return getpass.getuser()


@dataclass(frozen=True)
class Service:
    platform: str
    home: Path
    uid: int
    user: str
    runner: Runner
    sleep: Callable[[float], None] = field(default=time.sleep)

    @classmethod
    def here(cls) -> "Service":
        """This machine's service, driving the real manager."""
        return cls(platform=sys.platform, home=_home(), uid=os.getuid(),
                   user=_user(), runner=run_command)

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

    def _rc(self, cmd: list[str]) -> int:
        return self.runner(cmd)[0]

    def _target(self) -> str:
        return f"gui/{self.uid}/{LABEL}"

    # ---- what is there ---------------------------------------------------

    def reachable(self) -> bool:
        """Whether this session can talk to the manager at all. Over SSH
        there is no gui domain, and a container or WSL often has no user bus;
        a unit written there would never load."""
        self._require()
        if self.manager == "launchd":
            return self._rc(["launchctl", "print", f"gui/{self.uid}"]) == 0
        return self._rc(["systemctl", "--user", "show-environment"]) == 0

    def unit_exists(self) -> bool:
        return self.supported and self.path.exists()

    def installed(self) -> bool:
        """The unit file is there AND the manager has it. A file alone is
        what a failed load leaves behind, and it serves nothing."""
        if not self.unit_exists():
            return False
        if self.manager == "launchd":
            return self._rc(["launchctl", "print", self._target()]) == 0
        return self._rc(["systemctl", "--user", "is-enabled", "--quiet",
                         UNIT]) == 0

    def state(self) -> str:
        """What `status` reports: installed | not installed | unsupported."""
        if not self.supported:
            return "unsupported"
        return "installed" if self.installed() else "not installed"

    @staticmethod
    def _systemd_value(text: str, key: str) -> str | None:
        for line in text.splitlines():
            if line.startswith(f"{key}="):
                return line[len(key) + 1:].replace("%%", "%")
        return None

    def installed_config(self) -> dict | None:
        """argv0, port and root read back off the unit on disk, or None."""
        if not self.unit_exists():
            return None
        try:
            if self.manager == "launchd":
                doc = plistlib.loads(self.path.read_bytes())
                args = doc["ProgramArguments"]
                return {"argv0": args[0], "port": int(args[-1]),
                        "root": doc["WorkingDirectory"]}
            text = self.path.read_text()
            m = re.match(r'"((?:[^"\\]|\\.)*)" run --port (\d+)$',
                         self._systemd_value(text, "ExecStart") or "")
            argv0 = re.sub(r"\\(.)", r"\1", m.group(1)).replace("$$", "$")
            return {"argv0": argv0, "port": int(m.group(2)),
                    "root": self._systemd_value(text, "WorkingDirectory")}
        except Exception:      # noqa: BLE001
            return None

    def _launchd_print(self) -> str:
        rc, out = self.runner(["launchctl", "print", self._target()])
        return out if rc == 0 else ""

    def running(self) -> bool:
        """Whether the manager has the edge process up. `launchctl print`
        exits 0 for a loaded job that is not running, so read its state."""
        self._require()
        if self.manager == "launchd":
            return bool(re.search(r"^\s*state = running\s*$",
                                  self._launchd_print(), re.M))
        return self._rc(["systemctl", "--user", "is-active", "--quiet",
                         UNIT]) == 0

    def pid(self) -> int:
        """The manager's own pid for the edge, 0 when it runs none."""
        self._require()
        if self.manager == "launchd":
            m = re.search(r"^\s*pid = (\d+)\s*$", self._launchd_print(), re.M)
            return int(m.group(1)) if m else 0
        rc, out = self.runner(["systemctl", "--user", "show", "-p", "MainPID",
                               "--value", UNIT])
        try:
            return int(out.strip()) if rc == 0 else 0
        except ValueError:
            return 0

    def linger_off(self) -> bool:
        """systemd only: True when the user's units stop at logout and do not
        start at boot. An answer that cannot be read is not "off"."""
        if self.manager != "systemd":
            return False
        rc, out = self.runner(["loginctl", "show-user", self.user, "-p", "Linger"])
        return rc == 0 and out.strip() == "Linger=no"

    # ---- changing it -----------------------------------------------------

    def unload(self) -> None:
        """Stop the unit if the manager is running it. Not loaded is fine:
        this is the step that makes a re-install replace rather than collide."""
        self._require()
        if self.manager == "launchd":
            self._rc(["launchctl", "bootout", self._target()])
        else:
            self._rc(["systemctl", "--user", "stop", UNIT])

    def install(self, *, argv0: str, port: int, root: str | Path) -> bool:
        """Write the unit and start it. True when the manager accepted it.

        Expects `unload` (and the caller's stop of any detached daemon) to
        have run first. A False return leaves the file in place; `discard`
        removes it.
        """
        self._require()
        body = unit_text(self.platform, argv0, port, root, path=service_path(),
                         env_root=os.environ.get("NAKAGAI_EDGE_ROOT", ""))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(body)
        if self.manager == "launchd":
            cmd = ["launchctl", "bootstrap", f"gui/{self.uid}", str(self.path)]
            if self._rc(cmd) == 0:
                return True
            # Right after a bootout, launchd can still be tearing the old job
            # down and answers "5: Input/output error". One short wait.
            self.sleep(BOOTSTRAP_RETRY_S)
            return self._rc(cmd) == 0
        return all(self._rc(cmd) == 0 for cmd in (
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", UNIT],
            # restart, not start: it starts a stopped unit and re-reads a
            # rewritten one that is still running.
            ["systemctl", "--user", "restart", UNIT]))

    def discard(self) -> None:
        """Remove a unit the manager would not load, so nothing on disk
        claims a service that does not exist."""
        self.unload()
        if self.manager == "systemd":
            self._rc(["systemctl", "--user", "disable", UNIT])
        self.path.unlink(missing_ok=True)

    def uninstall(self) -> bool:
        """Stop the unit and remove it. The edge is not running afterwards."""
        self.unload()
        if self.manager == "systemd":
            self._rc(["systemctl", "--user", "disable", UNIT])
        self.path.unlink(missing_ok=True)
        if self.manager == "systemd":
            self._rc(["systemctl", "--user", "daemon-reload"])
        return not self.path.exists()

    def restart(self) -> bool:
        self._require()
        if self.manager == "launchd":
            return self._rc(["launchctl", "kickstart", "-k", self._target()]) == 0
        return self._rc(["systemctl", "--user", "restart", UNIT]) == 0
