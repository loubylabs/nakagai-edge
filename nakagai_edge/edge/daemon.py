"""Who is serving on the port, and can we prove it is ours.

A pid is not an identity. A pid read from a file written days ago can have
been recycled by the OS, and signalling a recycled pid kills a process that
was never ours. So nothing here signals anything on the strength of the
pidfile alone: `find_running` returns an edge only when the port is listening
AND the pid in the file is alive AND the file names that same port. Any other
combination returns a REASON instead, and the caller refuses.

The asymmetry between the two "no edge" answers is the useful part. An empty
reason means nothing is listening, so there is nothing to stop and a caller
may simply start one. A non-empty reason means something IS listening and we
cannot say what, which is the one case where a convenience command must get
out of the way and let a human look.

The record outlives the daemon. A stopping edge releases its claim by writing
pid 0 rather than deleting the file, because the port it served is a fact the
next `restart` needs and the process is not around to be asked. See
`release_pidfile`, which is also where the race with its own successor is
handled.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.identity import package_version

CONNECT_TIMEOUT_S = 0.35

# The port `nakagai-edge run` binds to unless told otherwise (cli.py, clients.py
# agree). A module-level name rather than a literal buried in find_running, so
# a test can substitute a port it actually controls instead of depending on
# whatever happens to be bound to 8330 on the machine running the suite.
DEFAULT_PORT = 8330


@dataclass(frozen=True)
class RunningEdge:
    pid: int
    port: int
    started_at: float
    version: str


def write_pidfile(state: EdgeState, *, port: int) -> None:
    """Written by run() at startup. Best effort: a daemon that cannot write
    this still serves, it just cannot be restarted by the command."""
    try:
        state.pid_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        state.pid_path.write_text(json.dumps(
            {"pid": os.getpid(), "port": int(port),
             "started_at": time.time(), "version": package_version()}))
    except Exception:
        pass


def read_pidfile(state: EdgeState) -> dict | None:
    """Missing, unreadable or malformed all read as None, which callers treat
    as "we do not know". Never as "nothing is running": those are different
    answers and only find_running is allowed to tell them apart."""
    try:
        doc = json.loads(state.pid_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(doc, dict):
        return None
    try:
        return {"pid": int(doc["pid"]), "port": int(doc["port"]),
                "started_at": float(doc.get("started_at") or 0.0),
                "version": str(doc.get("version") or "")}
    except (KeyError, TypeError, ValueError):
        return None


def release_pidfile(state: EdgeState) -> None:
    """Give up THIS process's claim on the port, and keep the port itself.

    Two hazards meet here, and both are races the obvious `unlink()` loses.

    Only our own record may be released. `run()` calls this from a `finally`
    that fires after the server has already let go of the port, so by the time
    this line runs a `restart` can have spawned a replacement that has already
    written its own pidfile. Unlinking unconditionally deletes the successor's
    record and leaves a live daemon with no proof of itself, which is the exact
    state `restart` refuses on and this whole feature exists to remove. A
    record naming someone else is therefore left exactly as found.

    And the port outlives the process that served it. An edge started on 9000
    that stops must come back on 9000, so the record is rewritten with pid 0
    rather than removed. Zero is never a live pid, so `find_running` can never
    claim a released record and nothing can be signalled on the strength of
    one; all that survives is the number `restart` needs.
    """
    doc = read_pidfile(state)
    if doc is None or doc["pid"] != os.getpid():
        return
    try:
        state.pid_path.write_text(json.dumps(
            {"pid": 0, "port": doc["port"], "started_at": 0.0,
             "version": doc["version"]}))
    except OSError:
        pass


def port_listening(port: int, host: str = "127.0.0.1") -> bool:
    try:
        with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_S):
            return True
    except OSError:
        return False


def pid_alive(pid: int) -> bool:
    """Signal 0 asks the kernel whether the pid exists without delivering
    anything. EPERM means it exists and belongs to someone else, which is
    still a live pid and still not one we may signal."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def find_running(state: EdgeState) -> tuple[RunningEdge | None, str]:
    """The one function allowed to say "that process is ours"."""
    doc = read_pidfile(state)
    port = (doc or {}).get("port", 0)
    if doc is not None and port_listening(port):
        if pid_alive(doc["pid"]):
            return RunningEdge(pid=doc["pid"], port=port,
                               started_at=doc["started_at"],
                               version=doc["version"]), ""
        # pid 0 is a released record: the daemon that wrote it stopped, and
        # something else has taken the port since. Naming "pid 0" would read
        # as a corrupt file rather than what it is.
        stale = (f"the recorded pid {doc['pid']} is gone" if doc["pid"] > 0
                 else "the record there names no live daemon")
        return None, (
            f"something is serving 127.0.0.1:{port} but {stale}, so this edge "
            f"cannot prove that process is its own. Stop it by hand and run "
            f"`nakagai-edge run` again.")
    # Nothing on the recorded port. Check the default before declaring the
    # coast clear, because the common case for a missing pidfile is a daemon
    # started before this file existed, and it is almost always on
    # DEFAULT_PORT.
    for candidate in (port, DEFAULT_PORT):
        if candidate and port_listening(candidate):
            return None, (
                f"something is serving 127.0.0.1:{candidate}, and there is no "
                f"pidfile naming it, so this edge cannot prove that process is "
                f"its own. A daemon started before this version predates the "
                f"pidfile: stop it by hand, then `nakagai-edge run`.")
    return None, ""


def armed_positions(state: EdgeState) -> list[dict]:
    """What a restart would stop watching, read straight off the ledger.

    Gates on "armed" and "firing" both. A firing position has an exit order
    in flight to the broker; cutting the watch there is the exact hazard
    `stop()`'s own docstring names, and `recover_interrupted` can only mark it
    `outcome_unknown` after the fact, it cannot undo a restart that already
    happened. Not gated on whether the brake is locally disarmed: a disarmed
    brake is a decision the owner made and can revisit, a restart is a
    process event that ends the watch either way, so what matters here is
    what is open, not what is currently being watched.
    """
    from nakagai_edge.edge.supervision import load
    rows = []
    for rec in load(state).values():
        if not isinstance(rec, dict):
            continue
        if rec.get("state") not in ("armed", "firing"):
            continue
        rows.append({"symbol": rec.get("symbol", "?"),
                     "qty": rec.get("quantity"),
                     "stop": rec.get("stop_price"),
                     "state": rec.get("state", "")})
    return rows


def stop(edge: RunningEdge, *, timeout_s: float = 10.0) -> bool:
    """SIGTERM, then wait for the port. Never SIGKILL.

    A daemon holding broker credentials can be mid-write to the audit journal
    or mid-flight on an exit order, and `recover_interrupted` can only report
    an interrupted exit if the process got far enough to record it. A
    convenience command does not get to make that call: it reports the refusal
    and lets a human decide.
    """
    try:
        os.kill(edge.pid, signal.SIGTERM)
    except ProcessLookupError:
        return not port_listening(edge.port)
    except OSError:
        return False
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if not port_listening(edge.port):
            return True
        time.sleep(0.2)
    return not port_listening(edge.port)


def relaunch_argv0() -> str:
    """The absolute path to the executable that invoked us.

    Absolute is not cosmetic. `spawn` hands the child `cwd=state.root`, and
    Popen resolves a relative argv[0] against the CHILD's cwd, not ours. So
    `.venv/bin/nakagai-edge restart`, typed from a project directory, would
    stop the daemon and then look for `~/.nakagai/edge/.venv/bin/nakagai-edge`,
    which is nothing, leaving the owner with no edge and an empty log. A bare
    name on PATH never showed the bug, because the shebang hands that case an
    absolute argv[0] already.

    `which` first, so a bare name resolves the way the shell resolved it, then
    abspath, which covers both a relative path with a directory component and
    a name `which` could not find at all.
    """
    return os.path.abspath(shutil.which(sys.argv[0]) or sys.argv[0])


def spawn(state: EdgeState, *, port: int) -> int:
    """Relaunch through argv[0], so the install shape is inherited.

    Whoever invoked `restart` invoked it from some install, and that install is
    the one they want serving. `uvx nakagai-edge@latest restart` therefore
    leaves latest running and `uv run nakagai-edge restart` leaves that
    project's venv running, with nothing recorded and no ps output parsed.

    The log is the other half of the problem this solves: a daemon started
    from a terminal that later closes has been writing into a closed stream,
    which is why there is nothing to read after the fact.

    Returns -1 rather than raising when the log cannot be opened or the
    launch itself fails. This is called after the old daemon (if any) is
    already stopped, so an exception escaping here would surface as a bare
    traceback with nothing left serving and no `edge.log` remedy printed;
    the caller checks the return value and reports it instead.
    """
    try:
        state.log_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        log = open(state.log_path, "ab", buffering=0)
    except OSError:
        return -1
    try:
        proc = subprocess.Popen(
            [relaunch_argv0(), "run", "--port", str(port)],
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, cwd=str(state.root))
    except OSError:
        return -1
    finally:
        log.close()
    return proc.pid


def wait_until_serving(port: int, *, timeout_s: float = 20.0) -> bool:
    """Proof, not assertion. A command that prints "restarted" without
    checking the port is theater."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if port_listening(port):
            return True
        time.sleep(0.3)
    return port_listening(port)
