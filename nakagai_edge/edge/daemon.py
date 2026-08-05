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
"""

import json
import os
import socket
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
    except OSError:
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


def clear_pidfile(state: EdgeState) -> None:
    try:
        state.pid_path.unlink()
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
        return None, (
            f"something is serving 127.0.0.1:{port} but the recorded pid "
            f"{doc['pid']} is gone, so this edge cannot prove that process is "
            f"its own. Stop it by hand and run `nakagai-edge run` again.")
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
