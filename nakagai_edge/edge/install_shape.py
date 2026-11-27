"""How was this edge installed, and what upgrades it.

Pure on purpose, apart from asking uv once where its cache is (injectable): it
takes two strings and returns three, so the whole table is testable without a
venv, a network, or a subprocess. The detection is a
heuristic and it can be wrong, which is exactly why `detect` returns a
DESCRIPTION alongside the commands. An owner who can see "project venv at
~/git/nakagai" next to the line can tell at a glance that we guessed wrong. An
owner handed only a command cannot.

Three strings rather than two because every in-place upgrade leaves the old
code serving until something relaunches it, and what relaunches it differs by
shape. The durable uv tool is the one install story; a uvx cache is advised
onto it rather than upgraded where it is.
"""

import functools
import shutil
import subprocess
from pathlib import Path

from nakagai_edge.edge.freshness import RESTART, UPGRADE

FALLBACK = "pip install -U nakagai-edge"

# Every in-place upgrade leaves the OLD code still serving: the running daemon
# holds the port and has the previous version already imported.
THEN = RESTART


def _ask_uv_cache_dir() -> str:
    """uv's own answer for where its cache lives ("" when uv is not on PATH
    or does not answer)."""
    uv = shutil.which("uv")
    if not uv:
        return ""
    try:
        done = subprocess.run([uv, "cache", "dir"], capture_output=True,
                              text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return ""
    return done.stdout.strip() if done.returncode == 0 else ""


# Asked once per process: it is a subprocess.
_uv_cache_dir = functools.cache(_ask_uv_cache_dir)


def is_ephemeral(prefix: str | Path, *, cache_dir: str | None = None) -> bool:
    """Whether this interpreter lives in uv's cache, which is what a `uvx`
    run is. Nothing durable (a login service above all) may point into it:
    uv deletes that directory whenever it cleans its cache.

    Asked of uv itself when it can be (`uv cache dir`, which honours
    UV_CACHE_DIR and the platform's cache location); the path-shape guess is
    only the fallback for a machine where uv does not answer.
    """
    cache = _uv_cache_dir() if cache_dir is None else cache_dir
    prefix = Path(prefix)
    if cache:
        return prefix.is_relative_to(Path(cache))
    parts = prefix.parts
    return ("uv" in parts and "tools" not in parts
            and any(p.startswith("archive") for p in parts))


def detect(prefix: str, argv0: str) -> tuple[str, str, str]:
    """(description, upgrade command, follow-up command).

    Never raises: a status command that dies deciding how to phrase its own
    advice is worse than no advice.
    """
    try:
        return _detect(Path(prefix), Path(argv0))
    except Exception:      # noqa: BLE001
        return "unrecognised install", FALLBACK, THEN


def _detect(prefix: Path, argv0: Path) -> tuple[str, str, str]:
    parts = prefix.parts
    # uv's tool dir is checked before its cache: a tool install lives under
    # .../uv/tools/... and the cache under .../uv/archive-v0/..., so the
    # narrower match has to win or every tool install reads as ephemeral.
    if "uv" in parts and "tools" in parts:
        return f"uv tool install at {prefix}", UPGRADE, THEN
    if is_ephemeral(prefix):
        # Onto the durable tool, then `setup` from it by absolute path: a
        # fresh tool dir may not be on PATH in this shell yet. A paired edge's
        # setup needs no code; it installs the login service, which stops the
        # uvx daemon first.
        return ("uvx ephemeral cache (not a durable install)",
                "uv tool install --upgrade nakagai-edge && uv tool update-shell",
                '"$(uv tool dir --bin)/nakagai-edge" setup')
    if prefix.name in (".venv", "venv"):
        project = prefix.parent
        if (project / "pyproject.toml").exists():
            return (f"project venv at {project}",
                    f"cd {project} && uv lock --upgrade-package nakagai-edge "
                    f"&& uv sync", THEN)
        return f"virtualenv at {prefix} (no project above it)", FALLBACK, THEN
    return f"install at {prefix}", FALLBACK, THEN
