"""How was this edge installed, and what upgrades it.

Pure on purpose: it takes two strings and returns three, so the whole table is
testable without a venv, a network, or a subprocess. The detection is a
heuristic and it can be wrong, which is exactly why `detect` returns a
DESCRIPTION alongside the commands. An owner who can see "project venv at
~/git/nakagai" next to the line can tell at a glance that we guessed wrong. An
owner handed only a command cannot.

Three strings rather than two because the follow-up differs by shape. Three
installs upgrade in place and then need the daemon bounced onto the new code;
uvx has nothing to upgrade in place and no `nakagai-edge` on PATH to bounce it
with, so its one command does both and there is no follow-up to print. The
table owns that asymmetry rather than the caller guessing at it.
"""

from pathlib import Path

FALLBACK = "pip install -U nakagai-edge"

# Every in-place upgrade leaves the OLD code still serving: the running daemon
# holds the port and has the previous version already imported.
THEN = "nakagai-edge restart"


def detect(prefix: str, argv0: str) -> tuple[str, str, str]:
    """(description, upgrade command, follow-up command).

    The follow-up is "" when the upgrade command already relaunches. Never
    raises: a status command that dies deciding how to phrase its own advice is
    worse than no advice.
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
        return f"uv tool install at {prefix}", "uv tool upgrade nakagai-edge", THEN
    if "uv" in parts and any(p.startswith("archive") for p in parts):
        # One command, and the asymmetry is the point. `uvx nakagai-edge@latest
        # run` cannot bind, because the daemon it is replacing still holds the
        # port; and a uvx user has no `nakagai-edge` on PATH to type a
        # follow-up with. `restart` stops the old daemon and relaunches through
        # argv[0], so running it under `uvx nakagai-edge@latest` is what leaves
        # latest serving. This is the install shape the README and every
        # packaged skill tell users to adopt, so a wrong line here costs more
        # than a wrong line anywhere else in this table.
        return ("uvx ephemeral cache (nothing to upgrade in place)",
                "uvx nakagai-edge@latest restart", "")
    if prefix.name in (".venv", "venv"):
        project = prefix.parent
        if (project / "pyproject.toml").exists():
            return (f"project venv at {project}",
                    f"cd {project} && uv lock --upgrade-package nakagai-edge "
                    f"&& uv sync", THEN)
        return f"virtualenv at {prefix} (no project above it)", FALLBACK, THEN
    return f"install at {prefix}", FALLBACK, THEN
