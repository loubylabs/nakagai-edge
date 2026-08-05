"""How was this edge installed, and what upgrades it.

Pure on purpose: it takes two strings and returns two strings, so the whole
table is testable without a venv, a network, or a subprocess. The detection is
a heuristic and it can be wrong, which is exactly why `detect` returns a
DESCRIPTION alongside the command. An owner who can see "project venv at
~/git/nakagai" next to the line can tell at a glance that we guessed wrong. An
owner handed only a command cannot.
"""

from pathlib import Path

FALLBACK = "pip install -U nakagai-edge"


def detect(prefix: str, argv0: str) -> tuple[str, str]:
    """(description, upgrade command). Never raises: a status command that
    dies deciding how to phrase its own advice is worse than no advice."""
    try:
        return _detect(Path(prefix), Path(argv0))
    except Exception:      # noqa: BLE001
        return "unrecognised install", FALLBACK


def _detect(prefix: Path, argv0: Path) -> tuple[str, str]:
    parts = prefix.parts
    # uv's tool dir is checked before its cache: a tool install lives under
    # .../uv/tools/... and the cache under .../uv/archive-v0/..., so the
    # narrower match has to win or every tool install reads as ephemeral.
    if "uv" in parts and "tools" in parts:
        return f"uv tool install at {prefix}", "uv tool upgrade nakagai-edge"
    if "uv" in parts and any(p.startswith("archive") for p in parts):
        return ("uvx ephemeral cache (nothing to upgrade in place)",
                "uvx nakagai-edge@latest run")
    if prefix.name in (".venv", "venv"):
        project = prefix.parent
        if (project / "pyproject.toml").exists():
            return (f"project venv at {project}",
                    f"cd {project} && uv lock --upgrade-package nakagai-edge "
                    f"&& uv sync")
        return f"virtualenv at {prefix} (no project above it)", FALLBACK
    return f"install at {prefix}", FALLBACK
