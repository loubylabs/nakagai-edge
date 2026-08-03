"""The skills that ship inside the wheel.

Access goes through importlib.resources rather than arithmetic on this module's
own path, so it keeps working from a zipimport and from the ephemeral
environment `uvx` builds. There is a test asserting this file never names the
module-path dunder at all, because the failure it guards against only shows up
in a packaging mode nobody runs locally.

Nothing here hands out a path: callers get content, and anything that wants a
file on disk copies it somewhere persistent itself. A path into a uvx cache is a
path that can vanish under its holder.
"""

from importlib import resources

_PACKAGE = "nakagai_edge.skills"


def _root():
    return resources.files(_PACKAGE)


def list_skills() -> list[str]:
    """Every packaged skill name, sorted."""
    return sorted(
        entry.name for entry in _root().iterdir()
        if entry.is_dir() and (entry / "SKILL.md").is_file())


def read_skill(name: str) -> str:
    """The raw SKILL.md body. Raises KeyError when there is no such skill."""
    path = _root() / name / "SKILL.md"
    if not path.is_file():
        raise KeyError(name)
    return path.read_text(encoding="utf-8")


def skill_description(name: str) -> str:
    """The `description:` line from the frontmatter, or "" when absent.

    Deliberately a line scan rather than a YAML parse: the frontmatter is two
    fields and a dependency for that would be silly.
    """
    lines = read_skill(name).splitlines()
    if not lines or lines[0].strip() != "---":
        return ""
    for line in lines[1:]:
        if line.strip() == "---":       # end of frontmatter, stop scanning
            break
        if line.startswith("description:"):
            return line.split(":", 1)[1].strip()
    return ""
