"""The defect being fixed is that skills were absent from the ARTIFACT while
present in the repo, so every assertion here reads a built wheel. A test that
globbed nakagai_edge/skills/ in the source tree would have passed throughout the
entire period the bug existed."""
import re
import subprocess
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXPECTED = {"connect-edge", "pair-agent", "verify-edge",
            "daily-brief", "halt", "check-the-evidence",
            "nakagai-chat", "verify"}


@pytest.fixture(scope="module")
def wheel(tmp_path_factory) -> zipfile.ZipFile:
    # The `uv` binary, not `python -m uv`: uv is not a dependency of this
    # project, so there is no uv module in the venv to run. CI puts the binary
    # on PATH via astral-sh/setup-uv, and so does any machine that builds here.
    out = tmp_path_factory.mktemp("wheel")
    proc = subprocess.run(["uv", "build", "--wheel", "-o", str(out)],
                          cwd=REPO, capture_output=True, text=True)
    assert proc.returncode == 0, f"uv build failed:\n{proc.stderr}"
    built = list(out.glob("*.whl"))
    assert len(built) == 1, f"expected one wheel, got {built}"
    return zipfile.ZipFile(built[0])


def test_every_skill_ships_in_the_wheel(wheel):
    shipped = {
        name.split("/")[2]
        for name in wheel.namelist()
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md")
    }
    assert shipped == EXPECTED, (
        f"skill bundle drift: missing={EXPECTED - shipped}, "
        f"undocumented={shipped - EXPECTED}")


def test_readme_names_the_exact_shipped_skill_bundle():
    readme = (REPO / "README.md").read_text()
    section = readme.split("## Skills", 1)[1].split("## Live chat", 1)[0]
    documented = set(re.findall(r"^- \*\*`([^`]+)`\*\*:", section, re.MULTILINE))
    assert documented == EXPECTED, (
        f"README skill inventory drift: missing={EXPECTED - documented}, "
        f"extra={documented - EXPECTED}")


def test_shipped_skills_are_not_empty(wheel):
    for name in wheel.namelist():
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md"):
            assert len(wheel.read(name)) > 200, f"{name} is suspiciously small"


def test_every_skill_has_name_and_description(wheel):
    for name in wheel.namelist():
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md"):
            body = wheel.read(name).decode()
            assert body.startswith("---\n"), f"{name} has no frontmatter"
            head = body.split("---")[1]
            assert "name:" in head and "description:" in head, f"{name} frontmatter is incomplete"
