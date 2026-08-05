"""Four ways this package gets onto a machine, and the one line that fixes
each. A heuristic that names what it detected is honest; one that emits a
command with no rationale is not, because a wrong guess is then invisible."""

from nakagai_edge.edge.install_shape import detect


def test_uvx_ephemeral_cache():
    desc, cmd = detect("/Users/x/.cache/uv/archive-v0/abc123",
                       "/Users/x/.cache/uv/archive-v0/abc123/bin/nakagai-edge")
    assert "uvx" in desc
    assert cmd == "uvx nakagai-edge@latest run"


def test_uv_tool_install():
    desc, cmd = detect("/Users/x/.local/share/uv/tools/nakagai-edge",
                       "/Users/x/.local/bin/nakagai-edge")
    assert "uv tool" in desc
    assert cmd == "uv tool upgrade nakagai-edge"


def test_project_venv_names_its_project(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    desc, cmd = detect(str(venv), str(venv / "bin" / "nakagai-edge"))
    assert str(tmp_path) in desc
    assert cmd == (f"cd {tmp_path} && uv lock --upgrade-package nakagai-edge "
                   "&& uv sync")


def test_a_venv_with_no_project_above_it_is_not_a_project_install(tmp_path):
    """No pyproject.toml means `uv lock` has nothing to lock, and printing it
    anyway would send the owner into an error with no explanation."""
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    desc, cmd = detect(str(venv), str(venv / "bin" / "nakagai-edge"))
    assert cmd == "pip install -U nakagai-edge"
    assert "no project above it" in desc


def test_unknown_shape_falls_back():
    desc, cmd = detect("/usr", "/usr/bin/nakagai-edge")
    assert cmd == "pip install -U nakagai-edge"
    assert desc
