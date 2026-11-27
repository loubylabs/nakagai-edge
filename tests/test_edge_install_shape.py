"""Four ways this package gets onto a machine, and the line that fixes each.
A heuristic that names what it detected is honest; one that emits a command
with no rationale is not, because a wrong guess is then invisible."""

from nakagai_edge.edge.freshness import UPGRADE_LINE
from nakagai_edge.edge.install_shape import detect, is_ephemeral

UVX = "/Users/x/.cache/uv/archive-v0/abc123"


def test_uvx_ephemeral_cache_moves_onto_the_durable_tool():
    """One install story: the durable uv tool. A uvx cache is not something
    to upgrade in place, and a login service pointed into it would break the
    day uv cleans its cache. So the advice installs the tool, then runs setup
    from it by absolute path (a fresh tool dir may not be on PATH yet), which
    installs the login service and replaces the uvx daemon."""
    desc, cmd, then = detect(UVX, f"{UVX}/bin/nakagai-edge")
    assert "uvx" in desc
    assert cmd == "uv tool install --upgrade nakagai-edge && uv tool update-shell"
    assert then == '"$(uv tool dir --bin)/nakagai-edge" setup'
    assert "uvx" not in cmd + then


def test_is_ephemeral_names_only_the_uvx_cache():
    assert is_ephemeral(UVX) is True
    assert is_ephemeral("/Users/x/.local/share/uv/tools/nakagai-edge") is False
    assert is_ephemeral("/usr") is False


def test_uv_tool_install():
    desc, cmd, then = detect("/Users/x/.local/share/uv/tools/nakagai-edge",
                             "/Users/x/.local/bin/nakagai-edge")
    assert "uv tool" in desc
    assert cmd == "uv tool upgrade nakagai-edge"
    assert then == "nakagai-edge restart"
    # The same line the version notice gives everywhere else.
    assert f"{cmd} && {then}" == UPGRADE_LINE


def test_project_venv_names_its_project(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='x'\n")
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    desc, cmd, then = detect(str(venv), str(venv / "bin" / "nakagai-edge"))
    assert str(tmp_path) in desc
    assert cmd == (f"cd {tmp_path} && uv lock --upgrade-package nakagai-edge "
                   "&& uv sync")
    assert then == "nakagai-edge restart"


def test_a_venv_with_no_project_above_it_is_not_a_project_install(tmp_path):
    """No pyproject.toml means `uv lock` has nothing to lock, and printing it
    anyway would send the owner into an error with no explanation."""
    venv = tmp_path / ".venv"
    (venv / "bin").mkdir(parents=True)
    desc, cmd, then = detect(str(venv), str(venv / "bin" / "nakagai-edge"))
    assert cmd == "pip install -U nakagai-edge"
    assert then == "nakagai-edge restart"
    assert "no project above it" in desc


def test_unknown_shape_falls_back():
    desc, cmd, then = detect("/usr", "/usr/bin/nakagai-edge")
    assert cmd == "pip install -U nakagai-edge"
    assert then == "nakagai-edge restart"
    assert desc


def test_every_shape_has_a_follow_up():
    """An upgrade on disk leaves the old code serving until something
    relaunches it, so no shape may drop its follow-up line."""
    shapes = [("/Users/x/.local/share/uv/tools/nakagai-edge", "/Users/x/.local/bin/x"),
              (UVX, f"{UVX}/bin/nakagai-edge"),
              ("/usr", "/usr/bin/nakagai-edge"),
              ("/opt/homebrew", "/opt/homebrew/bin/nakagai-edge")]
    assert all(detect(p, a)[2] for p, a in shapes)
