"""Four ways this package gets onto a machine, and the line that fixes each.
A heuristic that names what it detected is honest; one that emits a command
with no rationale is not, because a wrong guess is then invisible."""

from nakagai_edge.edge.install_shape import detect


def test_installed_distribution_is_release_040():
    from importlib.metadata import version

    assert version("nakagai-edge") == "0.4.0"


def test_uvx_ephemeral_cache_is_one_command_with_no_follow_up():
    """The install shape the README and every packaged skill tell users to
    adopt, so this row matters more than the other three combined.

    Both halves of the two-command version were unrunnable. `uvx
    nakagai-edge@latest run` cannot bind, because the daemon it is replacing
    still holds the port. And a uvx user has no `nakagai-edge` on PATH, so a
    follow-up line names a command they cannot type. `restart` stops the old
    daemon and relaunches through argv[0], so under `uvx nakagai-edge@latest`
    it is what leaves latest serving.
    """
    desc, cmd, then = detect("/Users/x/.cache/uv/archive-v0/abc123",
                             "/Users/x/.cache/uv/archive-v0/abc123/bin/nakagai-edge")
    assert "uvx" in desc
    assert cmd == "uvx nakagai-edge@latest restart"
    assert then == ""
    assert "run" not in cmd.split()


def test_uv_tool_install():
    desc, cmd, then = detect("/Users/x/.local/share/uv/tools/nakagai-edge",
                             "/Users/x/.local/bin/nakagai-edge")
    assert "uv tool" in desc
    assert cmd == "uv tool upgrade nakagai-edge"
    assert then == "nakagai-edge restart"


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


def test_only_uvx_suppresses_the_follow_up():
    """The control case. A `then` of "" everywhere would pass the uvx test
    above and quietly drop the restart line from every other shape, leaving
    three installs upgraded on disk and still serving the old code."""
    shapes = [("/Users/x/.local/share/uv/tools/nakagai-edge", "/Users/x/.local/bin/x"),
              ("/usr", "/usr/bin/nakagai-edge"),
              ("/opt/homebrew", "/opt/homebrew/bin/nakagai-edge")]
    assert all(detect(p, a)[2] for p, a in shapes)
