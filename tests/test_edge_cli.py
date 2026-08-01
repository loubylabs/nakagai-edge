"""The console script a stranger runs. It must not need the platform."""

import subprocess
import sys


def test_help_lists_every_subcommand():
    out = subprocess.run([sys.executable, "-m", "nakagai_edge.cli", "--help"],
                         capture_output=True, text=True, check=True).stdout
    for cmd in ("setup", "pair", "sync", "run", "login", "status", "listen"):
        assert cmd in out


def test_version_prints_and_exits_without_a_subcommand():
    """`--version` is the first thing anyone pastes into a bug report, so it has
    to work with no subcommand attached. The top-level parser requires one, and
    whether `--version` short-circuits that check is a detail of argparse's
    consume loop rather than something to take on faith."""
    from nakagai_edge.identity import package_version

    r = subprocess.run([sys.executable, "-m", "nakagai_edge.cli", "--version"],
                       capture_output=True, text=True)
    assert r.returncode == 0, f"stderr: {r.stderr}"
    assert package_version() in r.stdout


def test_setup_without_a_code_on_an_unpaired_edge_explains_itself(tmp_path, monkeypatch):
    """The message must stand on its own: an alpha user has no repo to read."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    r = subprocess.run([sys.executable, "-m", "nakagai_edge.cli", "setup",
                        "--platform", "https://api.nakag.ai"],
                       capture_output=True, text=True)
    assert r.returncode != 0
    assert "pairing code" in (r.stdout + r.stderr)
