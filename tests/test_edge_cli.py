"""The console script a stranger runs. It must not need the platform."""

import json
import subprocess
import sys

import httpx

from nakagai_edge.cli import main
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle, sync_once


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


def _refusing_platform():
    """A platform still shipping the previous bundle shape."""
    return PlatformClient(
        "https://api.test", "t",
        transport=httpx.MockTransport(lambda r: httpx.Response(
            200, json={"bundle_version": "v2", "connectors": {"connectors": []}},
            headers={"etag": "v2"})))


def test_sync_names_the_refused_bundle_rather_than_blaming_the_network(
        tmp_path, monkeypatch, capsys):
    """A refused bundle leaves fetched_at exactly where an unreachable platform
    would, so `sync` used to answer "the platform did not answer" and send the
    owner checking a URL that was never the problem."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    monkeypatch.setattr("nakagai_edge.cli._edge_client",
                        lambda st: _refusing_platform())

    assert main(["sync"]) == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "did not answer" not in out["error"]
    assert "upgrade the platform" in out["error"].lower()
    assert str(BUNDLE_SCHEMA) in out["error"]


def test_status_is_quiet_about_the_schema_when_the_bundle_reads_fine(tmp_path,
                                                                     monkeypatch,
                                                                     capsys):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    assert main(["status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["policy_fresh"] is True
    assert "schema_error" not in out


def test_status_reports_a_schema_mismatch_and_names_the_fix(tmp_path, monkeypatch,
                                                            capsys):
    """`status` is where an owner goes when the edge has gone quiet. A refused
    bundle only shows up later as everything refusing on stale policy, so this
    has to say what actually happened and what to do about it."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    assert sync_once(state, _refusing_platform()) is False

    assert main(["status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["policy_fresh"] is False
    assert str(BUNDLE_SCHEMA) in out["schema_error"]
    assert "upgrade the platform" in out["schema_error"].lower()
    assert "nakagai-edge" in out["schema_error"]
