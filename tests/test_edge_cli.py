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


REFUSED = httpx.Response(200, headers={"etag": "v2"},
                         json={"bundle_version": "v2",         # no schema_version:
                               "connectors": {"connectors": []}})   # today's platform

GOOD = httpx.Response(200, headers={"etag": "v3"},
                      json={"bundle_version": "v3", "schema_version": BUNDLE_SCHEMA,
                            "connectors": {"connectors": [
                                {"id": "demo", "kind": "mcp-http",
                                 "url": "https://d.test/mcp", "enabled": True}]},
                            "signing_public_key": "k"})


def _platform_answering(*answers):
    """One client giving each answer in turn, then repeating the last. An
    httpx exception in the list is raised instead, standing in for an outage."""
    queue = list(answers)

    def handler(req):
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(answer, Exception):
            raise answer
        return answer
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(handler))
    return lambda state: client


def _refusing_platform():
    """A platform still shipping the previous bundle shape."""
    return PlatformClient("https://api.test", "t",
                          transport=httpx.MockTransport(lambda r: REFUSED))


def _synced_edge(tmp_path):
    """A paired edge with a readable registry already on disk, which is the
    edge that auto-updates into a platform it has outrun."""
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    return state


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


def test_a_clean_sync_reports_success(tmp_path, monkeypatch, capsys):
    """The ordinary path, on an edge that has never been refused anything. It
    is pinned here because the fuller `setup` suite skips without the platform
    package installed, and a refusal check that misreads "no refusal on record"
    would break every sync anyone runs."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _synced_edge(tmp_path)
    monkeypatch.setattr("nakagai_edge.cli._edge_client", _platform_answering(GOOD))

    assert main(["sync"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True and out["connectors"] == 1


# ---- what the SECOND sync says, which is where the reason turns sticky ----
#
# The refusal stays on record until a sync succeeds, so `sync` must report the
# refusal THIS call made, not whatever the edge was already carrying.


def test_a_second_refusal_still_reports_the_refusal(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _synced_edge(tmp_path)
    monkeypatch.setattr("nakagai_edge.cli._edge_client",
                        _platform_answering(REFUSED, REFUSED))

    assert main(["sync"]) == 1
    assert main(["sync"]) == 1
    out = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert "upgrade the platform" in out["error"].lower()


def test_an_outage_after_a_refusal_reports_the_outage(tmp_path, monkeypatch, capsys):
    """The owner upgrades the platform and retries while the network is still
    down. Repeating the schema message here reads as "the upgrade did not
    take", which is the same wrong fault named from the other side."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _synced_edge(tmp_path)
    monkeypatch.setattr("nakagai_edge.cli._edge_client",
                        _platform_answering(REFUSED, httpx.ConnectError("down")))

    assert main(["sync"]) == 1
    assert main(["sync"]) == 1
    out = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert "did not answer" in out["error"]
    assert "upgrade the platform" not in out["error"].lower()


def test_a_good_bundle_after_a_refusal_syncs_and_clears(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    _synced_edge(tmp_path)
    monkeypatch.setattr("nakagai_edge.cli._edge_client",
                        _platform_answering(REFUSED, GOOD))

    assert main(["sync"]) == 1
    assert main(["sync"]) == 0
    out = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert out["ok"] is True and out["connectors"] == 1

    assert main(["status"]) == 0
    assert "schema_error" not in json.loads(capsys.readouterr().out)


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
