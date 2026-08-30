"""The console script a stranger runs. It must not need the platform."""

import json
import subprocess
import sys

import httpx
import pytest

from nakagai_edge.cli import main
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle, sync_once


def test_help_lists_every_subcommand():
    out = subprocess.run([sys.executable, "-m", "nakagai_edge.cli", "--help"],
                         capture_output=True, text=True, check=True).stdout
    for cmd in ("setup", "pair", "sync", "run", "login", "status", "listen",
                "connect"):
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


@pytest.mark.parametrize(("wake_command", "expected"), [
    ("agent run", True),
    ("", False),
    ("   ", False),
])
def test_listen_configures_candidate_wake_attestation_from_parsed_command(
        tmp_path, monkeypatch, wake_command, expected):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    configured = []

    class Client:
        def close(self):
            pass

    class Listener:
        def __init__(self, client, root, **kwargs):
            pass

        def run(self):
            return 0

    def edge_client(state, *, candidate_wake=False):
        configured.append(candidate_wake)
        return Client()

    monkeypatch.setattr("nakagai_edge.cli._edge_client", edge_client)
    monkeypatch.setattr("nakagai_edge.edge.listen.ChannelListener", Listener)

    args = ["listen"]
    if wake_command:
        args.extend(["--wake-command", wake_command])
    assert main(args) == 0
    assert configured == [expected]


def test_setup_without_a_code_on_an_unpaired_edge_explains_itself(tmp_path, monkeypatch):
    """The message must stand on its own: an alpha user has no repo to read."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    r = subprocess.run([sys.executable, "-m", "nakagai_edge.cli", "setup",
                        "--platform", "https://api.nakag.ai"],
                       capture_output=True, text=True)
    assert r.returncode != 0
    assert "pairing code" in (r.stdout + r.stderr)


def test_pair_diagnoses_the_platform_before_spending_the_code(tmp_path, monkeypatch,
                                                              capsys):
    """`pair --platform` is required and has no default, so it is the likeliest
    place to mistype a URL. Without preflight the user gets a bare
    `pairing failed (301)` and no idea what to do about it. This asserts the
    order too: a pairing code is single use, and a preflight that runs after
    the POST would burn it against the wrong server."""
    from nakagai_edge.edge.client import EdgeClientError

    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))

    def refuse(url, **kw):
        raise EdgeClientError(f"{url} is the nakagai API, but it does not "
                              "serve plain http")

    def must_not_pair(*a, **k):
        raise AssertionError("the code was spent despite a failed preflight")

    monkeypatch.setattr("nakagai_edge.edge.preflight.check_platform", refuse)
    monkeypatch.setattr("nakagai_edge.edge.client.pair", must_not_pair)

    assert main(["pair", "CODE1", "--platform", "http://api.nakag.ai"]) == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "does not serve plain http" in out["error"]


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
    # The final `status` call below routes through find_running, and a
    # tmp_path root with no pidfile falls back to DEFAULT_PORT: pin it to a
    # dead port rather than the real edge this machine happens to run on
    # 127.0.0.1:8330. latest_release is mocked too, so this stays offline
    # rather than making a live call to pypi.org on every run.
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
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
    # A dead port: `status` now also calls find_running, and a tmp_path root
    # with no pidfile would otherwise fall back to the real daemon this
    # machine happens to be running on 127.0.0.1:8330. latest_release is
    # mocked too, so this test does not depend on a live call to pypi.org.
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
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
    # Same reason as the test above: keep find_running off the real daemon,
    # and latest_release off the real pypi.org.
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
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


def test_status_keeps_its_documented_keys_and_gains_the_version_picture(
        tmp_path, monkeypatch, capsys):
    """docs/internal/EDGE.md and the verify-edge skill both tell a reader to
    expect `paired` and `policy_fresh`, so those are a contract. The advisory
    goes to stderr so a pipe into jq keeps working.

    DEFAULT_PORT is repointed at a dead port: this machine has a real edge
    listening on 127.0.0.1:8330, and a tmp_path root has no pidfile, so
    find_running's fallback check would otherwise reach out to that real
    daemon instead of reading back "nothing running" for a root that never
    started one.
    """
    import json as _json

    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: "9.9.9")
    assert main(["status"]) == 0
    out = capsys.readouterr()
    doc = _json.loads(out.out)
    assert "paired" in doc and "policy_fresh" in doc
    assert doc["latest_version"] == "9.9.9"
    assert doc["server_version"] == ""          # nothing synced in a tmp root
    assert doc["daemon"]["running"] is False
    assert doc["install"] and doc["upgrade"]
    assert "9.9.9" in out.err                   # the advisory, not on stdout


def test_status_carries_the_synced_edge_version_through(tmp_path, monkeypatch, capsys):
    """The version picture is only worth having if it survives the trip from
    the cached bundle to the printed JSON. A mutation that made
    server_edge_version always return "" would still pass the "nothing
    synced" assertion in the test above, since "" is also the right answer
    there; only a bundle that actually names a version can catch it."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k", "edge_version": "0.4.0"}, "v1")
    assert main(["status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["server_version"] == "0.4.0"


def test_status_says_nothing_on_stderr_when_current(tmp_path, monkeypatch, capsys):
    """The control case. Without it, an advisory printed unconditionally would
    pass the test above and nag on every single run."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
    assert main(["status"]) == 0
    assert capsys.readouterr().err == ""


def test_status_tells_being_current_apart_from_having_no_network(
        tmp_path, monkeypatch, capsys):
    """Two very different facts that a single empty string conflates.

    An owner reading `latest_version: ""` cannot tell whether they are on the
    newest release or whether pypi.org simply never answered, and the second
    is the one worth acting on. So the key carries the index's own answer,
    which equals `version` when this edge is current, and is null only when
    the index said nothing at all.
    """
    from nakagai_edge.identity import package_version

    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)

    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: package_version())
    assert main(["status"]) == 0
    current = json.loads(capsys.readouterr().out)
    assert current["latest_version"] == package_version()

    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
    assert main(["status"]) == 0
    offline = json.loads(capsys.readouterr().out)
    assert offline["latest_version"] is None

    assert current["latest_version"] != offline["latest_version"]


def test_status_and_restart_tell_one_story_about_the_port(
        tmp_path, monkeypatch, capsys):
    """A pidfile naming a pid that is gone, on a port something else holds.

    `restart` refuses, saying something IS serving that port. `status` used to
    throw the same sentence away and print `daemon.running: false`, so the two
    commands contradicted each other about one machine and the owner had no
    way to tell which was right. The note is that sentence, verbatim.

    Nothing is signalled to reach either answer: the pid in the record is
    dead, which is precisely why both commands refuse to act on it.
    """
    import socket

    import nakagai_edge.edge.daemon as d

    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
    monkeypatch.setattr(d, "spawn", lambda st, *, port: pytest.fail(
        "restart got past a port it cannot claim and launched a real daemon"))
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    # A generous backlog, because both commands below probe this port and
    # nothing ever accepts. `port_listening` completes a handshake and closes
    # its side each time, and on a listen(1) socket the second probe finds the
    # queue full and times out, which reads back as "the port freed" and would
    # send this test down the cold-start path instead.
    sock.listen(64)
    port = sock.getsockname()[1]
    try:
        state = EdgeState(tmp_path)
        state.pid_path.parent.mkdir(parents=True, exist_ok=True)
        state.pid_path.write_text(json.dumps(
            {"pid": 2**22, "port": port, "started_at": 1.0, "version": "0.2.1"}))

        assert main(["status"]) == 0
        note = json.loads(capsys.readouterr().out)["daemon"].get("note", "")
        assert str(port) in note

        assert main(["restart"]) == 2          # the refusal, not a failed launch
        assert note in capsys.readouterr().err
    finally:
        sock.close()


def test_the_note_is_empty_when_nothing_is_listening(tmp_path, monkeypatch, capsys):
    """The control for the test above. A note that always carried text would
    pass it and turn "your edge is not running", which is ordinary, into a
    permanent alarm about a port nobody is on."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: None)
    assert main(["status"]) == 0
    daemon_doc = json.loads(capsys.readouterr().out)["daemon"]
    assert daemon_doc["running"] is False
    assert "note" in daemon_doc
    assert daemon_doc["note"] == ""


def test_the_uvx_advisory_is_a_single_runnable_command(tmp_path, monkeypatch, capsys):
    """The install shape the README and every packaged skill tell users to
    adopt, so a wrong line here is the one that reaches the most people.

    `uvx nakagai-edge@latest run` cannot bind while the daemon it replaces
    holds the port, and a uvx user has no `nakagai-edge` on PATH to type a
    follow-up with, so the "then" line has to be suppressed for this shape
    rather than printed unrunnable.
    """
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: "9.9.9")
    monkeypatch.setattr(sys, "prefix", "/Users/x/.cache/uv/archive-v0/abc123")
    assert main(["status"]) == 0
    out = capsys.readouterr()
    assert json.loads(out.out)["upgrade"] == "uvx nakagai-edge@latest restart"
    assert "uvx nakagai-edge@latest restart" in out.err
    assert "then:" not in out.err


def test_every_other_shape_keeps_its_restart_line(tmp_path, monkeypatch, capsys):
    """The control. Suppressing the follow-up everywhere would pass the uvx
    test above and leave three install shapes upgraded on disk while the old
    code goes on serving, which is the failure that is hardest to notice."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    monkeypatch.setattr("nakagai_edge.edge.daemon.DEFAULT_PORT", 1)
    monkeypatch.setattr("nakagai_edge.edge.freshness.latest_release",
                        lambda **kw: "9.9.9")
    monkeypatch.setattr(sys, "prefix", "/usr")
    assert main(["status"]) == 0
    err = capsys.readouterr().err
    assert "then:    nakagai-edge restart" in err
