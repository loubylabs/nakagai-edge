"""nakagai_edge stands alone: it is what a stranger installs from a wheel."""

import json
import subprocess
import sys


def test_edge_release_is_chat_protocol_030():
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]

    assert project["version"] == "0.3.0"


def test_readme_describes_listener_context_and_unbounded_promotion():
    from pathlib import Path

    root = Path(__file__).resolve().parent.parent
    readme = (root / "README.md").read_text()
    normalized = " ".join(readme.split())

    assert "Signals and approvals are emitted as non-actionable context" in normalized
    assert "Hidden or unregistered events advance the cursor without becoming stdout lines" in normalized
    assert "seventeen" not in readme


def test_edge_imports_nothing_from_the_platform():
    """A fresh interpreter, not this one: the platform's own tests run in the
    same pytest session and import `nakagai.*` long before this test does, so
    checking `sys.modules` in-process would pass or fail on suite ordering
    rather than on what nakagai_edge itself pulls in."""
    probe = (
        "import nakagai_edge.edge.runtime, nakagai_edge.hub, sys, json\n"
        "leaked = sorted(m for m in sys.modules "
        "if m == 'nakagai' or m.startswith('nakagai.'))\n"
        "print(json.dumps(leaked))\n"
    )
    result = subprocess.run([sys.executable, "-c", probe],
                            capture_output=True, text=True, check=True)
    leaked = json.loads(result.stdout)
    assert leaked == [], f"nakagai_edge reached back into the platform: {leaked}"


def test_the_base_hub_is_the_edges():
    from nakagai_edge.hub import ConnectorError, ConnectorHub, GuardrailDenied  # noqa: F401

    assert not hasattr(ConnectorHub, "decide")


def test_the_sdk_still_exposes_the_seam_tolerance_overrides():
    """TolerantClientSession overrides one SDK method and reads one private SDK
    attribute. Both are load-bearing.

    An SDK upgrade that renames either would leave our override dead: the
    payload Robinhood sends today would start raising again. That failure is
    loud rather than silent, so nothing bad reaches the ledger, but a red line
    here beats an owner discovering it through a blank Positions page. mcp 2.0
    is precisely that rename happening once already: `_validate_tool_result`
    became `validate_tool_result`.
    """
    import inspect

    import anyio
    from mcp.client.session import ClientSession
    from mcp.shared.message import SessionMessage

    from nakagai_edge.hub import TolerantClientSession

    assert inspect.iscoroutinefunction(ClientSession.validate_tool_result), (
        "the SDK's output-schema validation is no longer an async method by "
        "this name; TolerantClientSession.validate_tool_result overrides "
        "nothing and hard failures are back")

    # ClientSession.__init__ does no I/O: it only stores its arguments, so a
    # real instance can be built from a pair of unconnected memory streams
    # without entering the session's async context. A source-text search for
    # the attribute name would still pass if the SDK renamed it to something
    # that merely contains the old name (`_new_tool_output_schemas_v2`
    # contains `_tool_output_schemas`); checking a live instance cannot.
    read_writer, read_stream = anyio.create_memory_object_stream[
        SessionMessage | Exception](0)
    write_stream, write_reader = anyio.create_memory_object_stream[
        SessionMessage](0)
    session = TolerantClientSession(read_stream, write_stream, connector_id="guard")
    try:
        assert hasattr(session, "_tool_output_schemas"), (
            "the SDK no longer caches output schemas under this name; "
            "TolerantClientSession can no longer find the schema to classify")
    finally:
        read_writer.close()
        read_stream.close()
        write_stream.close()
        write_reader.close()

    assert TolerantClientSession.validate_tool_result is not \
        ClientSession.validate_tool_result, "the override went missing"


def test_the_mcp_pin_declares_a_2x_floor():
    """This package is written against mcp 2.x and only 2.x.

    The guard is on the DECLARED constraint, not on the resolved version. Both
    this repo and the platform resolve through a lock, so the resolved version
    here is always the one that already works; `uvx nakagai-edge`, which is
    what a user runs, resolves fresh at every invocation and takes whatever the
    floor allows. A floor that slipped back to 1.x would therefore ship an edge
    that pairs and syncs and then dies starting its own MCP server, and this
    lockfile would never say so.
    """
    import tomllib
    from pathlib import Path

    from packaging.requirements import Requirement

    root = Path(__file__).resolve().parent.parent
    deps = tomllib.loads((root / "pyproject.toml").read_text())["project"]["dependencies"]
    mcp = next(Requirement(d) for d in deps if Requirement(d).name == "mcp")

    assert mcp.specifier.contains("2.0.0"), (
        "the declared floor excludes mcp 2.0.0, which is the release this "
        "package is written against")
    assert not mcp.specifier.contains("1.28.1"), (
        "the declared floor still admits mcp 1.x, which ships neither "
        "mcp.server.mcpserver nor ClientSession.validate_tool_result")


def test_the_mcpserver_import_path_exists():
    """The other half: the floor above is only worth having while this is the
    path the code needs. edge/runtime.py builds its whole tool surface on this
    class, so a rename inside 2.x fails here first and says which name moved,
    rather than surfacing as a daemon that will not start."""
    from mcp.server.mcpserver import MCPServer  # noqa: F401
