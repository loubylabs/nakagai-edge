"""The daemon actually SERVES: a real MCP request, over real HTTP, on the port
`run()` was given.

Every other test in this repo drives `create_edge_mcp`'s result by calling
`mcp.call_tool(...)` in process, which proves the tools work and proves nothing
at all about the transport. That is the gap this closes. An edge that imports
cleanly, registers all sixteen tools and then binds the wrong port, or never
binds at all, is indistinguishable from a healthy one until an owner's agent
cannot connect.

The port is asserted, not just "some port": the SDK's own default is 8000, and
the bind address now travels as an argument to `run_streamable_http_async`
rather than as fields on the server object, so a call that dropped it would
still start a server and still answer, just nowhere the agent is looking.
"""

import asyncio
import contextlib
import json
import socket

import httpx
import pytest

pytest.importorskip("mcp")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import Brake
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Reporter:
    async def snapshot_and_push(self):
        return {"connectors": []}


def _free_port() -> int:
    """A port nothing holds right now.

    Bound and released rather than hardcoded: the owner's real edge listens on
    8330 on this machine, and a test that took it would either fail or, worse,
    interrogate the live daemon.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_until_listening(port: int, deadline_s: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline_s
    while loop.time() < end:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            await asyncio.sleep(0.05)
            continue
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return
    raise AssertionError(f"nothing was listening on 127.0.0.1:{port} in {deadline_s}s")


def _edge(tmp_path):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    client = PlatformClient("https://api.test", "t",
                            transport=httpx.MockTransport(
                                lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    return create_edge_mcp(state, hub, client, audit, _Reporter(),
                           Brake(state, hub, client, audit)), hub


async def test_the_edge_answers_a_real_mcp_request_on_the_port_it_was_given(tmp_path):
    """Initialize, list, call: the whole handshake an agent performs, over the
    streamable-HTTP transport the daemon actually publishes.

    `get_connector_status` is the tool under test because it needs no broker
    and no platform: the registry synced above is empty, so the answer is a
    fact about this edge rather than about anything it could fail to reach.
    """
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    mcp, hub = _edge(tmp_path)
    port = _free_port()
    server = asyncio.create_task(
        mcp.run_streamable_http_async(host="127.0.0.1", port=port))
    try:
        await _wait_until_listening(port)

        # The URL `nakagai-edge connect` writes into a client's config, byte
        # for byte. A transport that only answered some other spelling of it
        # would pass an in-process test and fail every real agent.
        url = f"http://127.0.0.1:{port}/mcp/"
        async with streamable_http_client(url) as (read, write):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                assert init.server_info.name == "nakagai-edge"

                listed = {t.name for t in (await session.list_tools()).tools}
                assert {"call_connector", "get_connector_status",
                        "get_approval"} <= listed

                result = await session.call_tool("get_connector_status", {})
                assert result.is_error is not True
                body = json.loads(result.content[0].text)
                assert body["connectors"] == []
    finally:
        server.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server
        await hub.aclose()


def test_run_binds_the_loopback_address_and_the_port_it_was_handed(tmp_path,
                                                                  monkeypatch):
    """The other half: `run()` is what carries the operator's `--port` down to
    the call above, and it is the only place that names the host.

    Nothing here is asserted about serving, which the test above owns. The
    serve coroutine is replaced on the instance `run()` built, so the whole of
    `run()` executes for real up to that one call and then unwinds: promotion,
    the pidfile, the hub. `_loops` is stubbed because its six loops immediately
    dial a platform this test has no business standing up, and each of them is
    covered by its own module.
    """
    from nakagai_edge.edge import runtime

    state = EdgeState(tmp_path)
    # Port 1 refuses instantly and without DNS, so anything that does reach for
    # the platform fails locally rather than hanging on a name lookup.
    state.save_agent("http://127.0.0.1:1", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")

    monkeypatch.setattr(runtime, "sync_once", lambda *a, **k: None)

    async def _no_loops(*a, **k):
        return []
    monkeypatch.setattr(runtime, "_loops", _no_loops)

    served: list[dict] = []
    real_create = runtime.create_edge_mcp

    def recording_create(*args, **kwargs):
        mcp = real_create(*args, **kwargs)

        async def _serve(**kw):
            served.append(kw)
        mcp.run_streamable_http_async = _serve
        return mcp

    monkeypatch.setattr(runtime, "create_edge_mcp", recording_create)

    runtime.run(tmp_path, port=8331)

    assert served == [{"host": "127.0.0.1", "port": 8331}], (
        "run() no longer hands the serve call a loopback host and the port it "
        "was given; the daemon would listen somewhere the agent is not looking")
