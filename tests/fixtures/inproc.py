"""One in-process MCP session, for the tests that need a REAL downstream.

`ConnectorHub` takes a `connect` seam that yields an initialized session, and
several tests hand it a fixture server rather than a stub so the guardrails,
the schema validation and the result serialization all run for real. This is
the one place that knows how to build such a session.

`mode="legacy"` on purpose: it runs the initialize handshake over the SDK's
in-memory transport, so `session.initialize_result` is populated exactly as it
is against a live broker. The default `auto` mode dispatches in-process without
a handshake, which would leave `Connection.server_info` empty here and nowhere
else.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.client import Client
from mcp.client.session import ClientSession


@asynccontextmanager
async def connected_session(server: Any) -> AsyncIterator[ClientSession]:
    """An initialized ClientSession talking to `server` in this process."""
    async with Client(server, mode="legacy") as client:
        yield client.session


def connect_to(server: Any):
    """A `ConnectorHub(connect=...)` seam that dials `server`, whatever the spec.

    The hub calls this with a `ConnectorSpec` and expects an async context
    manager back, so the spec is deliberately ignored: the test named the
    downstream when it chose the fixture.
    """
    async def connect(_spec):
        return connected_session(server)
    return connect
