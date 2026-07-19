"""Nakagai's outbound MCP client runtime.

One `Connection` per enabled connector, each owned end-to-end by a single
asyncio task: that task enters the transport context, initializes the session,
snapshots the tool list, then parks on a stop event. Teardown happens *inside*
the same task. This matters: anyio cancel scopes may not be exited from a
different task than the one that entered them, which is why an
AsyncExitStack-across-tasks pool (and `ClientSessionGroup`, which also can't
carry an httpx OAuth `auth`) is the wrong shape here.

Calls from other tasks are safe: `ClientSession.call_tool` only writes to memory
streams owned by the session.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from nakagai_edge.config import ConnectorSpec, load_specs, resolve_env_refs
from nakagai_edge.guardrails import annotate_tools, evaluate

RECONNECT_BACKOFF_S = 30.0


class ConnectorError(Exception):
    """A connector could not be reached, or refused the call."""


class GuardrailDenied(ConnectorError):
    """The call never left Nakagai: a guardrail rejected it."""


def describe_exception(e: BaseException) -> str:
    """A one-line cause, digging through anyio's ExceptionGroup wrappers.

    The MCP transports run inside task groups, so a plain 404 or a TLS failure
    surfaces as "ExceptionGroup: unhandled errors in a TaskGroup (1
    sub-exception)". An operator debugging a connector needs the leaf.
    """
    while isinstance(e, BaseExceptionGroup) and e.exceptions:
        e = e.exceptions[0]
    detail = str(e)
    # httpx status errors bury the interesting part in a multi-line blob.
    first_line = detail.split("\n", 1)[0].strip()
    return f"{type(e).__name__}: {first_line}" if first_line else type(e).__name__


@dataclass
class Connection:
    spec: ConnectorSpec
    status: str = "disconnected"      # disconnected | connecting | connected | error
    tools: list[dict] = field(default_factory=list)
    server_info: dict = field(default_factory=dict)
    last_error: str = ""
    last_used: float = 0.0
    connected_at: float = 0.0
    error_until: float = 0.0          # backoff deadline after a failure
    _session: Any = None
    _task: asyncio.Task | None = None
    _ready: asyncio.Future | None = None
    _stop: asyncio.Event | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.spec.id, "name": self.spec.name or self.spec.id,
            "kind": self.spec.kind, "role": self.spec.role,
            "enabled": self.spec.enabled, "status": self.status,
            "tool_count": len(self.tools), "last_error": self.last_error,
            "server_info": self.server_info,
            "allow_writes": self.spec.guardrails.allow_writes,
            "auth_mode": self.spec.auth.mode,
        }


def serialize_result(result) -> dict:
    """A CallToolResult as JSON-safe data for the upstream agent.

    Downstream errors are surfaced as `is_error: true` with their text, not
    raised: an agent should see the broker's own message.
    """
    texts, blocks = [], []
    for block in getattr(result, "content", []) or []:
        kind = getattr(block, "type", None)
        if kind == "text":
            texts.append(block.text)
        else:
            blocks.append(block.model_dump(mode="json") if hasattr(block, "model_dump")
                          else {"type": kind})
    out: dict = {"is_error": bool(getattr(result, "isError", False))}
    joined = "\n".join(texts)
    if texts:
        # Downstream servers usually hand back a JSON document as text; parse it
        # so the agent gets data rather than a string holding data.
        try:
            out["data"] = json.loads(joined)
        except (json.JSONDecodeError, ValueError):
            out["text"] = joined
    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        # FastMCP mirrors most results into structuredContent: {"result": text}
        # for plain tools, or a copy of the JSON already parsed into `data`. The
        # upstream agent pays tokens for every byte; keep it only when it says
        # something the text didn't.
        data = out.get("data")
        redundant = structured == {"result": joined} or (
            "data" in out and structured in (data, {"result": data}))
        if not redundant:
            out["structured"] = structured
    if blocks:
        out["content"] = blocks
    return out


class ConnectorHub:
    """Lazy, cached connections to every configured downstream MCP server."""

    def __init__(self, root: Path, connect=None, approvals=None) -> None:
        self.root = Path(root)
        self._conns: dict[str, Connection] = {}
        self._lock = asyncio.Lock()
        self._connect = connect or self._default_connect  # test seam
        self._approvals = approvals

    @property
    def approvals(self):
        """Lazy: the journal lives under the root, which may not exist at init."""
        if self._approvals is None:
            from nakagai_edge.approvals import ApprovalQueue
            self._approvals = ApprovalQueue(self.root / "results" / "approvals.jsonl")
        return self._approvals

    # ---- registry -------------------------------------------------------

    def _registry_path(self) -> Path:
        return self.root / "config" / "connectors.yaml"

    def load_specs(self) -> dict[str, ConnectorSpec]:
        path = self._registry_path()
        if not path.exists():
            return {}
        return load_specs(yaml.safe_load(path.read_text()) or {})

    def spec(self, connector_id: str) -> ConnectorSpec:
        specs = self.load_specs()
        if connector_id not in specs:
            raise ConnectorError(f"no connector {connector_id!r}")
        return specs[connector_id]

    # ---- connection lifecycle -------------------------------------------

    async def _default_connect(self, spec: ConnectorSpec):
        """Yield an initialized ClientSession for `spec` (async context manager)."""
        from contextlib import asynccontextmanager

        # Fail before spawning: a `${VAR}` reference to an unset variable must
        # never launch a broker process with blank credentials.
        env = resolve_env_refs(spec.env, spec.id) if spec.transport == "stdio" else {}

        @asynccontextmanager
        async def _open():
            from mcp.client.session import ClientSession

            from nakagai_edge.identity import client_info

            timeout = timedelta(seconds=spec.timeout_s)
            if spec.transport == "stdio":
                from mcp.client.stdio import StdioServerParameters, stdio_client
                params = StdioServerParameters(command=spec.command, args=spec.args,
                                               env=env or None)
                async with stdio_client(params) as (read, write):
                    async with ClientSession(read, write, read_timeout_seconds=timeout,
                                             client_info=client_info()) as s:
                        await s.initialize()
                        yield s
            else:
                from mcp.client.streamable_http import streamable_http_client

                from nakagai_edge.auth import build_http_client
                http_client = build_http_client(spec, self.root)
                async with streamable_http_client(spec.url, http_client=http_client) as (
                        read, write, _get_session_id):
                    async with ClientSession(read, write, read_timeout_seconds=timeout,
                                             client_info=client_info()) as s:
                        await s.initialize()
                        yield s

        return _open()

    async def _run_connection(self, conn: Connection) -> None:
        """Own one downstream session for its whole life, in one task."""
        spec = conn.spec
        try:
            async with await self._connect(spec) as session:
                listed = await session.list_tools()
                conn._session = session
                conn.tools = [t.model_dump(mode="json") for t in listed.tools]
                init = getattr(session, "_init_result", None) or getattr(
                    session, "initialize_result", None)
                if init is not None and hasattr(init, "serverInfo"):
                    conn.server_info = init.serverInfo.model_dump(mode="json")
                conn.status, conn.last_error = "connected", ""
                conn.connected_at = time.monotonic()
                if conn._ready and not conn._ready.done():
                    conn._ready.set_result(True)
                await conn._stop.wait()          # teardown happens on the way out
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            # BaseException, not Exception: anyio raises BaseExceptionGroup,
            # which is not an Exception subclass and would otherwise escape.
            if isinstance(e, KeyboardInterrupt | SystemExit):
                raise
            conn.status = "error"
            conn.last_error = describe_exception(e)
            conn.error_until = time.monotonic() + RECONNECT_BACKOFF_S
            if conn._ready and not conn._ready.done():
                conn._ready.set_exception(ConnectorError(
                    f"connector {spec.id!r} failed to connect: {conn.last_error}"))
        finally:
            conn._session = None
            if conn.status != "error":
                conn.status = "disconnected"

    async def _ensure_connected(self, spec: ConnectorSpec) -> Connection:
        async with self._lock:
            conn = self._conns.get(spec.id)
            if conn and conn.status == "connected" and conn._task and not conn._task.done():
                conn.spec = spec
                return conn
            if conn and conn.status == "error" and time.monotonic() < conn.error_until:
                raise ConnectorError(
                    f"connector {spec.id!r} is in backoff after: {conn.last_error}")
            if conn and conn._task and not conn._task.done():
                await self._disconnect(conn)

            conn = Connection(spec=spec, status="connecting")
            conn._ready = asyncio.get_running_loop().create_future()
            conn._stop = asyncio.Event()
            conn._task = asyncio.create_task(self._run_connection(conn),
                                             name=f"connector-{spec.id}")
            self._conns[spec.id] = conn

        try:
            await asyncio.wait_for(conn._ready, timeout=spec.timeout_s)
        except asyncio.TimeoutError as e:
            conn.status, conn.last_error = "error", "timed out during connect"
            conn.error_until = time.monotonic() + RECONNECT_BACKOFF_S
            await self._disconnect(conn)
            raise ConnectorError(f"connector {spec.id!r} timed out during connect") from e
        return conn

    async def _disconnect(self, conn: Connection) -> None:
        if conn._stop:
            conn._stop.set()
        if conn._task and not conn._task.done():
            try:
                await asyncio.wait_for(asyncio.shield(conn._task), timeout=5.0)
            except (asyncio.TimeoutError, ConnectorError, Exception):
                conn._task.cancel()
        conn._session = None

    async def invalidate(self, connector_id: str) -> None:
        """Drop any live session. Call after a connector is edited or toggled."""
        async with self._lock:
            conn = self._conns.pop(connector_id, None)
        if conn:
            await self._disconnect(conn)

    async def aclose(self) -> None:
        for cid in list(self._conns):
            await self.invalidate(cid)

    # ---- the surface the MCP tools call ---------------------------------

    def _hint_for(self, conn: Connection, tool: str) -> bool | None:
        for t in conn.tools:
            if t.get("name") == tool:
                return (t.get("annotations") or {}).get("readOnlyHint")
        return None

    async def list_tools(self, connector_id: str) -> dict:
        """Downstream tools with Nakagai's policy verdict attached to each."""
        spec = self.spec(connector_id)
        spec.check_connectable()
        if not spec.enabled:
            raise GuardrailDenied(f"connector {connector_id!r} is disabled")
        conn = await self._ensure_connected(spec)
        conn.last_used = time.monotonic()
        return {"connector": connector_id, "tools": annotate_tools(spec, conn.tools)}

    def provenance(self, spec: ConnectorSpec, args: dict,
                   signal_id: str) -> tuple[dict | None, float]:
        """What this order claims to be: the signal Nakagai emitted under the cited
        id, and the order's notional.

        The base hub is what the edge runs, and the edge cannot answer this: it has
        no signals/ directory, because signals are the platform's. (None, 0.0) is the
        honest answer, not a stub. The platform re-derives provenance when the edge's
        record reaches it (api/agent_routes.py:436), so nothing is lost. PlatformHub
        overrides this for the platform's own enqueue path.
        """
        return None, 0.0

    async def _maybe_auto_approve(self, record) -> tuple[Any | None, bool]:
        """The mandate's verdict on this record, before a human is asked.

        The base hub has no mandate, no equity report and no signing key, so it never
        auto-approves. PlatformHub overrides this.
        """
        return None, False

    async def call(self, connector_id: str, tool: str, args: dict, *,
                   requested_by: str = "", workspace: str = "default",
                   approved: bool = False, signal_id: str = "") -> dict:
        """Proxy one downstream tool call, after the guardrails clear it.

        `approved=True` is the post-human-decision path: the guardrails run again
        against *current* config (a connector disabled since the request was made
        still refuses), but a verdict of `approve` no longer enqueues; it means
        "the human already said yes". Only the ApprovalQueue may set this.

        `signal_id` is the id of a signal Nakagai emitted that this order claims to
        execute. It is resolved and frozen onto the approval record for the audit
        trail at every rung. Under autopilot it is also what the envelope checks: an
        order citing nothing never auto-executes.
        """
        spec = self.spec(connector_id)
        spec.check_connectable()
        if not spec.enabled:
            raise GuardrailDenied(f"connector {connector_id!r} is disabled")

        # Connect first: the downstream's own readOnlyHint is the strongest
        # signal for write-classification, and it only exists once we've listed.
        conn = await self._ensure_connected(spec)
        if not any(t.get("name") == tool for t in conn.tools):
            known = ", ".join(sorted(t.get("name", "") for t in conn.tools)[:20])
            raise ConnectorError(f"connector {connector_id!r} has no tool {tool!r} "
                                 f"(has: {known})")

        verdict = evaluate(spec, tool, args, read_only_hint=self._hint_for(conn, tool))
        if verdict.decision == "deny":
            raise GuardrailDenied(verdict.reason)
        if verdict.decision == "approve" and not approved:
            queue = self.approvals
            if queue is None:
                raise GuardrailDenied(
                    f"{verdict.reason}; no approval queue is configured. "
                    f"Remove this tool from approvals.require_for to allow it")
            signal, notional = self.provenance(spec, args, signal_id)
            record = queue.enqueue(connector_id, tool, args,
                                   ttl_s=spec.guardrails.approvals.ttl_s,
                                   requested_by=requested_by, workspace=workspace,
                                   signal_id=signal_id, signal=signal,
                                   notional=notional)

            # The mandate gets to decide before a human is asked. Inline, not a
            # background sweeper: a sweeper would put a polling interval between
            # "the agent saw the setup" and "the order reached the broker", which is
            # the latency the autopilot rung exists to remove. It decides through
            # self.decide(), the same door the owner's tap goes through, so a
            # decline is never a denial: the record simply stays pending.
            decided, declined = await self._maybe_auto_approve(record)
            if decided is not None:
                return {"connector": connector_id, "tool": tool,
                        "is_write": verdict.is_write, "auto_approved": True,
                        "approval_id": decided.id, "status": decided.status,
                        "signal_id": decided.signal_id,
                        "decided_by": decided.decided_by,
                        "result": decided.result, "error": decided.error,
                        "message": "the mandate approved this order inside your "
                                   "autopilot envelope; nothing to poll"}

            return {"connector": connector_id, "tool": tool, "is_write": verdict.is_write,
                    "approval_required": True, "approval_id": record.id,
                    "status": record.status, "expires_at": record.expires_at,
                    "auto_declined": declined,
                    "message": f"{verdict.reason}. Poll get_approval({record.id!r}) "
                               f"for the outcome."}

        conn.last_used = time.monotonic()
        result = await conn._session.call_tool(
            tool, args, read_timeout_seconds=timedelta(seconds=spec.timeout_s))
        return {"connector": connector_id, "tool": tool, "is_write": verdict.is_write,
                **serialize_result(result)}

    async def probe(self, connector_id: str) -> dict:
        """Connect (or reuse) and report what the downstream says it can do."""
        spec = self.spec(connector_id)
        spec.check_connectable()
        started = time.monotonic()
        conn = await self._ensure_connected(spec)
        return {"ok": True, "connector": connector_id,
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
                "tool_count": len(conn.tools),
                "tools": [t.get("name") for t in conn.tools],
                "server_info": conn.server_info}

    def status(self) -> dict:
        """Runtime view of every registered connector (no connection attempted)."""
        specs = self.load_specs()
        out = []
        for cid, spec in specs.items():
            conn = self._conns.get(cid)
            if conn:
                conn.spec = spec
                out.append(conn.to_dict())
            else:
                out.append(Connection(spec=spec).to_dict())
        return {"connectors": out}


