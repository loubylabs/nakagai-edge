"""Nakagai's outbound MCP client runtime.

One `Connection` per enabled connector, each owned end-to-end by a single
asyncio task: that task enters the transport context, initializes the session,
snapshots the tool list, then parks on a stop event. Teardown happens *inside*
the same task. This matters: anyio cancel scopes may not be exited from a
different task than the one that entered them, which is why an
AsyncExitStack-across-tasks pool (and `ClientSessionGroup`, which also can't
carry an httpx OAuth `auth`) is the wrong shape here.

Calls from other tasks are safe: `ClientSession.call_tool` only writes to memory
streams owned by the session. `TolerantClientSession._tolerated` (below) is
mutated on that same call path from those same other tasks, and is safe for a
different reason: the check-then-add against it has no `await` between the
membership test and the `add`, so the whole thing is one atomic step under
asyncio's cooperative scheduling. Put an `await` inside that block and this
stops being true.
"""

import asyncio
import json
import os
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from mcp.client.session import ClientSession

from nakagai_edge.config import ConnectorSpec, load_specs, resolve_env_refs
from nakagai_edge.guardrails import annotate_tools, evaluate
from nakagai_edge.response_schema import undeclared_properties

RECONNECT_BACKOFF_S = 30.0
# Consecutive OAuth connect failures (no success between) before a connector
# with tokens on disk reports `needs_login`. No tokens at all is immediate.
OAUTH_FAILURES_FOR_LOGIN = 2
log = logging.getLogger("nakagai.edge")


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


def needs_login(e: BaseException) -> bool:
    """Whether a connect failure is one a browser login fixes.

    Two real types, never a message: the edge's own `NoTokensError` (no login
    has ever happened here) and the MCP SDK's `OAuthFlowError` family, which
    is what a dead refresh token ends in once the SDK falls back to a full
    authorization it has no browser for. Every leaf of an anyio group is
    checked, since that is how the transports deliver either one. A network
    error or a timeout is deliberately not here: no login fixes those.
    """
    from mcp.client.auth.exceptions import OAuthFlowError

    from nakagai_edge.auth import NoTokensError
    pending = [e]
    while pending:
        leaf = pending.pop()
        if isinstance(leaf, BaseExceptionGroup):
            pending.extend(leaf.exceptions)
        elif isinstance(leaf, NoTokensError | OAuthFlowError):
            return True
    return False


class TolerantClientSession(ClientSession):
    """A session that refuses a broker's bad data, not its bad bookkeeping.

    The SDK validates every result against the server's own declared output
    schema and raises, with no opt-out (`ClientSession.__init__` takes no
    flag). Robinhood declares its account objects closed and then returns
    `unsettled_funds`, which took the whole portfolio sweep down over a field
    no capability map mentions.

    `super().validate_tool_result` wraps three different failures into the
    same `RuntimeError`: missing structured content, a `ValidationError` from
    the compiled jsonschema validator, and a `SchemaError` from a schema that
    will not even compile. All three land in the `except RuntimeError` below
    and are decided
    by `undeclared_properties`, which re-runs `check_schema` and `iter_errors`
    on the same schema and instance. For the first two, that rerun turns up
    errors that are not `additionalProperties: false` refusals, so the
    classifier returns None and the SDK's own RuntimeError is re-raised
    untouched, message and all. For the third, re-running `check_schema` hits
    `jsonschema.SchemaError` again, which is why `response_schema.py` catches
    it too: without that branch, this override would raise a fresh SchemaError
    out of an active except block instead of relaying the SDK's message.
    """

    # The dedupe key is (tool, property set), and the property set is the
    # broker's to choose: nothing here bounds how many distinct sets a session
    # sees, so a broker that varies an undeclared property per call (a
    # per-request id, a date-stamped balance key) would otherwise grow this
    # set once per call, forever, in a daemon that holds one session for the
    # whole life of the connection. The cap bounds memory instead of the
    # broker; past it, a call with a never-before-seen property set logs again
    # rather than being remembered, which trades log noise for a fixed
    # ceiling. That is the right trade here: a broker noisy enough to hit the
    # cap is already the case this warning exists to surface.
    _MAX_TOLERATED = 1024

    def __init__(self, *args, connector_id: str = "", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._connector_id = connector_id
        self._tolerated: set[tuple[str, tuple[str, ...]]] = set()

    async def validate_tool_result(self, name: str, result) -> None:
        try:
            await super().validate_tool_result(name, result)
        except RuntimeError:
            extras = undeclared_properties(
                self._tool_output_schemas.get(name),
                getattr(result, "structured_content", None))
            if extras is None:
                raise
            seen = (name, tuple(extras))
            if seen not in self._tolerated:
                # Once per session per tool per property set, up to
                # _MAX_TOLERATED. The sweep runs on a 300s timer, so logging
                # every call would bury the connector's real errors under a
                # field nobody reads.
                if len(self._tolerated) < self._MAX_TOLERATED:
                    self._tolerated.add(seen)
                log.warning(
                    "connector %r tool %r returned %s, which its own output "
                    "schema does not declare. Relaying the payload: an "
                    "undeclared property says nothing about the fields Nakagai "
                    "reads. A declared field of the wrong type still refuses.",
                    self._connector_id or "?", name, ", ".join(extras))


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
    # Set by the hub (ConnectorHub.status), which counts OAuth failures per
    # connector id: a Connection is replaced on every reconnect, so a count
    # kept here would reset before it could ever reach two.
    login_failed: bool = False
    _session: Any = None
    _task: asyncio.Task | None = None
    _ready: asyncio.Future | None = None
    _stop: asyncio.Event | None = None

    def auth_state(self, root: Path) -> str:
        """"ok" or "needs_login", the connector report's `auth_state`.

        Only an OAuth connector can need a login. It does when this edge holds
        no tokens for it, or when its last connect failed on OAuth: the SDK
        drops a dead token only in memory, so the file alone still reads as
        logged in after the broker has ended the session.
        """
        from nakagai_edge.auth import has_oauth_tokens
        if self.spec.auth.mode != "oauth":
            return "ok"
        if self.login_failed or not has_oauth_tokens(root, self.spec.id):
            return "needs_login"
        return "ok"

    def to_dict(self, *, root: Path, with_tools: bool = False) -> dict:
        """This connector's runtime state, as data.

        `root` is the edge root, where the OAuth tokens `auth_state` reads
        live. `with_tools` adds the downstream's own tool schemas. Off by default
        because the two big readers want opposite things: the platform's
        connector report needs the schemas (it can never dial a broker to read
        them itself), while `list_connectors` is answered to an agent that pays
        tokens for every byte and already has `list_connector_tools` for the
        one connector it cares about.
        """
        out = {
            "id": self.spec.id, "name": self.spec.name or self.spec.id,
            "kind": self.spec.kind, "role": self.spec.role,
            "enabled": self.spec.enabled, "status": self.status,
            "tool_count": len(self.tools), "last_error": self.last_error,
            "server_info": self.server_info,
            "allow_writes": self.spec.guardrails.allow_writes,
            "auth_mode": self.spec.auth.mode,
            "auth_state": self.auth_state(root),
            # What this connector can be asked to DO, in the shared vocabulary.
            # An agent reads this to know which broker serves `place_order`
            # before it calls one, rather than discovering the gap as a refusal.
            "capabilities": self.spec.capability_names,
        }
        if with_tools:
            # Three keys per tool, never the whole dict: name and inputSchema
            # are what a capability map is derived from, description is what
            # the human approving that map reads. outputSchema, annotations and
            # _meta would be bytes on the wire nothing upstream looks at. Every
            # key is always present, so the reader takes three fields off every
            # entry rather than three off some of them.
            out["tools"] = [{"name": t.get("name") or "",
                             "description": t.get("description") or "",
                             "inputSchema": t.get("inputSchema") or {}}
                            for t in self.tools]
        return out


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
    out: dict = {"is_error": bool(getattr(result, "is_error", False))}
    joined = "\n".join(texts)
    if texts:
        # Downstream servers usually hand back a JSON document as text; parse it
        # so the agent gets data rather than a string holding data.
        try:
            out["data"] = json.loads(joined)
        except (json.JSONDecodeError, ValueError):
            out["text"] = joined
    structured = getattr(result, "structured_content", None)
    if structured is not None:
        # An MCPServer downstream mirrors most results into structured content:
        # {"result": text} for plain tools, or a copy of the JSON already
        # parsed into `data`. The
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
        # OAuth connect failures per connector id since its last success. One
        # is not yet a lost login: a token endpoint that answers 5xx once makes
        # the SDK drop its tokens in memory and fail the full flow, and the
        # next connect reloads the refresh token from disk and recovers.
        self._oauth_failures: dict[str, int] = {}
        # When the last of those failures happened. A token file written
        # after it (`nakagai-edge login`) clears the count at once.
        self._oauth_failed_at: dict[str, float] = {}
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
            from nakagai_edge.identity import client_info

            # Plain seconds: the SDK takes a float here, and passes it to
            # anyio.fail_after the same way for every request.
            timeout = float(spec.timeout_s)
            if spec.transport == "stdio":
                from mcp.client.stdio import StdioServerParameters, stdio_client
                params = StdioServerParameters(command=spec.command, args=spec.args,
                                               env=env or None)
                async with stdio_client(params) as (read, write):
                    async with TolerantClientSession(read, write,
                                                     read_timeout_seconds=timeout,
                                                     client_info=client_info(),
                                                     connector_id=spec.id) as s:
                        await s.initialize()
                        yield s
            else:
                from mcp.client.streamable_http import streamable_http_client

                from nakagai_edge.auth import build_http_client
                http_client = build_http_client(spec, self.root)
                async with streamable_http_client(
                        spec.url, http_client=http_client) as (read, write):
                    async with TolerantClientSession(read, write,
                                                     read_timeout_seconds=timeout,
                                                     client_info=client_info(),
                                                     connector_id=spec.id) as s:
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
                # `by_alias`, so these dicts stay in MCP's own wire spelling
                # (`inputSchema`, `annotations.readOnlyHint`). The SDK's model
                # fields are snake_case, and a plain dump would hand
                # `input_schema` to guardrails.py, to the platform's connector
                # report, and to the capability map the owner approves. Every
                # reader of `conn.tools` expects the wire names.
                conn.tools = [t.model_dump(mode="json", by_alias=True)
                              for t in listed.tools]
                init = session.initialize_result
                if init is not None:
                    conn.server_info = init.server_info.model_dump(
                        mode="json", by_alias=True)
                conn.status, conn.last_error = "connected", ""
                self._clear_login_failed(spec.id)
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
            if needs_login(e):
                # Only a success resets this; a network failure says nothing
                # about the login either way.
                self._oauth_failures[spec.id] = (
                    self._oauth_failures.get(spec.id, 0) + 1)
                self._oauth_failed_at[spec.id] = time.time()
                if (self._oauth_failures[spec.id] >= OAUTH_FAILURES_FOR_LOGIN
                        and self._read_login_failed_mark(spec.id) is None):
                    self._write_login_failed_mark(spec.id)
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

    async def call(self, connector_id: str, tool: str, args: dict, *,
                   account_key: str, requested_by: str = "",
                   approved: bool = False, signal_id: str = "") -> dict:
        """Proxy one downstream tool call, after the guardrails clear it.

        `approved=True` is the post-human-decision path: the guardrails run again
        against *current* config (a connector disabled since the request was made
        still refuses), but a verdict of `approve` no longer enqueues; it means
        "the human already said yes". Only the ApprovalQueue may set this.

        `signal_id` is the id of a signal Nakagai emitted that this order claims to
        execute. It is resolved and frozen onto the approval record for the audit
        trail.
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
            record = queue.enqueue(account_key, connector_id, tool, args,
                                   ttl_s=spec.guardrails.approvals.ttl_s,
                                   requested_by=requested_by,
                                   signal_id=signal_id)
            return {"connector": connector_id, "tool": tool, "is_write": verdict.is_write,
                    "approval_required": True, "approval_id": record.id,
                    "status": record.status, "expires_at": record.expires_at,
                    "message": f"{verdict.reason}. Poll get_approval({record.id!r}) "
                               f"for the outcome."}

        conn.last_used = time.monotonic()
        result = await conn._session.call_tool(
            tool, args, read_timeout_seconds=float(spec.timeout_s))
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

    def _login_failed_mark(self, connector_id: str) -> Path:
        """Where a tripped two-failure rule is remembered across restarts.

        The count is in memory, so without this a restarted daemon's first
        report says `ok` and the next trip reads to the platform as a new
        crossing, which emails the owner again.
        """
        from nakagai_edge.auth import token_path
        return token_path(self.root, connector_id).with_name(
            f"{connector_id}.login-failed.json")

    def _write_login_failed_mark(self, connector_id: str) -> None:
        path = self._login_failed_mark(connector_id)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"at": time.time()}))
            os.chmod(tmp, 0o600)
            os.replace(tmp, path)
        except OSError:
            log.warning("could not record the failed login for %r", connector_id)

    def _read_login_failed_mark(self, connector_id: str) -> float | None:
        try:
            return float(json.loads(
                self._login_failed_mark(connector_id).read_text())["at"])
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def _clear_login_failed(self, connector_id: str) -> None:
        self._oauth_failures.pop(connector_id, None)
        self._oauth_failed_at.pop(connector_id, None)
        self._login_failed_mark(connector_id).unlink(missing_ok=True)

    def _login_failed(self, connector_id: str) -> bool:
        """The two-failure rule, in memory or remembered on disk. A token
        file newer than the last failure (or the mark) is a login those
        failures predate, so it clears them."""
        from nakagai_edge.auth import token_path
        marked_at = self._read_login_failed_mark(connector_id)
        failed_at = max(self._oauth_failed_at.get(connector_id, 0.0),
                        marked_at or 0.0)
        if failed_at:
            try:
                written = token_path(self.root, connector_id).stat().st_mtime
            except OSError:
                written = 0.0
            if written > failed_at:
                self._clear_login_failed(connector_id)
                return False
        return (marked_at is not None
                or self._oauth_failures.get(connector_id, 0)
                >= OAUTH_FAILURES_FOR_LOGIN)

    def status(self, *, with_tools: bool = False) -> dict:
        """Runtime view of every registered connector (no connection attempted).

        `with_tools` carries each connector's tool schemas through: see
        Connection.to_dict for why the platform's report asks for them and the
        agent-facing status tools do not.
        """
        specs = self.load_specs()
        out = []
        for cid, spec in specs.items():
            login_failed = self._login_failed(cid)
            conn = self._conns.get(cid)
            if conn:
                conn.spec = spec
                conn.login_failed = login_failed
                out.append(conn.to_dict(root=self.root, with_tools=with_tools))
            else:
                out.append(Connection(spec=spec, login_failed=login_failed)
                           .to_dict(root=self.root, with_tools=with_tools))
        return {"connectors": out}
