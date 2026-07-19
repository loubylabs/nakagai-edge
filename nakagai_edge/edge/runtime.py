"""The edge shim itself: a FastMCP server on 127.0.0.1 that IS the agent's
whole world. Brokers are dialed with locally-held credentials through the
unmodified gateway runtime; the platform is reached as data (via a synced
nakagai-mcp connector) and as authority (approvals, audit, bundle).

Fail-closed: policy staler than sync.POLICY_TTL_S refuses every connector
call. Writes additionally require a live platform grant, so they are doubly
impossible offline."""

import asyncio
import json
import logging
import os
import time

import httpx

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.executor import poll_once
from nakagai_edge.edge.portfolio import PORTFOLIO_INTERVAL_S, PortfolioReporter
from nakagai_edge.edge.remote import RemoteApprovalQueue
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import POLICY_TTL_S, SYNC_INTERVAL_S, policy_fresh, sync_once

EXECUTOR_INTERVAL_S = 5
AUDIT_SHIP_INTERVAL_S = 30


def freshness_error() -> str:
    return json.dumps({"is_error": True, "error":
        "policy stale: the edge cannot reach the platform and its cached "
        "policy is past TTL; every connector call is refused until a sync "
        "succeeds"})


def build_hub(state: EdgeState, client: PlatformClient):
    from nakagai_edge.hub import ConnectorHub

    agent = state.agent()
    if agent is None:
        raise SystemExit("edge is not paired: run `nakagai-edge pair <code> "
                         "--platform <url>` first")
    # The synced registry's nakagai-mcp entry names this env var; exporting it
    # here keeps auth.py's env-indirection contract without a new auth mode.
    os.environ["NAKAGAI_AGENT_TOKEN"] = agent["token"]
    queue = RemoteApprovalQueue(client, state, agent["agent_id"])
    return ConnectorHub(state.root, approvals=queue)


def create_edge_mcp(state: EdgeState, hub, client: PlatformClient, audit: EdgeAudit, reporter):
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("nakagai-edge")

    def _gate() -> str | None:
        return None if policy_fresh(state, POLICY_TTL_S) else freshness_error()

    async def _guarded(connector_id: str, tool: str, args: dict) -> str:
        if (stale := _gate()) is not None:
            audit.record("denial", connector_id, tool, {"reason": "policy stale"})
            return stale
        from nakagai_edge.hub import ConnectorError, GuardrailDenied
        try:
            out = await hub.call(connector_id, tool, args)
            kind = "call" if not out.get("approval_required") else "intent"
            audit.record(kind, connector_id, tool, {"is_write": out.get("is_write")})
            return json.dumps(out, default=str)
        except GuardrailDenied as e:
            audit.record("denial", connector_id, tool, {"reason": str(e)})
            return json.dumps({"is_error": True, "error": str(e)})
        except (ConnectorError, ValueError, EdgeClientError, httpx.HTTPError) as e:
            audit.record("error", connector_id, tool, {"error": str(e)})
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def call_connector(connector_id: str, tool: str, args_json: str = "{}") -> str:
        """Call one tool on a configured connector (broker or platform). Writes
        enqueue for human approval on the platform; poll get_approval for the
        outcome."""
        try:
            args = json.loads(args_json or "{}")
        except json.JSONDecodeError as e:
            return json.dumps({"is_error": True, "error": f"bad args_json: {e}"})
        return await _guarded(connector_id, tool, args)

    @mcp.tool()
    async def list_connector_tools(connector_id: str) -> str:
        """Downstream tools with the local policy verdict attached to each."""
        if (stale := _gate()) is not None:
            return stale
        from nakagai_edge.hub import ConnectorError, GuardrailDenied
        try:
            return json.dumps(await hub.list_tools(connector_id), default=str)
        except (ConnectorError, GuardrailDenied, ValueError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def list_connectors() -> str:
        """Every configured connector and whether it is enabled."""
        if (stale := _gate()) is not None:
            return stale
        return json.dumps(hub.status(), default=str)

    @mcp.tool()
    async def get_connector_status() -> str:
        """Runtime state of every connector. Works even on stale policy, because
        an agent needs to see WHY everything else is refusing."""
        status = hub.status()
        status["policy_fresh"] = policy_fresh(state, POLICY_TTL_S)
        return json.dumps(status, default=str)

    @mcp.tool()
    async def get_approval(approval_id: str) -> str:
        """Status of a write intent you enqueued via call_connector."""
        if (stale := _gate()) is not None:
            return stale
        rec = hub.approvals.get(approval_id)
        if rec is None:
            return json.dumps({"is_error": True, "error": f"no approval {approval_id!r}"})
        return json.dumps(rec.public(), default=str)

    @mcp.tool()
    async def agent_checkin(status: str, note: str = "",
                            account_equity: float | None = None,
                            day_pnl: float | None = None) -> str:
        """Record a heartbeat for the owner's activity feed and get the current
        mandate back. Call once per session: `status` is one of
        scanning|research|backtesting|idle|alert, `note` a one-line summary of
        what you're doing or found. You are identified by your agent token -
        there is no name to pass.

        When `mandate.directives.report_equity` is true, ALSO relay your
        broker's own numbers: `account_equity` (the account's total value) and
        `day_pnl` (today's profit and loss, SIGNED: negative is a loss,
        measured against the prior session's close). Report BOTH or neither;
        one without the other is discarded.

        autopilot's daily-loss circuit breaker runs on these and nothing else.
        Nakagai never pulls them from your broker. While the dial is on and no
        recent report exists, autopilot will not auto-execute: it declines to a
        human tap rather than trade blind to the account's drawdown.

        Not gated on local policy freshness: this call goes straight to the
        platform rather than reading anything cached, so a stale local policy
        does not stop it - if the platform itself is unreachable, that comes
        back as an ordinary error below.

        When the response carries `pending_messages`, those are owner chat messages
        waiting since your last check-in. Reply to each with `send_message`.
        """
        try:
            out = client.agent_checkin(status, note, account_equity, day_pnl)
            return json.dumps(out, default=str)
        except (EdgeClientError, httpx.HTTPError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def await_events(timeout_s: float = 50, cursor: int = 0) -> str:
        """Hold the platform's live channel open and return the next batch of
        events after `cursor` (owner messages, approval outcomes, mandate
        changes, signals). Loop this while your mandate says live_link; an
        empty batch after a timeout is the normal idle rhythm. Goes straight
        to the platform, so stale local policy does not stop it."""
        try:
            out = client.await_events(after=cursor, timeout_s=timeout_s)
            return json.dumps(out, default=str)
        except (EdgeClientError, httpx.HTTPError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def send_message(text: str) -> str:
        """Send a message to the owner's chat pane on the platform. Plain
        text, capped at 4000 characters. Never gated: even halted, you may
        say you are halted."""
        try:
            out = client.send_message(text)
            return json.dumps(out, default=str)
        except (EdgeClientError, httpx.HTTPError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def refresh_portfolio() -> str:
        """Fetch fresh portfolio figures (totals + positions) from every
        broker with this edge's own credentials, push them to the owner's
        Portfolio page, and return the same document. You never supply
        numbers; this tool makes the edge go look for itself. Rate-limited:
        within 15s of the last sweep you get that snapshot back unchanged."""
        if (stale := _gate()) is not None:
            return stale
        try:
            return json.dumps(await reporter.snapshot_and_push(), default=str)
        except Exception as e:  # noqa: BLE001 (tool surface: report, don't crash)
            return json.dumps({"is_error": True, "error": str(e)})

    return mcp


async def _loops(state: EdgeState, hub, client: PlatformClient, audit: EdgeAudit, reporter):
    from nakagai_edge.edge.client import EdgeClientError

    async def syncer():
        while True:
            await asyncio.to_thread(sync_once, state, client)
            try:
                # The platform cannot dial our connectors, so we tell it what we
                # see. Best-effort: a platform that is down must not stop the
                # edge from serving its agent. Caught broadly, like executor()
                # below: EdgeClientError and httpx.HTTPError cover a rejected
                # token or an unreachable platform, but this loop runs forever
                # and a single uncaught exception (a non-JSON 200 body raising
                # ValueError inside _check, say) would kill it for good, taking
                # every later sync down with it, not just this one report.
                await asyncio.to_thread(
                    client.report_connectors, hub.status()["connectors"])
            except Exception as e:
                logging.getLogger("nakagai.edge").warning(
                    "connector status not reported to the platform this "
                    "cycle: %s", e)
            await asyncio.sleep(SYNC_INTERVAL_S)

    async def executor():
        while True:
            try:
                if await poll_once(hub, state, client, audit):
                    # Something just reached a terminal state, so a write may
                    # have executed: refresh the owner's figures now instead
                    # of waiting out the timer. Denials over-trigger this
                    # harmlessly (the sweep is read-only and the reporter is
                    # rate-limited); missing a real execution would not be
                    # harmless in the other direction.
                    await reporter.snapshot_and_push()
            except Exception:
                pass  # next pass retries; the journal has the details
            await asyncio.sleep(EXECUTOR_INTERVAL_S)

    async def shipper():
        while True:
            batch = audit.pending()
            if batch:
                try:
                    await asyncio.to_thread(client.ship_audit, batch)
                    audit.mark_shipped(len(batch))
                except EdgeClientError:
                    pass  # ship on reconnect
            await asyncio.sleep(AUDIT_SHIP_INTERVAL_S)

    async def portfolio_loop():
        while True:
            try:
                # Same broad-catch posture as syncer() above, same reason:
                # this loop runs forever and no single bad cycle may kill it.
                await reporter.snapshot_and_push()
            except Exception as e:  # noqa: BLE001
                logging.getLogger("nakagai.edge").warning(
                    "portfolio snapshot failed this cycle: %s", e)
            await asyncio.sleep(PORTFOLIO_INTERVAL_S)

    return [asyncio.create_task(syncer(), name="syncer"),
            asyncio.create_task(executor(), name="executor"),
            asyncio.create_task(shipper(), name="shipper"),
            asyncio.create_task(portfolio_loop(), name="portfolio_loop")]


def run(root, port: int = 8330) -> None:
    state = EdgeState(root)
    agent = state.agent()
    if agent is None:
        raise SystemExit("edge is not paired: run `nakagai-edge pair <code> "
                         "--platform <url>` first")
    client = PlatformClient(agent["platform_url"], agent["token"])
    sync_once(state, client)                 # best-effort warm start
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    reporter = PortfolioReporter(state, hub, client)
    mcp = create_edge_mcp(state, hub, client, audit, reporter)
    mcp.settings.host, mcp.settings.port = "127.0.0.1", port

    async def main():
        tasks = await _loops(state, hub, client, audit, reporter)
        try:
            await mcp.run_streamable_http_async()
        finally:
            for t in tasks:
                t.cancel()
            await hub.aclose()

    asyncio.run(main())
