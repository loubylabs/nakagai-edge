"""A stand-in for the platform's own MCP server, as the edge dials it.

It advertises the same twenty-six tools `nakagai_platform/mcp_server.py`
serves, with their real signatures, because promotion is DERIVED from those
schemas: a fixture that simplified them would prove the promotion works on a
shape the platform never sends. Nine of the names collide with the edge's own
tools on purpose (agent_checkin, call_connector, get_approval,
get_connector_status, list_connector_tools, list_peers, claim_message,
send_message, request_peer, accept_candidate, abstain_candidate); that
collision is the point of half the tests.

Every tool answers with the arguments it received and records them in `calls`,
so a test can assert what actually arrived rather than that something did. A
test that cares about `calls` clears it first: this module is imported once.
"""

import json

from mcp.server.mcpserver import MCPServer

mcp = MCPServer("nakagai-platform-fake")

calls: list[tuple[str, dict]] = []


def _echo(tool: str, args: dict) -> str:
    calls.append((tool, dict(args)))
    return json.dumps({"tool": tool, "args": args})


@mcp.tool()
def get_mandate() -> str:
    """What you are permitted to do right now."""
    return _echo("get_mandate", {})


@mcp.tool()
def agent_checkin(status: str, note: str = "",
                  account_equity: float | None = None,
                  day_pnl: float | None = None) -> str:
    """Heartbeat. Collides with the edge's own agent_checkin."""
    return _echo("agent_checkin", locals())


@mcp.tool()
async def await_events(timeout_s: float = 50, cursor: int = 0) -> str:
    """Long-poll for owner messages and signals."""
    return _echo("await_events", locals())


@mcp.tool()
def accept_candidate(candidate_id: str, rationale: str) -> str:
    """Platform candidate route that must lose to the bounded local tool."""
    return _echo("accept_candidate", locals())


@mcp.tool()
def abstain_candidate(candidate_id: str, rationale: str) -> str:
    """Platform candidate route that must lose to the bounded local tool."""
    return _echo("abstain_candidate", locals())


@mcp.tool()
def list_peers() -> str:
    """The peers on this owner's desk."""
    return _echo("list_peers", {})


@mcp.tool()
def claim_message(message_seq: int) -> str:
    """Claim one actionable message."""
    return _echo("claim_message", locals())


@mcp.tool()
def send_message(text: str, room_id: str, idempotency_key: str,
                 reply_to_seq: int = 0) -> str:
    """Say something to the owner. Collides with the edge's own send_message."""
    return _echo("send_message", locals())


@mcp.tool()
def request_peer(agent_ids: list[str], text: str, idempotency_key: str,
                 source_seq: int = 0) -> str:
    """Ask selected peers for owner-visible help."""
    return _echo("request_peer", locals())


@mcp.tool()
def scan_symbol(symbol: str) -> str:
    """Scan one symbol on demand."""
    return _echo("scan_symbol", locals())


@mcp.tool()
def get_signals(include_suppressed: bool = False, since: str = "") -> str:
    """Today's signal clusters."""
    return _echo("get_signals", locals())


@mcp.tool()
def get_scan_health() -> str:
    """Whether the scan loop is keeping up."""
    return _echo("get_scan_health", {})


@mcp.tool()
def get_autoexecute_allowlist() -> str:
    """Which plays may auto-execute."""
    return _echo("get_autoexecute_allowlist", {})


@mcp.tool()
def get_account_watchlist() -> str:
    """The symbols this account is scanning."""
    return _echo("get_account_watchlist", {})


@mcp.tool()
def set_autoexecute_allowlist(reason: str, add: list[str] | None = None,
                              drop: list[str] | None = None) -> str:
    """Change which plays may auto-execute. A write."""
    return _echo("set_autoexecute_allowlist", locals())


@mcp.tool()
def get_roster() -> str:
    """Every play on the roster."""
    return _echo("get_roster", {})


@mcp.tool()
def list_plays(category: str = "") -> str:
    """The plays, optionally one category."""
    return _echo("list_plays", locals())


@mcp.tool()
def get_play(name: str) -> str:
    """One play's definition."""
    return _echo("get_play", locals())


@mcp.tool()
def validate_rule_spec(spec_json: str) -> str:
    """Check a RuleSpec without running it."""
    return _echo("validate_rule_spec", locals())


@mcp.tool()
def run_screen(spec_json: str, symbols: str = "") -> str:
    """Run a screen. A write."""
    return _echo("run_screen", locals())


@mcp.tool()
def run_backtest(start: str, end: str, strategies: str = "sma_cross",
                 symbols: str = "", risk_pct: float = 0.01,
                 params_json: str = "", config: str = "") -> str:
    """Walk-forward backtest. A write."""
    return _echo("run_backtest", locals())


@mcp.tool()
def save_strategy_config(name: str, strategy: str, params_json: str,
                         notes: str = "") -> str:
    """Store a strategy configuration. A write."""
    return _echo("save_strategy_config", locals())


@mcp.tool()
def get_runs(strategy: str = "", symbol: str = "", limit: int = 20,
             store: str = "sandbox") -> str:
    """The proving record."""
    return _echo("get_runs", locals())


@mcp.tool()
def sync_data(symbols: str = "", start: str = "2023-07-01") -> str:
    """Pull bars into the cache. A write."""
    return _echo("sync_data", locals())


@mcp.tool()
def get_connector_status() -> str:
    """Collides with the edge's own get_connector_status."""
    return _echo("get_connector_status", {})


@mcp.tool()
async def list_connector_tools(connector_id: str) -> str:
    """Collides with the edge's own list_connector_tools."""
    return _echo("list_connector_tools", locals())


@mcp.tool()
async def call_connector(connector_id: str, tool: str, args_json: str = "{}",
                         signal_id: str = "") -> str:
    """Collides with the edge's own call_connector."""
    return _echo("call_connector", locals())


@mcp.tool()
def get_approval(approval_id: str) -> str:
    """Collides with the edge's own get_approval."""
    return _echo("get_approval", locals())


if __name__ == "__main__":
    mcp.run()
