"""A tiny downstream MCP server for gateway tests.

Stands in for a broker: one annotated read tool, one unannotated read-ish tool,
one account-scoped read, one write. Runnable as a stdio server so the real
`stdio_client` transport gets exercised too:

    uv run --group mcp python tests/fixtures/echo_mcp.py
"""

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

mcp = FastMCP("echo")


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
def echo(text: str) -> str:
    """Echo text back. Explicitly read-only."""
    return text


@mcp.tool()
def search(query: str) -> str:
    """Search for something. No readOnlyHint, so classification falls to config."""
    return f"results for {query}"


@mcp.tool()
def get_portfolio(account_number: str) -> str:
    """Account-scoped read."""
    return f'{{"account": "{account_number}", "cash": "1000"}}'


@mcp.tool()
def place_equity_order(symbol: str, account_number: str, quantity: int = 1) -> str:
    """A write. Must never execute in tests without allow_writes."""
    return f"PLACED {quantity} {symbol} on {account_number}"


@mcp.tool()
def boom() -> str:
    """Always fails. Exercises downstream-error passthrough."""
    raise RuntimeError("downstream exploded")


if __name__ == "__main__":
    mcp.run()
