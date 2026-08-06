"""A broker that disagrees with Robinhood on every axis at once.

Deliberately alien. If any capability test passes against Robinhood's shape
and fails here, something upstream is still hardcoded, which is the exact
failure this fixture exists to catch.
"""

from mcp.server.mcpserver import MCPServer

mcp = MCPServer("alien-broker")

ACCOUNTS = [{"acct": "AL-1", "label": "Alien Main", "kind": "margin"}]
HOLDINGS = {"AL-1": [{"ticker": "aapl", "qty": "25", "cost": "187.20"},
                     {"ticker": "msft", "qty": "3", "cost": "410.00"}]}
PRICES = {"AAPL": "190.00", "MSFT": "415.00"}


@mcp.tool()
def accounts_list() -> dict:
    """List accounts."""
    return {"accounts": ACCOUNTS}


@mcp.tool()
def balances(acct: str) -> dict:
    """Account balances."""
    return {"net_liq": "104238.55", "settled_cash": "50.00",
            "power": "12038.10", "denom": "USD"}


@mcp.tool()
def holdings(acct: str) -> dict:
    """Open holdings."""
    return {"holdings": HOLDINGS.get(acct, [])}


@mcp.tool()
def ticker(tickers: list[str]) -> dict:
    """Last prices."""
    return {"ticks": [{"tkr": s, "last": PRICES.get(s.upper(), "1.00")}
                      for s in tickers]}


@mcp.tool()
def submit(acct: str, ticker: str, action: str, qty: float,
           limit: float | None = None, trigger: float | None = None,
           kind: str = "LIMIT") -> dict:
    """Submit an order."""
    return {"order_ref": "AL-ORD-1", "state": "accepted"}


@mcp.tool()
def orders(acct: str, state: str = "") -> dict:
    """Working orders.

    The row's own status key is `stage`, deliberately NOT the `state` this tool
    takes as an argument and NOT the `state` Robinhood puts it under. A field
    both fixtures spelled the same way would let a hardcoded path pass on both
    connectors, which is the one thing this fixture exists to catch.
    """
    return {"working": [{"ref": "AL-ORD-1", "tkr": "aapl", "action": "BUY_TO_OPEN",
                         "qty": "25", "stage": "working"}]}


@mcp.tool()
def scrub(acct: str, ref: str) -> dict:
    """Cancel an order."""
    return {"ref": ref, "state": "cancelled"}


if __name__ == "__main__":
    mcp.run()
