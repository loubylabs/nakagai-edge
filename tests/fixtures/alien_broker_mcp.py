"""A broker that disagrees with Robinhood on every axis at once.

Deliberately alien. If any capability test passes against Robinhood's shape
and fails here, something upstream is still hardcoded, which is the exact
failure this fixture exists to catch.
"""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("alien-broker")

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


if __name__ == "__main__":
    mcp.run()
