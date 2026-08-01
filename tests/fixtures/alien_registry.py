"""Registry entries used across the capability tests.

Two connectors that do the same seven things in completely different words.
Every migrated path is tested against both.
"""

ALIEN_CONNECTOR = {
    "id": "alien-broker",
    "name": "Alien broker (fixture)",
    "kind": "mcp-stdio",
    "role": "broker",
    "command": "uv",
    "args": ["run", "python", "tests/fixtures/alien_broker_mcp.py"],
    "enabled": True,
    "guardrails": {
        "allow_writes": True,
        "read_only_tools": ["accounts_list", "balances", "holdings", "ticker"],
        "accounts": {"allow": ["AL-1"], "arg_names": ["acct"]},
        "approvals": {"require_for": ["submit"]},
    },
    "capabilities": {
        "list_accounts": {
            "tool": "accounts_list",
            "items": ["accounts"],
            "fields": {"account": ["acct"], "nickname": ["label"],
                       "type": ["kind"]}},
        "get_balance": {
            "tool": "balances",
            "args": {"account": "acct"},
            "fields": {"equity": ["net_liq"], "cash": ["settled_cash"],
                       "buying_power": ["power"], "currency": ["denom"]}},
        "list_positions": {
            "tool": "holdings",
            "args": {"account": "acct"},
            "items": ["holdings"],
            "fields": {"symbol": ["ticker"], "quantity": ["qty"],
                       "avg_price": ["cost"]}},
        "get_quote": {
            "tool": "ticker",
            "args": {"symbols": "tickers"},
            "items": ["ticks"],
            "fields": {"symbol": ["tkr"], "price": ["last"]}},
        "place_order": {
            "tool": "submit",
            "args": {"symbol": "ticker", "side": "action", "quantity": "qty",
                     "price": "limit", "stop": "trigger", "account": "acct"},
            "values": {"side": {"buy": ["BUY_TO_OPEN", "BUY_TO_COVER"],
                                "sell": ["SELL_TO_CLOSE", "SELL_SHORT"]}},
            "market_args": {"kind": "MARKET"}},
    },
}

ROBINHOOD_CONNECTOR = {
    "id": "robinhood-trading",
    "name": "Robinhood Trading MCP",
    "kind": "mcp-http",
    "role": "broker",
    "url": "https://agent.robinhood.com/mcp/trading",
    "enabled": True,
    "guardrails": {
        "allow_writes": True,
        "read_only_tools": ["get_*"],
        "accounts": {"allow": ["463605220"]},
        "approvals": {"require_for": ["place_*", "cancel_*"]},
    },
    "capabilities": {
        "list_accounts": {
            "tool": "get_accounts",
            "items": ["data.accounts"],
            "fields": {"account": ["account_number"], "nickname": ["nickname"],
                       "type": ["type", "brokerage_account_type"]}},
        "get_balance": {
            "tool": "get_portfolio",
            "args": {"account": "account_number"},
            "fields": {"equity": ["data.total_value", "data.equity"],
                       "cash": ["data.cash"],
                       "buying_power": ["data.buying_power.buying_power",
                                        "data.buying_power"],
                       "currency": ["data.currency"]}},
        "list_positions": {
            "tool": "get_equity_positions",
            "args": {"account": "account_number"},
            "items": ["data.positions", "data.results"],
            "fields": {"symbol": ["symbol"], "quantity": ["quantity"],
                       "avg_price": ["average_buy_price"]}},
        "get_quote": {
            "tool": "get_quotes",
            "args": {"symbols": "symbols"},
            "items": ["data.quotes"],
            "fields": {"symbol": ["symbol"],
                       "price": ["last_trade_price", "mark_price"]}},
        "place_order": {
            "tool": "place_equity_order",
            "args": {"symbol": "symbol", "side": "side", "quantity": "quantity",
                     "price": "limit_price", "stop": "stop_price",
                     "account": "account_number"},
            "values": {"side": {"buy": ["buy", "buy_to_open", "buy_to_cover"],
                                "sell": ["sell", "sell_to_open", "sell_short"]}},
            "market_args": {"type": "market", "time_in_force": "gfd"}},
    },
}
