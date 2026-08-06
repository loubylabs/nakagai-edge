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
        "read_only_tools": ["accounts_list", "balances", "holdings", "ticker",
                            "orders"],
        "accounts": {"allow": ["AL-1"], "arg_names": ["acct"]},
        "approvals": {"require_for": ["submit", "scrub"]},
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
        # The plain verb FIRST, then the ones the broker also answers with.
        # Every spelling here is recognized when a side is read back off an
        # order, and the first is the single one the edge sends when it places
        # one or builds a stop's exit. Leading with BUY_TO_OPEN would send that
        # for every buy, including the one meant to cover a short, which is the
        # trap README's "The order inside `values.side` is load-bearing" rule
        # warns about; a fixture every migrated path is tested against should
        # model the practice rather than the trap.
        "place_order": {
            "tool": "submit",
            "args": {"symbol": "ticker", "side": "action", "quantity": "qty",
                     "price": "limit", "stop": "trigger", "account": "acct"},
            "values": {"side": {"buy": ["BUY", "BUY_TO_OPEN", "BUY_TO_COVER"],
                                "sell": ["SELL", "SELL_TO_CLOSE", "SELL_SHORT"]}},
            "market_args": {"kind": "MARKET"}},
        "list_orders": {
            "tool": "orders",
            "args": {"account": "acct", "status": "state"},
            "items": ["working"],
            "fields": {"order_id": ["ref"], "symbol": ["tkr"], "side": ["action"],
                       "quantity": ["qty"], "status": ["stage"]},
            "values": {"side": {"buy": ["BUY", "BUY_TO_OPEN", "BUY_TO_COVER"],
                                "sell": ["SELL", "SELL_TO_CLOSE", "SELL_SHORT"]}}},
        "cancel_order": {
            "tool": "scrub",
            "args": {"order_id": "ref", "account": "acct"}},
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
        # Rooted at `data` like its five siblings, rather than repeating that
        # prefix on every field path. The portfolio sweep hands this node
        # straight to the web as raw figures, so the root has to be somewhere
        # the map states once and both readers agree on.
        "get_balance": {
            "tool": "get_portfolio",
            "args": {"account": "account_number"},
            "items": ["data"],
            "fields": {"equity": ["total_value", "equity"],
                       "cash": ["cash"],
                       "buying_power": ["buying_power.buying_power",
                                        "buying_power"],
                       "currency": ["currency"]}},
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
        # The broker that declares everything the fill journal can use: both
        # enrichment fields, and a word for `filled` so the sweep can ask for
        # filled orders instead of taking the default. The alien fixture above
        # deliberately declares neither, so both halves stay exercised.
        #
        # `tool` and `args.status` here are the REAL broker's, not this
        # fixture's to choose. Unlike ALIEN_CONNECTOR above, which exists to
        # prove translation works and may spell things however it likes, this
        # entry carries the live connector's id and url, so the platform's
        # shipped registry was written by copying it.
        #
        # It said `get_orders` and `status` until 2026-08-06. Robinhood serves
        # neither: the tool is `get_equity_orders` and the filter parameter is
        # `state`. The copy reached production, every fill-journal cycle failed
        # with "order history unreadable", and no fill the owner placed in the
        # Robinhood app ever reached the platform (chrvsd/nakagai#327, fixed
        # there by #328). These tests passed throughout, because a fixture that
        # invents a tool name and a hub that serves the invented name agree
        # with each other perfectly.
        #
        # So: change these two only against a live tool list. The platform side
        # now pins every name in its own map against a recorded one.
        "list_orders": {
            "tool": "get_equity_orders",
            "args": {"account": "account_number", "status": "state"},
            "items": ["data.orders"],
            "fields": {"order_id": ["id"], "symbol": ["symbol"], "side": ["side"],
                       "quantity": ["quantity"], "status": ["state"],
                       "fill_price": ["average_price"],
                       "filled_at": ["last_transaction_at"]},
            "values": {"side": {"buy": ["buy", "buy_to_open", "buy_to_cover"],
                                "sell": ["sell", "sell_to_open", "sell_short"]},
                       "status": {"filled": ["filled"]}}},
        "cancel_order": {
            "tool": "cancel_order",
            "args": {"order_id": "order_id", "account": "account_number"}},
    },
}
