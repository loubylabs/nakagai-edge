# nakagai-edge

The edge connector for [Nakag.ai](https://nakag.ai): a user-run runtime that is
the only place a broker credential is ever written to disk. Your agent talks to
exactly one MCP endpoint, the edge, and never sees a token. The platform never
sees one either.

## Why an edge exists

Broker credentials could live on the platform host. That topology has two
problems no amount of hardening fixes: one platform compromise exposes every
user's brokerage, and if the platform holds the token and makes the broker
call, the platform placed the trade. The fix is a custody split:

- **Control plane: the platform (`api.nakag.ai`).** Source of truth for
  everything that is not a broker secret: settings, the mandate, the monitor
  watchlist (what an account watches) and the auto-execute allowlist (the
  separate, smaller list autopilot may trade unattended), strategy configs, the
  connector registry, guardrail policy, the approval queue and its signing key,
  and audit ingest. It issues *signed decisions*.
  It never dials a broker and never executes an edge-origin trade.
- **Data plane: the edge (this package, user-run).** Sole holder of broker
  credentials, stored locally under mode-0600 token files. Serves MCP on
  `127.0.0.1` to the agent, dials brokers with local credentials, and dials
  the platform as just another connector using the agent's own token.

**One-endpoint topology.** The edge proxies the platform's MCP tools upstream,
so an agent configures a single MCP endpoint (the edge's localhost port) and
reaches signals, watchlist, strategies, backtests, sync, and every broker
connector through it.

## Quickstart

```bash
# In the Nakag.ai web app: Agents page -> "Add agent" -> get a 10-minute pairing code.

# One command: pairs, syncs the registry, and (after you confirm at the
# prompt) opens a browser to log you into your broker, then serves.
uvx nakagai-edge setup <code> --platform https://api.nakag.ai
```

`setup` is idempotent: re-running it on a healthy edge just starts the server,
and it is also the repair path when something has drifted. The individual steps
remain available: `edge pair`, then `edge sync`, then `edge login <id>`, then
`edge run`. `edge status` reports pairing and policy freshness without doing
anything.

Point your agent's MCP client (OpenClaw, Claude Code, Hermes, ...) at
`http://127.0.0.1:8330/mcp/`.

## Live chat with your agent

`edge run` serves tools. It does not make you reachable. For that, run:

```bash
nakagai-edge listen
```

It holds the platform's chat channel open and prints one JSON object per owner
message on stdout, `{"seq", "text", "from", "at", "cursor"}`. Point your agent at
those lines and have it answer with the `send_message` tool. While it runs, the
web app's chat pane reports "Agent connected", because the platform counts an
agent as present only while a poll is genuinely held.

Notes that matter:

* **One listener per edge.** A second one refuses to start. Two would both
  receive every message and both answer it.
* **Dedupe on `seq`.** Delivery is at-least-once and `send_message` carries no
  idempotency key, so a re-delivery you answer twice posts twice.
* **A first-ever run starts from now.** It will not replay your history. After
  that the read position is kept in `cache/channel-cursor.json`, so a gap between
  runs is picked up on the next start. `--replay` (default 20) bounds that to the
  **newest** N messages of the gap, since the recent end is the part still worth
  answering; it says on stderr how many it skipped.
* Only owner messages are printed. Signals, briefings, and approval events are
  dropped rather than fed to an agent.
* **Chat is never mandate-gated.** The kill switch halts trading authority, not
  speech: a halted agent must still be able to tell you that it is halted.

## The write path

1. **Intent.** The agent calls a write tool through the edge's MCP surface.
   The edge's guardrails classify it first, fail-closed, so an intent that
   would already be denied never leaves the edge.
2. **Pending approval.** A write matching the approval policy is enqueued to
   the platform.
3. **A human approves in the web app, or the mandate does.**
4. **Signed grant.** On approve, the platform signs an Ed25519 artifact:
   `{approval_id, agent_id, connector_id, tool, args_hash, account, expires_at}`.
5. **Edge verifies and executes.** The edge checks the signature, recomputes
   `args_hash` from its own copy of the arguments, checks expiry, and re-runs
   guardrails against its own synced policy before the broker is ever dialed.
6. **Execution report.** The edge ships the outcome back and the approval
   record closes.

The platform never holds a broker credential at any point in this chain. It
authorizes; the edge acts.

## The capability layer

A broker connector is a downstream MCP server with its own name for everything:
its own tool for placing an order, its own key for an account, its own field
for a position's quantity. When those names live in the edge, a second broker
is not a config change, and the failure is not a loud one. The brake stops
seeing positions while every display goes on reporting them as guarded.

So the edge knows seven things a broker can be asked to do, and nothing about
how any particular broker spells them: `list_accounts`, `get_balance`,
`list_positions`, `get_quote`, `list_orders`, `place_order`, `cancel_order`.

Code owns the meaning and the type of every canonical field. `quantity` is a
number, `symbol` is upper-cased, `side` is `buy` or `sell`. A connector's map
owns location only: which downstream tool, which argument keys, which response
paths. That split is the safety property. A wrong map produces a visibly wrong
number or an extraction failure; it can never make `quantity` mean notional.

**The vocabulary is closed on purpose.** These seven cover shares. Options,
futures and crypto each need their own notional math and their own envelope
reasoning before they can be let in. Four option contracts at $2.50 compute as
$10 of notional against a $2,000 per-order cap, when the real exposure is
$1,000: an option's notional is contracts times premium times the 100
multiplier. A broker that adds futures tomorrow is refused by default rather
than waved through under a notional nobody checked.

**Adding a broker is data, not code.** A connector declares its map in the
registry, so a new brokerage is a registry entry rather than a release of this
package:

```yaml
- id: alien-broker
  kind: mcp-http
  role: broker
  capabilities:
    list_positions:
      tool: holdings
      args: {account: acct}
      items: [holdings]
      fields:
        symbol: [ticker]
        quantity: [qty]
        avg_price: [cost]
    place_order:
      tool: submit
      args: {symbol: ticker, side: action, quantity: qty, account: acct}
      values:
        side:
          buy: [BUY_TO_OPEN, BUY_TO_COVER]
          sell: [SELL_TO_CLOSE, SELL_SHORT]
      market_args: {kind: MARKET}
```

The agent gets seven named tools it learns once and uses against any broker.
`call_connector` remains the raw escape hatch: a broker tool outside the
vocabulary is still reachable by its own name, through the same guardrails, the
same approval queue, and the same audit record.

**Three read-only classifications, each of which fails silently.** An
unclassified tool counts as a write (`unknown_is_write`, fail closed), and
`check_accounts` denies a write that names no account whenever account tiers
exist. That pair is right for an agent and wrong for the edge acting on its own
behalf, so any tool the edge dials for itself has to be classified read-only,
either by the downstream server's own `readOnlyHint` or by the owner's
`read_only_tools` glob:

- **A connector's `get_quote` tool.** Otherwise the brake goes blind: no price,
  no breach, no fire, and every display still saying guarded.
- **A connector's `list_accounts` tool.** Otherwise account inference enqueues
  an approval instead of answering the question it was asked.
- **Anything else the edge dials on its own behalf**, for the same reason. The
  map moved the tool names out of the edge; it did not move this requirement,
  which now has to hold once per connector rather than once in total.

**The bundle schema gate.** The edge refuses a policy bundle whose
`schema_version` it does not understand. `ConnectorSpec` reaches the platform
through PyPI and pydantic ignores unknown fields, so an edge running ahead of
the platform would parse the older bundle cleanly and simply not find what the
newer shape carries. Losing the capability map that builds a stop's exit order
records every supervised position as unguarded while every display still calls
it guarded. Refusing beats half-understanding. A refused bundle leaves the
previous registry untouched and does not stamp freshness, so within the policy
TTL everything fails closed. `edge sync` reports the refusal on the spot and
`edge status` carries a `schema_error` until a sync succeeds; the fix is to
upgrade whichever side is behind, or pin an older `nakagai-edge`.

**Two things a registry entry must get right.** Both are the connector author's
job and both fail silently:

- **Every broker connector must declare `place_order.values.side`.** There is
  no default buy/sell vocabulary any more. There used to be a
  Robinhood-flavoured one, and it would have quietly mistranslated the next
  broker's spelling. Without it an entry's side cannot be classified and the
  position is recorded `blocked` with the anomaly "unclassifiable order side":
  visible, unguarded, and never acted on.
- **A connector whose responses are enveloped must root its scalar capabilities
  with `items:`.** Robinhood wraps everything in `{"data": ..., "guide": ...}`,
  so its `get_balance` needs `items: [data]` and unprefixed field paths beneath
  it. Without that the raw figures ship with the envelope still on and the
  Portfolio page renders blank.

## The brake

Every out-of-sample number in Nakagai's evidence store was measured on a
strategy that exits. Live, the agent places an entry and goes to sleep. The
brake is what exits.

When the platform grants an entry it also signs an **exit warrant** scoped to
that position: reduce-only, capped at the entry quantity, single-use, and
expiring. The edge watches the position against its approved stop and places a
market exit when the level is confirmed broken, with no agent and no model
awake. The warrant is renewed on the ordinary sync cadence.

It is armed by default, because the stop it enforces is one you already
approved when you stamped the entry. Two properties are deliberate and worth
knowing:

- **It fires on stale policy and through a platform outage.** Every other path
  in the edge refuses when policy goes stale, because every other path exists
  to restrain the agent. The brake's authority is in the signed warrant, and
  firing only reduces exposure.
- **The kill switch does not stop it.** The kill switch halts the agent.
  Killing the agent must not strip the stops off your open positions.

```bash
nakagai-edge brake status              # what is watched, and its risk in R
nakagai-edge brake off                 # disarm, locally, with no network
nakagai-edge brake off --position <id> # release one position
nakagai-edge brake on                  # re-arm
```

The brake does not promise the level. A gap opens a position under its stop and
the exit goes off at the market, below it. That is what a stop is.

A connector must declare a `place_order` capability with `market_args` before
its positions can be supervised. Without it, positions are recorded as
unguarded and reported that way rather than silently ignored.

## Failure modes

- **Platform unreachable.** The edge caches the bootstrap bundle with a policy
  TTL (default 15 minutes). Reads may continue on the cached policy while the
  TTL holds; once it expires, everything is refused. Writes are impossible by
  construction the whole time: a write needs a live round trip to the
  platform's approval queue.
- **Revocation.** Revoking an agent takes effect on the agent's next platform
  call: the bearer token 401s. Writes were already gated on a live platform
  round trip, so revocation closes them structurally.

## Development

```bash
uv sync
uv run pytest
```

A handful of integration tests exercise the edge against the Nakag.ai platform
package and skip automatically when it is not installed. The import closure of
`nakagai_edge` itself is intentionally small (no pandas, numpy, or pyarrow) and
enforced by `tests/test_import_closure.py`.
