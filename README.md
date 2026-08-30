# nakagai-edge

The edge connector for [Nakag.ai](https://nakag.ai): a user-run runtime that is
the only place a broker credential is ever written to disk. Your agent talks to
exactly one MCP endpoint, the edge, and never sees a token. The platform never
sees one either.

Version 0.5.2 is the current release. An agent woken for one execution
candidate can inspect that candidate, accept or abstain with a rationale, then
stop. A listener-owned local scope enforces that boundary for the wake. The
same candidate decision tools and read-only inspection remain available, while
every other write is refused until the wake ends or expires. The agent cannot
alter a prepared order field. The platform compiles the order, and local policy
or the brake can still refuse execution. Semantic
`place_order` remains the only owner-mediated order entry: a raw
`call_connector` request whose tool exactly matches the selected connector's
declared `capabilities.place_order.tool` is refused before dispatch with
`canonical_order_required`. Other raw connector operations remain available.

A listener started with a parsed, nonempty `--wake-command` adds
`X-Nakagai-Candidate-Wake: 1` only to its authenticated event polls. This
attests that the current listener is configured to wake an agent. It remains a
self-reported readiness fact and grants no execution authority. The separate
`X-Nakagai-Candidate-Protocol: 1` header continues to mean only that the poll
understands execution candidate events.

Every local approval queue operation still requires an explicit `account_key`.
A paired edge uses its stable `agent_id` for that key. The key stays local; the
hosted platform resolves account authority from the bearer token.

The queue exposes two bounded owner reads. `history(account_key, statuses=...,
limit=..., before=...)` returns uncleared records in terminal-occurrence order.
`attention(account_key, limit=..., after=...)` returns pending and
outcome-unknown records with an exact account-scoped total and oldest time.
Both file and PostgreSQL modes apply the account predicate before ordering,
cursoring, and limiting. Neither read derives tenant authority from stored data.

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

**One-endpoint topology.** Your agent points at one URL,
`http://127.0.0.1:8330/mcp/`, and finds the whole surface there: the edge's own
tools (the broker vocabulary, approvals, the brake, check-in and chat) beside
the platform's own tools, which the edge promotes to first-class names when it
starts. `get_signals`, `get_mandate`, `get_roster`, `run_backtest` and the rest
are called by name, not through a generic escape hatch.

## Quickstart

```bash
# In the Nakag.ai web app: Agents page -> "Add agent" -> get a 10-minute pairing code.

# One command: pairs, syncs the registry, (after you confirm at the prompt)
# opens a browser to log you into your broker, connects your agent, then serves.
uvx nakagai-edge setup <code> --platform https://api.nakag.ai
```

`setup` is idempotent: re-running it on a healthy edge just starts the server,
and it is also the repair path when something has drifted. The individual steps
remain available: `nakagai-edge pair`, then `nakagai-edge sync`, then
`nakagai-edge login <id>`, then `nakagai-edge run`.

Two commands answer for the daemon once it is up:

```bash
nakagai-edge status     # pairing, policy freshness, what is running, what is available
nakagai-edge restart     # stop it and start a fresh one, detached, with a log
```

`status` does nothing but report. Its JSON goes to stdout, so a pipe into `jq`
keeps working, and the upgrade advice goes to stderr when there is any.

`restart` stops the daemon it can prove is its own, then relaunches through the
same install you invoked it from, detached, with output appended to
`~/.nakagai/edge/edge.log`. It comes back on the port the old daemon served,
takes no `--port`, and waits for the port to answer before reporting success.
It refuses rather than guessing: while the brake is watching an open position
(`--force` overrides), and when something is on the port that this edge cannot
prove is its own, which is what a daemon started before the pidfile existed
looks like. That first restart on an existing machine is still done by hand.
The stop is SIGTERM and only SIGTERM; a process holding your broker credentials
is not something a convenience command escalates on.

## Connect your agent

`setup` wires up any agent client it recognizes, and prints the endpoint for any
it does not. Already set up? `nakagai-edge connect` does the wiring alone,
without serving:

```bash
uvx nakagai-edge connect
```

The one client recognized today is Claude Code, detected by `claude` being on
your PATH. It gets an MCP entry added with
`claude mcp add --scope user nakagai --transport http <url>`, at user scope
because a project-scoped entry needs a per-project approval, and the nine skills
below are copied into `~/.claude/skills/`. A client that is not detected is not
an obstacle: the snippet below is the whole contract.

`--no-register` withholds the wiring, never the endpoint. Both
`setup --no-register` and `connect --no-register` still print the URL and the
snippet, and touch no client config at all.

The contract is one URL, and it carries no credential:

```
http://127.0.0.1:8330/mcp/
```

Paste this into any MCP client:

```json
{
  "mcpServers": {
    "nakagai": {
      "type": "http",
      "url": "http://127.0.0.1:8330/mcp/"
    }
  }
}
```

The edge holds your platform token and your broker credentials. Neither ever
enters your agent's config.

### What is on the endpoint

The edge serves its own broker, approval, check-in, and room-aware chat tools,
then promotes the eligible platform tools to first-class names. Local tools win
when a name overlaps. Nothing is exposed twice or prefixed. `await_events` is
never promoted into the edge MCP server. `nakagai-edge listen` is the event
reader for an edge-connected agent.

A promoted name is the same call typed a shorter way. It travels the same
guarded door as `call_connector`: the same classification, the same approval
policy, the same audit record, and the same refusal once cached policy goes
stale. Promotion changed which names exist, not what any of them is allowed to
do.

Promotion happens once, at startup, before the first client connects. If the
platform is unreachable at that moment, the promoted names are absent for the
life of that process and the log says so;
`call_connector("nakagai-mcp", ...)` still reaches every one of them, and a
restart picks them up. If the platform goes down after startup, the tools stay
listed and a call comes back with an error naming the `nakagai-mcp` connector,
because a name that fails legibly beats a set of tools that silently vanish.

## Skills

Nine skills ship inside the wheel:

- **`connect-edge`**: connect a local edge to a hosted platform, and diagnose
  the known failure modes.
- **`pair-agent`**: pair a new agent with the hosted platform, directly or
  through an edge, and run the first-session protocol.
- **`verify-edge`**: the health ladder, from the local edge up to the platform
  relay, with an opt-in write-path drill through approvals.
- **`daily-brief`**: signals, open risk, portfolio and pending approvals in one
  pass.
- **`halt`**: stop trading authority now, and say precisely what is and is not
  stopped.
- **`check-the-evidence`**: pull a play's proving record before endorsing it,
  and say so plainly when there is none.
- **`nakagai-chat`**: hold the owner's live chat channel open and answer
  messages as they arrive.
- **`verify`**: launch an isolated local platform and verify web, API, and MCP
  changes end to end.
- **`candidate-trader`**: inspect one execution candidate, choose accept or
  abstain with a rationale, then stop. Accepting cannot change a prepared
  field, and local policy or the brake can still refuse execution.

A client that reads skills as files gets them installed by `connect` (Claude
Code: `~/.claude/skills/`). Any MCP client can read exactly the same text off
the endpoint instead: `nakagai://skills` lists them with their descriptions,
`nakagai://skills/{name}` is one skill's full text, and each is offered as an
MCP prompt under its own name.

An edit of yours is never overwritten. `connect` records a hash of what it
wrote, so a later run replaces only a file that still matches, and a skill you
have tuned is left alone and reported as left alone.

## Live chat with your agent

`nakagai-edge run` serves tools. It does not make you reachable. For that, run:

```bash
nakagai-edge listen
```

It holds the platform's chat channel open and prints one safe, server-routed JSON
object per eligible event on stdout. Point your agent at those lines and have it
use the local chat tools. While it runs, the web app's chat pane reports "Agent
connected", because the platform counts an agent as present only while a poll is
genuinely held.

Every emitted line retains the event envelope fields `seq`, `kind`, `at`, and
`cursor`, plus these server-authored routing fields when present: `room_id`,
`reply_to_seq`, `sender_agent_id`, `dispatch_mode`, `response_required`,
`claim_required`, `claim_expires_at`, `retry_at`, `recipient_status`,
`recipient_count`, `source_seq`, and `hop_count`. The edge only renders the safe
body fields for that event kind. It does not infer room membership, claim
authority, or whether a reply is required.

The local chat tools are:

- `list_peers()`, which returns the live, same-account peer directory.
- `claim_message(message_seq)`, which claims or renews an actionable Anyone
  request.
- `send_message(text, room_id, idempotency_key, reply_to_seq=0)`, which sends
  a room-scoped message or linked reply.
- `request_peer(agent_ids, text, idempotency_key, source_seq=0)`, which creates
  an owner-visible Desk request for selected peers.

Notes that matter:

* **One listener per edge.** A second one refuses to start. Two would both
  receive the same routed work and could race to claim it.
* **Dedupe on `seq`, retry with `idempotency_key`.** Delivery is at-least-once.
  Preserve a stable idempotency key when retrying a claim-linked response or a
  peer request, so the platform returns the original accepted write instead of
  publishing another one.
* **A first-ever run starts from now.** It will not replay your history. After
  that the read position is kept in `cache/channel-cursor.json`, so a gap between
  runs is picked up on the next start. `--replay` (default 20) bounds that to the
  **newest** N messages of the gap, since the recent end is the part still worth
  answering; it says on stderr how many it skipped.
* **Read the envelope before acting.** `response_required` is server-authored.
  An addressed `agent_msg` can be context only, while an `agent_request` can
  require a response. Signals and approvals are emitted as non-actionable
  context when their registered renderer admits them. Hidden or unregistered
  events advance the cursor without becoming stdout lines.
* **The listener does not spend agent turns unless you opt in.** Pass
  `--wake-command` to start one local process for each response-required event.
  The rendered event arrives as JSON on stdin, no shell interprets owner text,
  and a second process never overlaps the first. For example:

  ```bash
  nakagai-edge listen --wake-command \
    "codex exec -C /path/to/workspace 'Use the nakagai-chat skill. Handle the JSON event supplied on stdin and reply through Nakagai when response_required is true.'"
  ```

  This starts bounded non-interactive Codex turns. It cannot inject a message
  into an already-open Codex app conversation, and non-actionable context does
  not start a turn.
* **Candidate wakes are one decision.** An `execution_candidate` is an
  addressed, response-required event. Use `candidate-trader`: inspect one
  candidate, choose accept or abstain, provide a concise rationale, and stop.
  An acceptance leaves every prepared field under platform control. Local
  policy and the brake retain their authority to refuse execution.
* **Claim first when required.** If `claim_required` is true, call
  `claim_message(seq)` before reasoning or using another tool. A `409` is an
  ordinary coordination outcome. `already_claimed`, `claim_lost`, and
  `already_responded` return their structured JSON bodies, including `retry_at`
  when the platform has one. A retry hint can schedule work, but the next
  `agent_checkin` also returns unresolved, claimable work.
* **Keep replies linked and room-scoped.** Use the event's `room_id`, its `seq`
  as `reply_to_seq`, and a stable `idempotency_key` with `send_message`. Use
  `list_peers` before `request_peer`; peer requests are visible to the owner in
  Desk and are never a private agent channel.
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

So the generic broker surface knows seven things a broker can be asked to do, and nothing about
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
      args:
        symbol: ticker
        side: action
        order_type: kind
        quantity: qty
        limit_price: limit
        stop_price: trigger
        time_in_force: tif
        account: acct
      outbound_types:
        symbol: string
        side: string
        order_type: string
        quantity: string
        limit_price: string
        stop_price: string
        time_in_force: string
        account: string
      values:
        side:
          buy: [BUY]
          sell: [SELL]
        order_type:
          limit: [LIMIT]
          market: [MARKET]
```

**A `place_order` map has to name every canonical outbound field**: symbol,
side, order_type, quantity, limit_price, stop_price, time_in_force, and
account. The edge reads an executed entry back through symbol, side, quantity,
limit_price, and stop_price to
build the ledger record the brake watches, so a map missing one places real
orders that are then supervised by nothing, absent from `get_open_risk` while
the Portfolio page still lists them. A connector declaring an incomplete
`place_order` is refused when the registry is parsed, by name and by which keys
are missing, rather than found later by a position that had no stop watching
it. A connector that places no orders at all simply declares no `place_order`.

The model-facing `place_order` tool creates limit entries only. Quantity must
be a positive whole-share count, and both the limit and protective stop must
be positive. Market orders carrying either priced field are refused. The only
market orders the edge constructs are reduce-only warrant exits, and those
payloads omit both priced fields.

**The order inside `values.side` is load-bearing.** The list is every spelling
this connector recognizes when it reads a side back off an order, and the first
entry is the single spelling the edge sends when it places one or builds a
stop's exit. So list them all, and put first the one that is correct whether the
order opens or closes. A broker with separate verbs mapped as
`buy: [BUY_TO_OPEN, BUY_TO_COVER]` sends `BUY_TO_OPEN` for every buy, including
the one meant to cover a short.

The generic broker surface exposes seven named tools that work against any
broker. The shared MCP surface remains present during an execution-candidate
wake. Read-only inspection remains allowed. The edge enforces
`accept_candidate` and `abstain_candidate` as the only write actions for the
same candidate during that wake. Connector writes and order construction are
refused in code until the wake ends or expires.
`connector_id` is optional only while exactly one enabled broker declares the
capability. Enable a second and the edge stops filling it in: the call comes
back naming both candidates and the agent has to say which brokerage it meant.
Letting registry order decide which broker received an order is not something
an agent can review or an owner can predict, so this is the one place the layer
gets louder rather than quieter as brokers are added.

`call_connector` remains the raw escape hatch for a broker tool outside the
vocabulary. A connector's declared `place_order` tool is the exact exception:
calling that raw name returns `canonical_order_required`, so every order starts
through semantic `place_order`. Other raw operations still use the same
guardrails, approval queue, and audit record.

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
previous registry untouched and does not stamp freshness, so the cached policy
goes on aging and everything is refused once the TTL lapses. `nakagai-edge sync`
reports the refusal on the spot and `nakagai-edge status` carries a
`schema_error` until a sync succeeds; the fix is to
upgrade whichever side is behind, or pin an older `nakagai-edge`.

**Two things a registry entry must get right.** Both are the connector author's
job and both fail silently:

- **Every broker connector must declare `place_order.values.side`.** There is
  no default buy/sell vocabulary any more. There used to be a
  Robinhood-flavored one, and it would have quietly mistranslated the next
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
nakagai-edge brake on                  # re-arm stops and candidate entries
```

An urgent candidate supervision failure leaves `candidate_entries_armed`
false in `brake status` while existing warrant exits remain armed. Inspect the
reported position and broker outcome first. `brake on` clears that local entry
lockout when the owner is ready to permit another candidate.

The brake does not promise the level. A gap opens a position under its stop and
the exit goes off at the market, below it. That is what a stop is.

A connector must declare a complete `place_order` capability before its
positions can be supervised. The canonical market exit uses the same resolver
as an entry, with `order_type: market`. A connector that cannot express it gets
a ledger record marked unguarded and shown that way by `get_open_risk` and the
Portfolio page. A connector that declares no `place_order` at all leaves no
ledger record to make, so its positions are absent from `get_open_risk`
entirely; the Portfolio page still shows them unguarded, because a position
with no record cannot be marked guarded. The Portfolio page is the surface that
sees both, so it is the one to check before assuming a stop is being watched.

## The fill journal

Positions you open by hand, in your broker's own app, already reach the
platform: the portfolio sweep reports what the broker holds, not what the agent
did, so they are on the Portfolio page like any other. What they lack is
everything downstream of the supervision ledger. There is no approval behind
them, so there is no ledger record, no stop, no exit warrant, and nothing that
will exit them. They are absent from `get_open_risk` and contribute nothing to
portfolio heat, like any broker position with no supervision record.

The fill journal is what tells the platform they were yours. Every
`FILLS_INTERVAL_S` (300s) the edge reads each broker's order history through
its `list_orders` capability, records what it has not seen before, and ships it
to the platform. A connector that declares no `list_orders` has no history to
read and is skipped in silence.

Three properties are deliberate:

- **First sight anchors; it does not backfill.** The first sweep of an account
  records the order ids it finds and ships **nothing**. `list_orders` has no
  `since` argument, so without this the first sweep would upload however much
  of your personal trading history the broker chose to return. The journal
  means "since you connected", and the anchor is never evicted.
- **Attribution joins on the broker's own order id.** When the edge places an
  order it reads the broker's id for it off the `place_order` map and reports
  it, so the platform can match a fill to the approval that caused it. A fill
  no approval claims is one you placed. "Not in the supervision ledger" would
  not do: a stopless agent order is not in it either.
- **Nothing labels a position.** A broker's position rows carry no order id, so
  a holding you bought part of by hand and part through the agent has no clean
  origin. The Portfolio page therefore says nothing about origin at all, rather
  than splitting a row by arithmetic that drifts wrong after the first transfer
  or corporate action. The journal is the record.

The journal is local-first, like the audit trail and for the same reason: it is
written here before the platform is told, so an outage costs latency and never
a record.

## Failure modes

- **Platform unreachable.** The edge caches the bootstrap bundle with a policy
  TTL (default 15 minutes). Reads may continue on the cached policy while the
  TTL holds; once it expires, everything is refused. Writes are impossible by
  construction the whole time: a write needs a live round trip to the
  platform's approval queue. An edge that started while the platform was down
  serves its own 16 tools and none of the promoted ones, since the tool list is
  built once at startup; run `nakagai-edge restart` once the platform answers.
- **Revocation.** Revoking an agent takes effect on the agent's next platform
  call: the bearer token 401s. Writes were already gated on a live platform
  round trip, so revocation closes them structurally.

- **A newer release exists.** `nakagai-edge status` names it, alongside the
  version the platform ships, and prints the upgrade line for the install shape
  it detected. That is the whole behavior: the edge never updates itself, and
  it never refuses to start. This daemon is the sole holder of your broker
  credentials, so replacing it is your decision, not a web index's. No network
  means `latest_version: null`, which is why that key is null rather than empty
  when the index says nothing: "you are current" and "nobody answered" must not
  look the same. On the quickstart's uvx install the upgrade is one command,
  `uvx nakagai-edge@latest restart`, which stops the old daemon and relaunches
  from latest.

## Development

```bash
uv sync
uv run pytest
```

A handful of integration tests exercise the edge against the Nakag.ai platform
package and skip automatically when it is not installed. The import closure of
`nakagai_edge` itself is intentionally small (no pandas, numpy, or pyarrow) and
enforced by `tests/test_import_closure.py`.
