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
  everything that is not a broker secret: settings, mandate, watchlist,
  strategy configs, the connector registry, guardrail policy, the approval
  queue and its signing key, and audit ingest. It issues *signed decisions*.
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
uvx --from git+https://github.com/loubylabs/nakagai-edge nakagai-edge setup <code> --platform https://api.nakag.ai
```

`setup` is idempotent: re-running it on a healthy edge just starts the server,
and it is also the repair path when something has drifted. The individual steps
remain available: `edge pair`, then `edge sync`, then `edge login <id>`, then
`edge run`. `edge status` reports pairing and policy freshness without doing
anything.

Point your agent's MCP client (OpenClaw, Claude Code, Hermes, ...) at
`http://127.0.0.1:8330/mcp/`.

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
