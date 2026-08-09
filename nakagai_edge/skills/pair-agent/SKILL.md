---
name: pair-agent
description: Pair and onboard a new MCP agent with the hosted Nakagai platform (app.nakag.ai / api.nakag.ai), either directly or through nakagai-edge. Use when asked to install the edge package, configure Codex, Claude Code, or another MCP client with an nk_agent token, exchange an edge pairing code, verify the connection, or run the first-session protocol.
---

# Pair an agent with the hosted Nakagai platform

You are the agent being paired. Complete the chosen path through verification;
do not stop after writing client configuration. Pairing gets you an
`nk_agent_...` bearer token and points your MCP client at the right endpoint.
Two topologies:

- **Direct**: you dial the platform's own MCP at `https://api.nakag.ai/mcp/`.
  Right for signals, research, strategies, backtests. No broker access.
- **Through an edge**: you dial one localhost endpoint
  (`http://127.0.0.1:8330/mcp/`) served by a user-run `nakagai-edge`, which
  proxies the platform tools AND holds the broker credentials. Required for
  anything that touches a broker. Setting the edge itself up is the
  `connect-edge` skill; this skill covers getting the credential.

Either way your token opens exactly two surfaces, `/mcp` and `/api/agent/*`,
and nothing else. You can stage an order but structurally cannot approve one:
approval needs a different token you never hold plus an allowlisted human
login. Do not try to reach other `/api/*` routes; they will 401 and that is
the design, not a bug to work around.

## Step 1: a human mints the credential

You cannot mint your own credential; the owner does it in the web app:
**app.nakag.ai, Agents page, "Add agent"**. Two modes:

- **Direct mode**: shows the `nk_agent_...` token once, plus ready-to-paste
  client config. Have the user copy it immediately; it is stored hashed and
  cannot be re-shown.
- **Edge mode**: shows a single-use pairing code valid for 10 minutes. The
  code is not the credential; it is exchanged for one (step 2b).

Self-hosters can also mint on the platform machine itself. This one is not an
edge command and there is no `uvx` form of it: it is the platform's own CLI, run
from a platform checkout with that checkout's environment loaded.

```bash
set -a && source .env.local && set +a
uv run nakagai agent pair --name <agent-name>
```

Revocation is the same page and takes effect on the token's next platform
call.

## Step 2a: direct connection (token in hand)

First verify the token without printing it. A `200` proves that the hosted
agent surface accepts it:

```bash
read -s NAKAGAI_AGENT_TOKEN
export NAKAGAI_AGENT_TOKEN
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer $NAKAGAI_AGENT_TOKEN" \
  https://api.nakag.ai/api/agent/bundle
```

Configure the client the owner actually named. Do not configure every client
found on the machine.

Codex stores remote MCP configuration globally. Keep the token in the
environment rather than writing it into `~/.codex/config.toml`:

```bash
codex mcp add nakagai --url https://api.nakag.ai/mcp/ \
  --bearer-token-env-var NAKAGAI_AGENT_TOKEN
codex mcp list
```

The environment variable must be present in the process that launches Codex.
After adding the server, start a new Codex session; editing the configuration
cannot add tools to the already-running session.

Claude Code can store the header in its user-scoped MCP configuration:

```bash
claude mcp add --transport http nakagai-platform https://api.nakag.ai/mcp/ \
  --header "Authorization: Bearer $NAKAGAI_AGENT_TOKEN"
```

For another MCP client, adapt this client-neutral block. Use its secret or
environment-variable facility if it has one; never commit the expanded token:

```json
{
  "mcpServers": {
    "nakagai": {
      "type": "http",
      "url": "https://api.nakag.ai/mcp/",
      "headers": {
        "Authorization": "Bearer ${NAKAGAI_AGENT_TOKEN}"
      }
    }
  }
}
```

Two rules that prevent the two classic failures:

- **Keep the trailing slash on `/mcp/`.** The bare path 307-redirects, and
  behind the TLS-terminating proxy the redirect Location is `http://`, which
  makes httpx-based clients silently drop the Authorization header. Result:
  a 401 with a perfectly valid token.
- **Keep the token out of committed files.** Prefer `claude mcp add` (stores
  config outside the repo) over a `.mcp.json` block; if `.mcp.json` is used,
  reference the token as an env expansion, never inline.

## Step 2b: edge connection (pairing code in hand)

Install the published CLI as a user-level tool, then let `setup` perform the
whole supported sequence: exchange the code, save both the token and real
`agent_id` with private permissions, sync connector policy, register detected
clients, and serve the local MCP endpoint.

```bash
uv tool install nakagai-edge
nakagai-edge --version
nakagai-edge setup <code> --platform https://api.nakag.ai
```

Use `uv tool upgrade nakagai-edge` when it is already installed. `uvx
nakagai-edge@latest setup ...` is the no-install equivalent.

Do not copy a direct-mode token into `~/.nakagai/edge/agent.json`. The edge
also needs the matching `agent_id` for signed approvals and warrants; a blank,
guessed, or mismatched id leaves read-only calls looking healthy while the
approval path fails. Have the owner mint an **Edge mode** code and use `setup`
or `pair`.

If setup is intentionally driven in pieces:

```bash
nakagai-edge pair <code> --platform https://api.nakag.ai
nakagai-edge sync
nakagai-edge connect
nakagai-edge restart
nakagai-edge status
```

The local endpoint is `http://127.0.0.1:8330/mcp/`. `connect` only registers
clients it recognizes; for any other client, use the configuration block it
prints. Do not replace an explicitly requested direct hosted connection with
the local edge unless the owner chose edge topology.

The underlying pairing exchange, for a client that must implement it, is:

```
POST https://api.nakag.ai/api/agents/pair
{"code": "<pairing code>"}
```

Public by design (the caller has no credential yet), defended in depth:
rate-limited per IP, single-use codes, 10-minute expiry. Success returns
`{"ok": true, "agent_id": ..., "token": "nk_agent_..."}`; store the token
with 0600 permissions. Any failure is an opaque `403 pairing failed` that
deliberately does not say which check tripped; a 429 means back off a
minute. A code that failed once may already be burned; when in doubt have
the human mint a fresh one.

## Step 3: first-session protocol

Once connected, in order:

1. `get_mandate()`: your marching orders; market phase, what is permitted,
   whether to stay live.
2. `agent_checkin(status, note)`: heartbeat for the owner's activity feed.
   Once per session. If `mandate.directives.report_equity` is true, include
   `account_equity` and `day_pnl` together or not at all.
3. `get_signals(since="today")` on the very first call to bootstrap your
   cursor; afterwards call it bare, the cursor is keyed to your token.
4. Run `uvx nakagai-edge listen`. It holds the channel for you, so there is no
   `await_events` loop to write: one JSON object per event, on stdout, with
   the `seq` you should dedupe on.
   **Answer a line only when `reply_expected` is true**, with
   `send_message(text)`. Two kinds carry it:
   - `owner_msg`, the owner talking to you.
   - `signal_referred`, one setup someone put a question mark against. Either
     the owner pressed "Ask the agent", or their own confluence dial cleared
     on its own; `referred_by` is their email in the first case and the
     literal `"confluence"` in the second. Answer it about that specific
     setup, citing the `signal_id` it names. It grants you no authority: it
     is a request to look, not to act, and the mandate still decides what you
     may do about what you find.

   Every other kind, a `signal`, a `market_event`, an `approval_decided`, a
   `mandate_changed`, is context you absorb silently; hundreds of signals and
   events a session means replying to each would bury the owner's conversation
   in their own pane. A `market_event` is one recorded observation about one
   symbol on one bar: `detector` names what fired (`sharp_move`, a volume
   surge, a new range extreme, an opening gap) and `magnitude` carries its
   numbers. It gives no direction and no entry, stop or target, so it
   authorizes nothing; it is background for what you already watch.
   Chat is never mandate-gated, so keep answering even when halted; say that
   you are halted. Delivery is at-least-once and `send_message` has no
   idempotency key, so track which `seq` values you have already answered.
   `pending_messages` on a check-in is the same set of messages; treat it as
   informational.

## Verify

For a direct client, confirm its MCP server list says `nakagai` is enabled,
start a fresh client session, and list tools. A populated list containing
`get_mandate` proves the direct connection.

For an edge, require all of these:

1. `nakagai-edge status` reports paired, fresh policy, and a running daemon.
2. The local MCP client's `get_connector_status` reports `nakagai-mcp`.
3. `list_connector_tools("nakagai-mcp")` returns a populated tool list with
   `get_mandate` and no auth error.

Then run step 3 and confirm the check-in lands on the owner's activity feed.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| 401 at `/mcp/` with a fresh token | Wrong token type (platform API token where an agent token belongs), or the agent was revoked | Mint a new agent token; never use `NAKAGAI_API_TOKEN` as an agent |
| 401 only through a client, while curl with the same token works | Missing trailing slash; auth header stripped following the `http://` redirect | Use `https://api.nakag.ai/mcp/` exactly |
| `403 pairing failed` | Code expired (10 min), already used, or mistyped; deliberately unspecified | Mint a fresh code |
| `429 too many pairing attempts` | Per-IP rate limit | Wait a minute, then one careful retry |
| `421 Misdirected Request` (self-hosted) | Hostname not in `NAKAGAI_MCP_ALLOWED_HOSTS` | Add the public hostname to that env var |
| Staged order never gets approved | Approval is human-side in the web app; you cannot approve | Tell the owner it is waiting on the Approvals page |

## Related

- `https://app.nakag.ai/docs/agent-pairing` (token model, the three secrets,
  full loop walkthrough). The docs viewer is behind a login, so it needs the
  owner's session.
- `https://app.nakag.ai/docs/edge` (custody split, write path)
- `connect-edge` skill (running and debugging the edge itself)
