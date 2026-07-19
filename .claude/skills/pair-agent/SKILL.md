---
name: pair-agent
description: Pair a new agent with the hosted Nakagai platform (app.nakag.ai / api.nakag.ai), directly or through an edge, and run the first-session protocol. Use when a new user asks to connect an agent, mint or exchange a pairing token, or set up Claude Code against the hosted app.
---

# Pair an agent with the hosted Nakagai platform

You are the agent being paired. Pairing gets you an `nk_agent_...` bearer
token and points your MCP client at the right endpoint. Two topologies:

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

Self-hosters can also mint on the platform machine itself:

```bash
set -a && source .env.local && set +a
uv run nakagai agent pair --name <agent-name>
```

Revocation is the same page and takes effect on the token's next platform
call.

## Step 2a: direct connection (token in hand)

```bash
claude mcp add --transport http nakagai-platform https://api.nakag.ai/mcp/ \
  --header "Authorization: Bearer nk_agent_..."
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

Normally the edge CLI does the exchange (`nakagai-edge setup <code>
--platform https://api.nakag.ai`; see the `connect-edge` skill). The
underlying contract, for any client that must do it by hand:

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
4. While `directives.live_link` is true, loop `await_events(timeout_s,
   cursor)`; an empty batch after ~50s is idle rhythm, not a disconnect.
   Answer `owner_msg` events with `send_message(text)`. When `live_link` is
   false, do not hold the line; self-schedule from
   `directives.check_interval_minutes` and re-check the mandate on wake.

## Verify

Ask for the platform tool list (direct: list tools on the connection; edge:
`list_connector_tools("nakagai-mcp")`). A populated list with `get_mandate`
present means paired and connected. Then run step 3 and confirm the check-in
lands on the owner's activity feed.

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

- `docs/public/agent-pairing.md` (token model, the three secrets, full loop
  walkthrough)
- `docs/public/edge.md` (custody split, write path)
- `connect-edge` skill (running and debugging the edge itself)
