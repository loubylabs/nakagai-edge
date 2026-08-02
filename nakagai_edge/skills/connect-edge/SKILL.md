---
name: connect-edge
description: Connect a local nakagai-edge to a hosted platform (pair, sync, run, verify) and diagnose the known failure modes, including the Fly-proxy 401. Use when asked to connect, start, re-pair, or debug the edge against app/api.nakag.ai or any hosted platform.
---

# Connect the edge to a hosted platform

The edge is the user-run process that holds broker credentials and serves MCP
to agents on `127.0.0.1:8330`. Connecting it to a platform means: paired
(agent token on disk), synced (registry + policy fresh), running (port 8330
up), and the `nakagai-mcp` connector actually reaching the platform's `/mcp/`
endpoint. Verify all four; the first three can be healthy while the fourth
401s.

Every command below runs from anywhere, with no checkout of anything. `uvx
nakagai-edge ...` fetches the published package and runs it; if the package is
already installed, the `nakagai-edge` entrypoint on PATH is the same binary and
either form works.

## 1. Establish current state

```bash
uvx nakagai-edge status
```

- `"paired": true` with the right `platform_url`: skip to step 3.
- Not paired, or paired to the wrong platform: step 2.
- `policy_fresh: false` just means the edge has not synced recently; it does
  not require re-pairing.

State lives in `~/.nakagai/edge/`: `agent.json` (platform URL + agent token),
`config/connectors.yaml` (synced registry), `cache/meta.json` (bundle etag +
fetch stamp), `secrets/tokens/<id>.json` (broker OAuth tokens, mode 0600).

## 2. Pair (only when not paired)

Pairing needs a 10-minute code from the platform web app: Agents page, "Add
agent". A human must fetch it; there is no CLI path to mint one from the edge
side.

```bash
uvx nakagai-edge setup <code> --platform https://api.nakag.ai
```

`setup` is idempotent (pair, sync, optional broker login, serve). To drive
the steps individually: `pair <code> --platform <url>`, then `sync`, then
`login <broker-id>` if a broker needs OAuth, then `run`.

## 3. Sync, then serve

```bash
uvx nakagai-edge sync
uvx nakagai-edge run          # long-running; background it
```

`run` serves MCP on `http://127.0.0.1:8330/mcp/` and starts the sync,
executor, audit-ship, and portfolio loops. Point the agent's MCP client at
that one URL; it carries platform tools, broker tools, and approval polling.

Registry rewrites are etag-gated: `sync` rewrites `config/connectors.yaml`
only when the platform bundle changed. So a registry that looks stale after an
edge upgrade has simply never been rewritten. Drop the cached etag to force it:

```bash
rm ~/.nakagai/edge/cache/meta.json && uvx nakagai-edge sync
```

## 4. Verify, in order

1. Port up: `curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8330/mcp/`
   returns `307` (redirect into the MCP transport; that is the healthy answer
   for a bare GET).
2. Over MCP, call `get_connector_status`: expect `policy_fresh: true` and
   `nakagai-mcp` with `status: connected` and a nonzero `tool_count`.
3. Over MCP, call `list_connector_tools` for `nakagai-mcp`: expect the
   platform tool list with per-tool `policy` verdicts, no `is_error`.

Do not stop at step 1 or 2 alone: connectors dial lazily, so `nakagai-mcp`
may report `disconnected` with no error until something calls it. Step 3 is
the proof.

## Known failure modes

| Symptom | Cause | Fix |
| --- | --- | --- |
| `nakagai-mcp` fails with `401 Unauthorized for url .../mcp/` while the same token works on `/api/agent/*` | The connector URL lacks the trailing slash. The platform mounts at `/mcp/` and 307-redirects the bare path; behind a TLS-terminating proxy (Fly) the Location header says `http://`, and httpx drops the Authorization header on a redirect to an insecure origin. | The synced registry must carry `<platform>/mcp/` (trailing slash) so no redirect happens. Current releases write it that way, so upgrade the edge, force a registry rewrite (step 3) and restart `run`. Confirm the fix in `~/.nakagai/edge/config/connectors.yaml`: the `nakagai-mcp` url ends in a slash. |
| Registry edits never appear in `config/connectors.yaml` | Etag-gated sync skipped the rewrite | `rm ~/.nakagai/edge/cache/meta.json` then `sync` |
| Every connector call refused with "policy stale" | Edge cannot reach the platform and the policy TTL (15 min) expired | Restore connectivity, `sync`; this is fail-closed by design |
| `edge is not paired` from `run` | No `agent.json` | Step 2 |
| Platform calls 401 after working previously | Agent revoked on the web Agents page | Re-pair with a fresh code |
| `demo-broker` shows `error: Connection closed` on a local edge | Its registry command path (`/data/demo_broker_mcp.py`) exists only in the hosted container | Ignore locally, or disable the connector |
| Bare `HTTPStatusError` naming a URL without a trailing slash | A connector URL missing its `/`; httpx does not follow redirects by default in raw clients | Add the slash to the connector URL |

## Diagnostic technique for auth failures

Compare surfaces with the same token (read it from
`~/.nakagai/edge/agent.json`; never print it whole):

```bash
TOKEN=$(python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.nakagai/edge/agent.json')))['token'])")
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN" https://api.nakag.ai/api/agent/bundle   # 200 = token valid
curl -s -o /dev/null -w "%{http_code}\n" -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" https://api.nakag.ai/mcp/ -d '{}'   # 200/4xx-not-401 = gate passed
curl -s -o /dev/null -D - -X POST -H "Authorization: Bearer $TOKEN" https://api.nakag.ai/mcp | grep -i location   # http:// here = the redirect trap above
```

If `/api/agent/bundle` is 200 but the edge's `nakagai-mcp` connector 401s,
the token is fine and the problem is in how the request is built (redirects,
stripped headers), not in pairing.

## Related

- `https://app.nakag.ai/docs/edge` (custody model, write path, the two distinct
  logins). The docs viewer is behind a login, so it needs the owner's session.
- `https://app.nakag.ai/docs/agent-pairing` (token model, minting, revocation)
- Broker OAuth is separate from platform pairing: `uvx nakagai-edge login <id>`
  writes `secrets/tokens/<id>.json`; platform pairing never touches it.
