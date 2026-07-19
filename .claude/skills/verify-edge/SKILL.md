---
name: verify-edge
description: Run the edge health ladder: prove the local edge, its pairing, every connector, and the platform relay (check-in, portfolio, audit, events) are green, with an opt-in write-path drill through approvals. Use when asked whether the edge is healthy, connected, relaying, or "everything green", or after connect-edge / pair-agent to confirm the result.
---

# Verify the edge, end to end

A ladder of checks from the local process outward. Run them in order and
report a green/red table at the end; each rung names the skill or fix that
repairs it. Every rung is read-only except the last, which is opt-in and
needs a human.

Roots and endpoints assumed: edge state in `~/.nakagai/edge/`, edge MCP at
`http://127.0.0.1:8330/mcp/`, hosted platform at `https://api.nakag.ai`.
Substitute for self-hosted setups.

## Rung 1: process

```bash
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8330/mcp/
```

`307` = up (that redirect is the healthy answer for a bare GET). Connection
refused = not running: `connect-edge` step 3.

## Rung 2: pairing and policy

```bash
uv run nakagai-edge status
```

Expect `paired: true`, the intended `platform_url`, and `policy_fresh:
true`. Stale policy with the platform reachable means the sync loop is not
running (restart `run`); stale with the platform unreachable is the
fail-closed design doing its job.

## Rung 3: connectors

Over the edge MCP, call `get_connector_status`. Judge each entry by its
kind:

- `mcp-http` / `mcp-stdio` connectors should be `connected` with a nonzero
  `tool_count` AFTER first use; they dial lazily, so `disconnected` with no
  `last_error` just means untouched. Force the dial with
  `list_connector_tools(<id>)` before judging it red.
- `data` / `view` kinds (alpaca-data, yfinance-data, tradingview-view) hold
  no session; `disconnected` is their permanent, healthy state.
- `demo-broker` erroring with a `/data/...` path on a LOCAL edge is a known
  hosted-container fixture quirk, not a failure.

Red flags worth acting on: `status: error` with a `last_error` on a real
connector, and any 401 from `nakagai-mcp` (see `connect-edge`'s
failure-mode table; usually the trailing-slash redirect trap or a revoked
token).

## Rung 4: platform relay (the "is anything actually landing?" rung)

These prove data flows OUT to the platform, not just that auth works:

1. `agent_checkin(status="idle", note="verify-edge health check")`: expect
   the current mandate back, no error. This also stamps the owner's
   activity feed, which the owner can eyeball as receipt.
2. `list_connector_tools("nakagai-mcp")`: expect the platform tool list
   with policy verdicts. This is the full edge-to-platform MCP proxy path.
3. `refresh_portfolio()`: expect a portfolio document; it was also pushed
   to the owner's Portfolio page. A rate-limit echo (same snapshot within
   15s) still counts as green.
4. `await_events(timeout_s=5, cursor=-1)`: an empty batch is green; the
   point is that the long-poll channel answers rather than errors.

Direct raw checks when MCP tools are unavailable (token from
`~/.nakagai/edge/agent.json`; never print it whole):

```bash
TOKEN=$(python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.nakagai/edge/agent.json')))['token'])")
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $TOKEN" https://api.nakag.ai/api/agent/bundle   # 200
```

## Rung 5 (opt-in, needs a human): write path through approvals

Only with the owner's explicit go-ahead, because it creates a real approval
record and, on a real broker, a real order. Prefer the demo broker where
available.

1. Stage: `call_connector` on a broker with a small `place_*` order; expect
   `approval_required: true` and an `approval_id` (or `auto_approved: true`
   under an armed autopilot mandate).
2. Human approves on the platform's Approvals page.
3. Poll `get_approval(approval_id)` until status leaves `pending`; expect
   `executed` with a result, or a broker-side `error` message (which still
   proves the relay: the signed grant reached the edge and the edge dialed
   the broker).
4. If the record instead sits `pending` forever, the platform never issued
   a decision the edge could see: check the executor loop is running (it
   polls every ~5s) and the approver setup on the platform side.

Never resubmit an approval that returned `error` with
`outcome_unknown: true`; the broker may already hold the order.

## Report format

End with a table: one row per rung (and per connector in rung 3), columns
`check | result | evidence | fix`. Evidence is the actual observed value
(status code, tool count, mandate phase), not "ok". Anything amber (lazy
connector never dialed, rung 5 skipped) is listed as NOT VERIFIED rather
than green; unverified and green must never look the same.

## Related

- `connect-edge` (repair anything red in rungs 1-3)
- `pair-agent` (credential problems, revocation, re-pairing)
- `docs/public/edge.md` (write path and failure-mode reference)
