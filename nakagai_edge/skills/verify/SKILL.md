---
name: verify
description: Launch and drive the nakagai platform locally to verify web/API changes end-to-end with an isolated scratch workspace.
---

# Verify Nakagai end to end

Use an isolated workspace and confirm ports 8321 and 3100 are available. Do not stop unrelated processes.

```bash
NAKAGAI_VERIFY_ROOT=$(mktemp -d)
NAKAGAI_ROOT="$NAKAGAI_VERIFY_ROOT" uv run nakagai workspace init --seed-config config
DATABASE_URL= NAKAGAI_API_TOKEN= NAKAGAI_MULTI_TENANT= NAKAGAI_ROOT="$NAKAGAI_VERIFY_ROOT" uv run nakagai-api
```

With `NAKAGAI_API_TOKEN` empty, this is local open mode and no bearer middleware is installed. Keep it bound to the local development environment.

In another terminal:

```bash
cd web
npm run dev
```

Check `http://127.0.0.1:8321/api/health` and `http://127.0.0.1:3100/login`. Browser routes require a real Supabase session. Create a disposable `verify-<date>@nakag.ai` identity through the approved test project and remove it after verification. Never use production identities or founder workspace state.

Verify the changed story through the UI and its matching API or MCP surface. For MCP, call `get_mandate` first. `run_backtest` and `sync_data` require explicit symbols. Bootstrap `get_signals` with `since="today"` on a new agent cursor.

Run the focused automated tests after the walkthrough. Stop only the processes started for this run and remove the scratch directory when finished.
