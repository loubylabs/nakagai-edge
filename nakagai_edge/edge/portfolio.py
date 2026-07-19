"""Portfolio snapshot: the edge fetches figures with its OWN broker calls and
pushes them to the platform as display state (POST /api/agent/portfolio).

Three triggers share one code path here: the timer loop in runtime.py, the
refresh_portfolio MCP tool, and the executor after a completed write. None of
them carries data. An agent poke is a request that the edge go look for
itself; the numbers never originate anywhere but the broker responses this
module collects, through the hub and therefore through guardrails.

Money stays strings, verbatim from the broker: the edge relays figures, it
never does arithmetic on them.
"""

import asyncio
import logging
import time
from pathlib import Path

import yaml

PORTFOLIO_INTERVAL_S = 300      # the timer loop's cadence
REFRESH_MIN_INTERVAL_S = 15     # a poke inside this window is not a sweep

log = logging.getLogger("nakagai.edge")


def _unwrap(out: dict):
    """hub.call returns {"data": <downstream result>}; Robinhood nests its own
    {"data": ..., "guide": ...} envelope inside that. Peel both, drop the
    guide: figures only."""
    payload = out.get("data")
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    return payload


def broker_specs(root) -> list:
    """Enabled MCP brokers from the synced registry, in file order."""
    from nakagai_edge.config import load_specs
    path = Path(root) / "config" / "connectors.yaml"
    if not path.exists():
        return []
    specs = load_specs(yaml.safe_load(path.read_text()) or {})
    return [s for s in specs.values()
            if s.enabled and s.role == "broker" and s.is_mcp]


def tiered_accounts(spec, listed: list[dict]) -> list[tuple[dict, str]]:
    """(account, tier) pairs the snapshot should fetch.

    No account lists configured means no restriction (check_accounts
    semantics), so every listed account is fetched at the full tier. With
    lists, the union of both tiers is fetched, each under its own label; a
    configured account the broker did not list is still tried, so its refusal
    surfaces as that account's error instead of a silent hole.
    """
    g = spec.guardrails.accounts
    if not g.allow and not g.read:
        return [(a, "full") for a in listed]
    by_number = {str(a.get("account_number", "")): a for a in listed}
    pairs = []
    for tier, numbers in (("full", g.allow), ("read", g.read)):
        for num in numbers:
            pairs.append((by_number.get(num, {"account_number": num}), tier))
    return pairs


async def connector_snapshot(hub, spec) -> dict:
    """One connector's snapshot document. Failures degrade to the smallest
    honest unit: a dead account keeps its siblings, a dead get_accounts keeps
    the other connectors (the caller assembles those)."""
    entry: dict = {"id": spec.id, "error": "", "accounts": []}
    try:
        listed = _unwrap(await hub.call(spec.id, "get_accounts", {}))
        accounts = listed.get("accounts", []) if isinstance(listed, dict) else []
    except Exception as e:  # noqa: BLE001 (per-connector degradation, by design)
        entry["error"] = f"{type(e).__name__}: {e}"
        return entry
    for account, tier in tiered_accounts(spec, accounts):
        num = str(account.get("account_number", ""))
        row = {"account_number": num,
               "nickname": account.get("nickname") or "",
               "type": account.get("type") or "",
               "tier": tier, "error": "", "portfolio": {}, "positions": []}
        try:
            args = {"account_number": num}
            figures = _unwrap(await hub.call(spec.id, "get_portfolio", args))
            row["portfolio"] = figures if isinstance(figures, dict) else {}
            held = _unwrap(await hub.call(spec.id, "get_equity_positions", args))
            if isinstance(held, dict):
                held = held.get("positions") or held.get("results") or []
            row["positions"] = held if isinstance(held, list) else []
        except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
            row["error"] = f"{type(e).__name__}: {e}"
        entry["accounts"].append(row)
    return entry


class PortfolioReporter:
    """One code path for all three triggers, rate-limited so a confused agent
    cannot convert "poke" into "hammer Robinhood": inside the window the
    fresh-enough snapshot comes back without a broker sweep."""

    def __init__(self, state, hub, client):
        self._state, self._hub, self._client = state, hub, client
        self._last_run = 0.0
        self._last_doc: dict | None = None
        self._lock = asyncio.Lock()

    async def snapshot_and_push(self) -> dict:
        async with self._lock:
            now = time.time()
            if (self._last_doc is not None
                    and now - self._last_run < REFRESH_MIN_INTERVAL_S):
                return self._last_doc
            doc = {"connectors": [await connector_snapshot(self._hub, s)
                                  for s in broker_specs(self._state.root)]}
            self._last_run = time.time()
            self._last_doc = doc
            try:
                await asyncio.to_thread(
                    self._client.report_portfolio, doc["connectors"])
            except Exception as e:  # noqa: BLE001 (a down platform must not hurt the edge)
                log.warning("portfolio snapshot not reported to the platform "
                            "this cycle: %s", e)
            return doc
