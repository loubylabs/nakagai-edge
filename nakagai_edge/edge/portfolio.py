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

from nakagai_edge.capability import first, read_partial, resolve

PORTFOLIO_INTERVAL_S = 300      # the timer loop's cadence
REFRESH_MIN_INTERVAL_S = 15     # a poke inside this window is not a sweep

log = logging.getLogger("nakagai.edge")


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

    `listed` has already been through the map, so its rows carry the canonical
    `account` field whatever the broker calls its own account number.

    No account lists configured means no restriction (check_accounts
    semantics), so every listed account is fetched at the full tier. With
    lists, the union of both tiers is fetched, each under its own label; a
    configured account the broker did not list is still tried, so its refusal
    surfaces as that account's error instead of a silent hole.
    """
    g = spec.guardrails.accounts
    if not g.allow and not g.read:
        return [(a, "full") for a in listed]
    by_number = {str(a.get("account", "")): a for a in listed}
    pairs = []
    for tier, numbers in (("full", g.allow), ("read", g.read)):
        for num in numbers:
            pairs.append((by_number.get(num, {"account": num}), tier))
    return pairs


async def connector_snapshot(hub, spec) -> dict:
    """One connector's snapshot document. Failures degrade to the smallest
    honest unit: a dead account keeps its siblings, a dead account list keeps
    the other connectors (the caller assembles those).

    The map chooses which tools to dial and locates the fields the edge itself
    decides on. Nothing else is reshaped: this document's key names are the
    web's contract, so `account_number`, `nickname` and `type` keep their
    spelling here while their values now arrive through `list_accounts`.
    """
    entry: dict = {"id": spec.id, "error": "", "accounts": []}
    try:
        listed = await _listed(hub, spec)
    except Exception as e:  # noqa: BLE001 (per-connector degradation, by design)
        # CapabilityError arrives here too. A connector that never declared
        # list_accounts says so in its own entry and contributes no accounts,
        # rather than taking the other brokers' sweep down with it.
        entry["error"] = f"{type(e).__name__}: {e}"
        return entry
    for account, tier in tiered_accounts(spec, listed):
        num = str(account.get("account", ""))
        row = {"account_number": num,
               "nickname": account.get("nickname") or "",
               "type": account.get("type") or "",
               "tier": tier, "error": "", "portfolio": {}, "positions": []}
        if not num:
            # Listed by the broker, but with nothing readable to name it by.
            # The row is kept and not dialed: there is no identifier to ask
            # about, and inventing one would ask about somebody else's account.
            # Saying so here beats sending the broker a blank account and
            # relaying whatever it makes of that.
            row["error"] = ("this connector listed an account with no readable "
                            "identifier, so its balances and positions cannot "
                            "be fetched")
            entry["accounts"].append(row)
            continue
        try:
            row["portfolio"] = await _figures(hub, spec, num)
            row["positions"] = await _positions(hub, spec, num)
        except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
            row["error"] = f"{type(e).__name__}: {e}"
        entry["accounts"].append(row)
    return entry


async def _listed(hub, spec) -> list[dict]:
    """The broker's accounts, normalized one row at a time.

    `read_partial`, not `extract`, for exactly the reason `_positions` uses it.
    `account` is a required field, so `extract` would DROP a row whose
    identifier will not read, and a dropped account does not come back as a
    visible hole: it takes its balances and every position underneath it out of
    the document, and no display says anything happened. Keeping the row with
    no `account` makes the same fault loud instead, which is the rule
    supervision.unreadable() applies to positions.
    """
    cap = spec.capability("list_accounts")
    tool, args = resolve("list_accounts", cap, {})
    payload = (await hub.call(spec.id, tool, args)).get("data")
    rows = first(payload, cap.items) if cap.items else payload
    if not isinstance(rows, list):
        return []
    return [read_partial("list_accounts", cap, row)
            for row in rows if isinstance(row, dict)]


async def _figures(hub, spec, account: str) -> dict:
    """The broker's own balance payload, verbatim.

    The map picks the tool and names the node the figures sit at; nothing is
    extracted. The web renders these keys directly today, so reshaping them
    here would break the Portfolio page a release before its own migration
    lands.

    `cap.items` is what replaced the hardcoded peel this module used to do.
    Robinhood nests a {"data": ..., "guide": ...} envelope inside hub.call's
    own, and the map now says where that sits instead of this module knowing
    the word "data". A broker that wraps nothing declares no items, and the
    whole payload is the figures.

    A payload missing the node the map names yields no figures at all, where
    the old hardcoded peel passed the outer payload through instead. Stricter
    on purpose: that payload is not the shape the map describes, and handing
    the web the rest of the envelope would put a `guide` string where the
    money goes. Blank tiles read as broken; wrong ones do not. The account row
    itself still reports, so nothing disappears over it.
    """
    cap = spec.capability("get_balance")
    tool, args = resolve("get_balance", cap, {"account": account})
    payload = (await hub.call(spec.id, tool, args)).get("data")
    figures = first(payload, cap.items) if cap.items else payload
    return figures if isinstance(figures, dict) else {}


async def _positions(hub, spec, account: str) -> list:
    """Raw position rows carrying normalized `symbol` and `quantity`.

    Both keys deliberately overwrite whatever the broker called them, because
    supervision and mark_guarded key off exactly these two and must not go on
    guessing. Every other field the broker sent survives for the web.

    Two things here are load-bearing and easy to get wrong:

    Rows are normalized ONE AT A TIME rather than by zipping the raw list
    against `extract`'s output. `extract` drops what it cannot read, so the two
    lists would fall out of alignment and attach one position's quantity to
    another position's symbol.

    `read_partial`, not `read_row`: a row whose quantity is unreadable KEEPS
    its symbol and stays in the list without a `quantity`. Dropping it would
    make the position look gone, which releases the brake and leaves something
    live without a stop. That is exactly the mistake supervision.unreadable()
    exists to prevent.
    """
    cap = spec.capability("list_positions")
    tool, args = resolve("list_positions", cap, {"account": account})
    payload = (await hub.call(spec.id, tool, args)).get("data")
    rows = first(payload, cap.items) if cap.items else payload
    if not isinstance(rows, list):
        return []
    return [{**row, **read_partial("list_positions", cap, row)}
            for row in rows if isinstance(row, dict)]


def mark_guarded(state, connectors: list[dict], *, brake_armed: bool = True,
                disarmed=frozenset(), now: float | None = None) -> list[dict]:
    """Tag each position with whether the brake is watching it.

    Display state only, exactly like every other figure in this document: no
    guardrail, approval, or authorization path may read it back. An unguarded
    position nobody can see is a silent failure, and this is what ends that.

    `brake_armed`/`disarmed` default permissive so a caller that has not been
    updated to pass the live disarm state behaves as it always did, rather
    than reporting every position unguarded. `now` is what warrant expiry is
    judged against, and it is read here rather than left to the caller so no
    display path can silently skip that check.
    """
    from nakagai_edge.edge.supervision import is_guarded, load
    now = time.time() if now is None else now
    guarded = {(r["connector_id"], r["account"], r["symbol"])
               for r in load(state).values()
               if is_guarded(r, brake_armed=brake_armed, disarmed=disarmed,
                             now=now)}
    for entry in connectors:
        for account in entry.get("accounts") or []:
            for row in account.get("positions") or []:
                if isinstance(row, dict):
                    key = (entry.get("id", ""),
                           str(account.get("account_number", "")),
                           str(row.get("symbol", "")).upper())
                    row["guarded"] = key in guarded
    return connectors


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
        # Imported here, not at module scope: brake.py is the one place the
        # disarm switches actually live, and snapshot_and_push is the one
        # portfolio caller close enough to the loop to read them fresh each
        # sweep rather than trusting a value handed in once at construction.
        from nakagai_edge.edge.brake import armed, disarmed_positions
        async with self._lock:
            now = time.time()
            if (self._last_doc is not None
                    and now - self._last_run < REFRESH_MIN_INTERVAL_S):
                return self._last_doc
            doc = {"connectors": mark_guarded(
                self._state, [await connector_snapshot(self._hub, s)
                              for s in broker_specs(self._state.root)],
                brake_armed=armed(self._state),
                disarmed=disarmed_positions(self._state))}
            self._last_run = time.time()
            self._last_doc = doc
            try:
                await asyncio.to_thread(
                    self._client.report_portfolio, doc["connectors"])
            except Exception as e:  # noqa: BLE001 (a down platform must not hurt the edge)
                log.warning("portfolio snapshot not reported to the platform "
                            "this cycle: %s", e)
            return doc
