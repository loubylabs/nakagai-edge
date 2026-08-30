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
import math
import time
from pathlib import Path

import yaml

from nakagai_edge.capability import CapabilityError, first, read_partial, resolve

PORTFOLIO_INTERVAL_S = 300      # the timer loop's cadence
REFRESH_MIN_INTERVAL_S = 15     # a poke inside this window is not a sweep

log = logging.getLogger("nakagai.edge")

def _status(*statuses: str) -> str:
    """One conservative status for a connector or account read.

    An unreadable source dominates an unsupported one: both block autonomous
    entry, but unreadable says the edge attempted a read and cannot describe
    what the broker holds. Empty arrays are authoritative only under `ok`.
    """
    if "unreadable" in statuses:
        return "unreadable"
    if "unsupported" in statuses:
        return "unsupported"
    return "ok"


def _error(label: str, error: Exception | str) -> str:
    detail = str(error)
    return f"{label}: {detail}" if detail else label


def _finite(value, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or (positive and number <= 0):
        return None
    return number


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
    unknown = [a for a in listed if a.get("unknown") is True]
    readable = [a for a in listed if a.get("unknown") is not True]
    g = spec.guardrails.accounts
    if not g.allow and not g.read:
        return [(a, "full") for a in readable + unknown]
    by_number = {str(a.get("account", "")): a for a in readable}
    pairs = []
    for tier, numbers in (("full", g.allow), ("read", g.read)):
        for num in numbers:
            pairs.append((by_number.get(num, {"account": num}), tier))
    return pairs + [(a, "full") for a in unknown]


async def connector_snapshot(hub, spec) -> dict:
    """One connector's snapshot document. Failures degrade to the smallest
    honest unit: a dead account keeps its siblings, a dead account list keeps
    the other connectors (the caller assembles those).

    The map chooses which tools to dial and locates the fields the edge itself
    decides on. Nothing else is reshaped: this document's key names are the
    web's contract, so `account_number`, `nickname` and `type` keep their
    spelling here while their values now arrive through `list_accounts`.
    """
    observed_at = time.time()
    entry: dict = {"id": spec.id, "status": "ok", "observed_at": observed_at,
                   "error": "", "accounts": []}
    try:
        listed = await listed_accounts(hub, spec)
    except Exception as e:  # noqa: BLE001 (per-connector degradation, by design)
        # CapabilityError arrives here too. A connector that never declared
        # list_accounts says so in its own entry and contributes no accounts,
        # rather than taking the other brokers' sweep down with it.
        entry["error"] = f"{type(e).__name__}: {e}"
        entry["status"] = ("unsupported" if isinstance(e, CapabilityError)
                           else "unreadable")
        return entry
    for account, tier in tiered_accounts(spec, listed):
        num = str(account.get("account", ""))
        row = {"account_number": num, "status": "ok", "observed_at": observed_at,
               "nickname": account.get("nickname") or "",
               "type": account.get("type") or "",
               "tier": tier, "error": "", "portfolio": {}, "positions": [],
               "open_orders": []}
        if not num:
            # Listed by the broker, but with nothing readable to name it by.
            # The row is kept and not dialed: there is no identifier to ask
            # about, and inventing one would ask about somebody else's account.
            # Saying so here beats sending the broker a blank account and
            # relaying whatever it makes of that.
            if account.get("unknown") is True:
                row["unknown"] = True
                row["error"] = "list_accounts unreadable"
            else:
                row["error"] = ("this connector listed an account with no readable "
                                "identifier, so its balances and positions cannot "
                                "be fetched")
            row["status"] = "unreadable"
            entry["accounts"].append(row)
            continue
        errors = []
        if "get_balance" not in spec.capabilities:
            row["status"] = _status(row["status"], "unsupported")
            errors.append("get_balance unsupported")
        else:
            try:
                row["portfolio"] = await _figures(hub, spec, num)
            except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
                row["status"] = _status(row["status"], "unreadable")
                errors.append(_error("get_balance unreadable", e))
        position_status, positions, position_error = await position_rows(hub, spec, num)
        order_status, orders, order_error = await _open_orders(hub, spec, num)
        row["positions"], row["open_orders"] = positions, orders
        row["status"] = _status(row["status"], position_status, order_status)
        errors.extend(e for e in (position_error, order_error) if e)
        row["error"] = "; ".join(errors)
        entry["accounts"].append(row)
    entry["status"] = _status(*(a["status"] for a in entry["accounts"]))
    return entry


def account_snapshot_readable(
        doc: dict, connector_id: str, account: str) -> bool:
    """Whether one freshly assembled portfolio account is authoritative."""
    for connector in doc.get("connectors") or []:
        if connector.get("id") != connector_id or connector.get("status") != "ok":
            continue
        for row in connector.get("accounts") or []:
            if (str(row.get("account_number") or "") == account
                    and row.get("status") == "ok"):
                return True
    return False


async def listed_accounts(hub, spec) -> list[dict]:
    """The broker's accounts, normalized one row at a time.

    Public because the fill journal sweeps the same accounts this does, and two
    readers of `list_accounts` that could disagree about which accounts exist is
    a way for one sweep to silently cover a set the other does not.

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
    payload = (await hub.call(
        spec.id, tool, args, account_key=hub.account_key
    )).get("data")
    rows = first(payload, cap.items) if cap.items else payload
    if not isinstance(rows, list):
        return [{"unknown": True}]
    out = []
    for row in rows:
        if not isinstance(row, dict):
            out.append({"unknown": True})
            continue
        out.append(read_partial("list_accounts", cap, row))
    return out


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
    payload = (await hub.call(
        spec.id, tool, args, account_key=hub.account_key
    )).get("data")
    figures = first(payload, cap.items) if cap.items else payload
    if not isinstance(figures, dict):
        raise ValueError("get_balance response is not the mapped object")
    return figures


async def balance_evidence(hub, spec, account: str) -> dict:
    """Canonical broker equity and signed day P&L for the existing check-in.

    This helper is deliberately separate from the display snapshot. Task 3
    calls it immediately before candidate acceptance and relays its two values
    through `agent_checkin`, preserving that endpoint as the platform's one
    equity authority. `ok` requires finite positive equity and finite signed
    day P&L. A connector that does not declare get_balance is unsupported; a
    missing map field or unusable broker value is unreadable.
    """
    try:
        cap = spec.capability("get_balance")
    except CapabilityError:
        return {"status": "unsupported", "equity": None, "day_pnl": None}
    try:
        tool, args = resolve("get_balance", cap, {"account": account})
        payload = (await hub.call(
            spec.id, tool, args, account_key=hub.account_key
        )).get("data")
        figures = first(payload, cap.items) if cap.items else payload
    except Exception:  # noqa: BLE001 (the caller needs a bounded readiness fact)
        return {"status": "unreadable", "equity": None, "day_pnl": None}
    if not isinstance(figures, dict):
        return {"status": "unreadable", "equity": None, "day_pnl": None}
    values = read_partial("get_balance", cap, figures)
    equity = _finite(values.get("equity"), positive=True)
    day_pnl = _finite(values.get("day_pnl"))
    if equity is None or day_pnl is None:
        return {"status": "unreadable", "equity": None, "day_pnl": None}
    return {"status": "ok", "equity": equity, "day_pnl": day_pnl}


async def position_rows(hub, spec, account: str) -> tuple[str, list, str]:
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
    try:
        cap = spec.capability("list_positions")
    except CapabilityError as e:
        return "unsupported", [], _error("list_positions unsupported", e)
    try:
        tool, args = resolve("list_positions", cap, {"account": account})
        payload = (await hub.call(
            spec.id, tool, args, account_key=hub.account_key
        )).get("data")
    except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
        return "unreadable", [{"unknown": True}], _error("list_positions unreadable", e)
    rows = first(payload, cap.items) if cap.items else payload
    if not isinstance(rows, list):
        return "unreadable", [{"unknown": True}], "list_positions unreadable"
    out, unknown = [], False
    required = ("symbol", "quantity")
    for row in rows:
        if not isinstance(row, dict):
            out.append({"unknown": True})
            unknown = True
            continue
        normalized = read_partial("list_positions", cap, row)
        unreadable = any(field not in normalized for field in required)
        out.append({**row, **normalized, **({"unknown": True} if unreadable else {})})
        unknown = unknown or unreadable
    return ("unreadable" if unknown else "ok", out,
            "list_positions unreadable" if unknown else "")


async def _open_orders(hub, spec, account: str) -> tuple[str, list, str]:
    """Open-order evidence through fills.py's sole connector parser."""
    # fills.py imports broker discovery from this module, so this stays local
    # rather than turning two read-only helpers into an import cycle.
    from nakagai_edge.edge.fills import open_order_rows
    try:
        spec.capability("list_orders")
    except CapabilityError as e:
        return "unsupported", [], _error("list_orders unsupported", e)
    try:
        rows = await open_order_rows(hub, spec, account)
    except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
        return "unreadable", [{"unknown": True}], _error("list_orders unreadable", e)
    if rows is None:
        return "unreadable", [{"unknown": True}], "list_orders unreadable"
    if any(row.get("unknown") is True for row in rows):
        return "unreadable", rows, "list_orders unreadable"
    return "ok", rows, ""


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

    async def snapshot_and_push(self, *, force: bool = False,
                                require_ack: bool = False) -> dict:
        # Imported here, not at module scope: brake.py is the one place the
        # disarm switches actually live, and snapshot_and_push is the one
        # portfolio caller close enough to the loop to read them fresh each
        # sweep rather than trusting a value handed in once at construction.
        from nakagai_edge.edge.brake import armed, disarmed_positions
        async with self._lock:
            now = time.time()
            if (not force and self._last_doc is not None
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
                acknowledged = await asyncio.to_thread(
                    self._client.report_portfolio, doc["connectors"])
                if require_ack and acknowledged.get("ok") is not True:
                    raise ValueError("platform did not acknowledge fresh portfolio evidence")
            except Exception as e:  # noqa: BLE001 (a down platform must not hurt the edge)
                if require_ack:
                    raise
                log.warning("portfolio snapshot not reported to the platform "
                            "this cycle: %s", e)
            return doc
