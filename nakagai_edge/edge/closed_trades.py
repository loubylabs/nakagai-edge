"""Closed trades: the broker's own record of each closing trade, with the
realized profit or loss it computed, recorded locally and shipped.

Pairing buys with sells here would be wrong in ways the broker is not: a short
covered by a plain buy, lots opened before any stored history, snapshots that
lag fills. So the edge never pairs anything. It relays what the broker says it
realized, one row per closing trade, through the connector's
`list_closed_trades` map.

**The privacy rule is a moment.** Each (connector, account) records its
`first_sight` in `cache/closed-trades-from.json`: the moment it first
answered. With no history window granted, only trades that closed at or after
first sight are sent. A window the owner grants on the platform (the bundle's
`trade_history`, the same grant the fill journal reads) reaches back
`days` from now, and is applied as it stands on each sweep: lowering or
turning it off narrows the next sweep, and a granted window rolls forward
with time. Nothing before the earlier of the two is ever sent. A failed read
creates nothing and records no larger grant.

**A row ships only when its own `closed_at` proves it falls inside**, whatever
span the broker was asked for, so a broker that ignores the span cannot send
older rows. A row with no readable zoned instant is dropped.

**Identity is not content.** The broker gives these rows no id. `trade_key`
hashes what identifies a trade (connector, account, the close instant
normalized to UTC seconds, symbol, side, quantity) plus its ordinal among rows
sharing that identity in one read, so two trades of the same size closed in
the same second stay two. Price and realized profit are content: a broker
that revises them, or re-spells the timestamp, ships the same key again with
the new content, and the platform overwrites.
"""

import asyncio
import hashlib
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from nakagai_edge.capability import (SPANS, CapabilityError, first, pages,
                                     read_row)
from nakagai_edge.edge.fills import (SHIP_LIMIT, at_or_after, fetcher,
                                     parse_instant, ship)
from nakagai_edge.edge.journal import Journal
from nakagai_edge.edge.portfolio import (broker_specs, listed_accounts,
                                         tiered_accounts)
from nakagai_edge.edge.sync import trade_history_days

CAPABILITY = "list_closed_trades"
PAGE_LIMIT = 25                 # pages one account's read may follow
SPAN_SLACK_DAYS = 2             # a span must reach this much past the window
FROM_RELPATH = Path("cache") / "closed-trades-from.json"
JOURNAL_RELPATH = Path("results") / "closed-trades.jsonl"
ROW_FIELDS = ("closed_at", "symbol", "side", "quantity", "price", "realized_pnl")

log = logging.getLogger("nakagai.edge")


def utc_second(stamp: str) -> str:
    """A zoned instant as UTC at second precision, `2026-11-25T15:04:05Z`.
    Raises ValueError for a stamp parse_instant cannot read; such rows are
    dropped before they reach here."""
    moment = parse_instant(stamp)
    if moment is None:
        raise ValueError(f"not a zoned instant: {stamp!r}")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def trade_key(row: dict, ordinal: int = 0) -> str:
    """One closed trade's identity: sha256 over canonical JSON of connector_id,
    account, closed_at normalized to UTC seconds, symbol, side and quantity,
    plus `ordinal` when above 0.

    The broker gives no id, and its timestamps stop at the second, so the same
    size closed twice in one second arrives as two rows with one identity.
    `ordinal` is n for the n-th such row in one read, in the broker's order;
    the broker returns both on every read, so the numbering is stable.
    """
    doc = {"connector_id": row.get("connector_id"), "account": row.get("account"),
           "closed_at": utc_second(row.get("closed_at")),
           "symbol": row.get("symbol"), "side": row.get("side"),
           "quantity": row.get("quantity")}
    if ordinal > 0:
        doc["ordinal"] = ordinal
    return hashlib.sha256(json.dumps(doc, sort_keys=True, separators=(",", ":"))
                          .encode()).hexdigest()


def content(row: dict) -> str:
    """What may change under one identity: price, realized profit or loss, and
    the broker's own spelling of the close time."""
    return json.dumps([row.get("price"), row.get("realized_pnl"),
                       row.get("closed_at")], separators=(",", ":"))


class SendFrom:
    """Per (connector, account): `first_sight`, when it first answered, and
    `days`, the grant last applied by a complete read."""

    def __init__(self, root) -> None:
        self.path = Path(root) / FROM_RELPATH

    def _load(self) -> dict:
        try:
            doc = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return doc if isinstance(doc, dict) else {}

    def get(self, account_key: str) -> dict | None:
        entry = self._load().get(account_key)
        if not isinstance(entry, dict):
            return None
        seen, days = entry.get("first_sight"), entry.get("days")
        if isinstance(seen, bool) or not isinstance(seen, (int, float)):
            return None
        if isinstance(days, bool) or not isinstance(days, int) or days < 0:
            days = 0
        return {"first_sight": float(seen), "days": days}

    def set(self, account_key: str, first_sight: float, days: int) -> None:
        doc = self._load()
        doc[account_key] = {"first_sight": first_sight, "days": days}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        tmp.replace(self.path)


def send_from(first_sight: float, grant: int, now: float) -> float:
    """The instant before which nothing is sent, under the grant as it stands
    now: first sight with no grant, else the earlier of first sight and
    `now - grant` days."""
    if grant <= 0:
        return first_sight
    return min(first_sight, now - grant * 86400)


def choose_span(cap, since: float, now: float) -> str | None:
    """The narrowest canonical span reaching SPAN_SLACK_DAYS past `since` that
    the map can translate, widening when it cannot. None when it translates
    none of them. A map that maps no span argument is asked for its default
    (""). The slack keeps a window that just fits from losing its oldest day
    to a broker counting the span a little short."""
    if "span" not in cap.args:
        return ""
    words = cap.values.get("span") or {}
    back = (now - since) / 86400
    covering = [word for word, reach in SPANS
                if reach is None or back + SPAN_SLACK_DAYS <= reach]
    return next((word for word in covering if words.get(word)), None)


class ClosedTradesReporter:
    """One sweep: read each account's closed trades, journal what is new or
    revised inside its window, ship what is unshipped. Never raises."""

    def __init__(self, state, hub, client) -> None:
        self._state, self._hub, self._client = state, hub, client
        self._journal = Journal(Path(state.root) / JOURNAL_RELPATH)
        self._from = SendFrom(state.root)
        self._lock = asyncio.Lock()
        # trade_key -> content last journaled; the last line per key wins.
        self._seen = {r.get("trade_key"): content(r)
                      for r in self._journal.records()}

    def _specs(self) -> list:
        """Enabled brokers that declare closed trades; the rest, in silence."""
        out = []
        for spec in broker_specs(self._state.root):
            try:
                spec.capability(CAPABILITY)
            except CapabilityError:
                continue
            out.append(spec)
        return out

    async def sweep(self) -> list[dict]:
        """Journal whatever is new or revised. Returns the rows journaled."""
        async with self._lock:
            journaled: list[dict] = []
            for spec in self._specs():
                try:
                    listed = await listed_accounts(self._hub, spec)
                except Exception as e:  # noqa: BLE001 (per-connector degradation)
                    log.warning("closed trades unreadable for %s this cycle: %s",
                                spec.id, e)
                    continue
                for account, _tier in tiered_accounts(spec, listed):
                    number = str(account.get("account", ""))
                    if not number:
                        continue
                    try:
                        journaled.extend(await self._account(spec, number))
                    except Exception as e:  # noqa: BLE001 (per-account degradation)
                        log.warning("closed trades unreadable for %s account "
                                    "%s this cycle: %s", spec.id, number, e)
            await ship(self._journal, self._client.report_closed_trades,
                       SHIP_LIMIT, "closed trades")
            return journaled

    def _new(self, row: dict) -> bool:
        return self._seen.get(row["trade_key"]) != content(row)

    async def _account(self, spec, account: str) -> list[dict]:
        account_key = f"{spec.id}\x1f{account}"
        now = time.time()
        entry = self._from.get(account_key)
        grant = trade_history_days(self._state, spec.id)
        first_sight = entry["first_sight"] if entry else now
        stored = entry["days"] if entry else 0
        if entry and grant < stored:
            # A lowered or revoked grant narrows at once, read or no read.
            self._from.set(account_key, first_sight, grant)
            stored = grant
        # A window that grew this sweep must read past rows already journaled
        # to reach the older ones it just let in.
        grew = grant > stored
        bound = datetime.fromtimestamp(send_from(first_sight, grant, now),
                                       tz=timezone.utc)
        cap = spec.capability(CAPABILITY)
        span = choose_span(cap, bound.timestamp(), now)
        if span is None:
            log.warning("%s translates no span that reaches back far enough, "
                        "so its closed trades are not read", spec.id)
            return []

        def fresh(row: dict) -> bool:
            return at_or_after(row["closed_at"], bound) and (grew or self._new(row))

        # Each payload is read exactly once, in the broker's order, as it
        # arrives, so ordinals count across every page of this read and the
        # early stop judges the same keys that get journaled.
        read_pages: list[list[dict] | None] = []
        ordinals: Counter = Counter()
        tally = {"raw": 0, "unreadable": 0}
        fetch = fetcher(self._hub, spec, CAPABILITY)

        async def fetch_and_read(args: dict):
            payload = await fetch(args)
            read_pages.append(self._rows(spec, account, payload, ordinals, tally))
            return payload

        def enough(_payload) -> bool:
            rows = read_pages[-1]
            # A page that is not the list the map declares ends the read: the
            # whole read is refused below, so paging on would only cost calls.
            return rows is None or not any(fresh(r) for r in rows)

        args = {"account": account, "span": span or None}
        _payloads, truncated = await pages(cap, fetch_and_read, args,
                                           limit=PAGE_LIMIT, enough=enough)
        rows: list[dict] = []
        for read in read_pages:
            if read is None:
                log.warning("closed trades on %s account %s did not read as a "
                            "list, so none are sent this cycle", spec.id, account)
                return []
            rows.extend(read)
        if tally["raw"] and tally["unreadable"] == tally["raw"]:
            log.warning("all %d closed trades %s returned for account %s have no "
                        "readable zoned close time, so none are sent; check the "
                        "map's closed_at path", tally["raw"], spec.id, account)
        if truncated:
            log.warning("closed trades on %s account %s hold more than %d pages; "
                        "older closed trades are not sent",
                        spec.id, account, PAGE_LIMIT)
        if entry is None or grew:
            # Before journaling: a bound that cannot be recorded must not
            # ship rows that a later sweep would judge against another bound.
            self._from.set(account_key, first_sight, grant)
        journaled = []
        for row in rows:
            if not at_or_after(row["closed_at"], bound) or not self._new(row):
                continue
            self._seen[row["trade_key"]] = content(row)
            self._journal.append(row)       # the journal line is the wire row
            journaled.append(row)
        return journaled

    @staticmethod
    def _rows(spec, account: str, payload, ordinals: Counter,
              tally: dict) -> list[dict] | None:
        """One payload read into wire rows, or None when it is not the list
        the map declares. A row with no symbol or no readable zoned close time
        is dropped and counted: it cannot be placed against the window, and it
        cannot be identified. `ordinals` counts rows sharing one identity
        across one whole read."""
        cap = spec.capability(CAPABILITY)
        node = first(payload, cap.items) if cap.items else payload
        if not isinstance(node, list):
            return None
        out = []
        for raw in node:
            tally["raw"] += 1
            read = read_row(CAPABILITY, cap, raw)
            if read is None or parse_instant(read.get("closed_at")) is None:
                tally["unreadable"] += 1
                continue
            row = {"connector_id": spec.id, "account": account,
                   **{f: read.get(f) for f in ROW_FIELDS}}
            base = trade_key(row)
            row["trade_key"] = trade_key(row, ordinals[base])
            ordinals[base] += 1
            out.append(row)
        return out
