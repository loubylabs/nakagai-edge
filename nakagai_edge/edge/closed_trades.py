"""Closed trades: the broker's own record of each closing trade, with the
realized profit or loss it computed, recorded locally and shipped.

Pairing buys with sells here would be wrong in ways the broker is not: a short
covered by a plain buy, lots opened before any stored history, snapshots that
lag fills. So the edge never pairs anything. It relays what the broker says it
realized, one row per closing trade, through the connector's
`list_closed_trades` map.

**The privacy rule is a moment, not an anchor.** Each (connector, account)
keeps a "send from" instant in `cache/closed-trades-from.json`. First sight of
an account sets it to now, so nothing that closed before the edge connected is
sent. A history window the owner grants on the platform (the bundle's
`trade_history`, the same grant the fill journal reads) moves it back once per
larger grant, and it never moves forward. A failed read creates and moves
nothing.

**A row ships only when its own `closed_at` proves it falls at or after the
send-from instant**, whatever span the broker was asked for, so a broker that
ignores the span cannot send older rows. A row with no readable zoned instant
is dropped.

**The broker gives these rows no id.** `trade_key` is a digest of everything
the row says, so two byte-identical rows are one trade. That is the only
identity available, and it is what dedupes the journal and the platform.
"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from nakagai_edge.capability import (SPANS, CapabilityError, first, pages,
                                     read_row)
from nakagai_edge.edge.fills import SHIP_LIMIT, at_or_after, fetcher, ship
from nakagai_edge.edge.journal import Journal
from nakagai_edge.edge.portfolio import (broker_specs, listed_accounts,
                                         tiered_accounts)
from nakagai_edge.edge.sync import trade_history_days

CAPABILITY = "list_closed_trades"
PAGE_LIMIT = 25                 # pages one account's read may follow
FROM_RELPATH = Path("cache") / "closed-trades-from.json"
JOURNAL_RELPATH = Path("results") / "closed-trades.jsonl"
KEY_FIELDS = ("connector_id", "account", "closed_at", "symbol", "side",
              "quantity", "price", "realized_pnl")
ROW_FIELDS = KEY_FIELDS[2:]

log = logging.getLogger("nakagai.edge")


def trade_key(row: dict) -> str:
    """A stable identity for one closed trade: sha256 over canonical JSON of
    every value the row carries. The broker gives no id, so two byte-identical
    rows are one trade."""
    doc = json.dumps({f: row.get(f) for f in KEY_FIELDS}, sort_keys=True,
                     separators=(",", ":"))
    return hashlib.sha256(doc.encode()).hexdigest()


class SendFrom:
    """Per (connector, account): the instant before which nothing is sent, and
    the largest grant already applied to it."""

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
        sent_from, days = entry.get("from"), entry.get("days")
        if isinstance(sent_from, bool) or not isinstance(sent_from, (int, float)):
            return None
        if isinstance(days, bool) or not isinstance(days, int):
            days = 0
        return {"from": float(sent_from), "days": days}

    def set(self, account_key: str, sent_from: float, days: int) -> None:
        doc = self._load()
        doc[account_key] = {"from": sent_from, "days": days}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        tmp.replace(self.path)


def target(entry: dict | None, days: int, now: float) -> dict:
    """Where send-from should stand after this sweep, before the read proves
    it may move. First sight is now; a grant larger than the one applied moves
    it back to `now - days`; nothing moves it forward."""
    current = entry or {"from": now, "days": 0}
    if days > current["days"]:
        return {"from": min(current["from"], now - days * 86400), "days": days}
    return current


def choose_span(cap, send_from: float, now: float) -> str | None:
    """The narrowest canonical span covering `send_from` that the map can
    translate, widening when it cannot. None when it translates none of them.
    A map that maps no span argument at all is asked for its default ("")."""
    if "span" not in cap.args:
        return ""
    words = cap.values.get("span") or {}
    back = (now - send_from) / 86400
    covering = [word for word, reach in SPANS if reach is None or back <= reach]
    return next((word for word in covering if words.get(word)), None)


class ClosedTradesReporter:
    """One sweep: read each account's closed trades, journal what is new and
    inside its send-from instant, ship what is unshipped. Never raises."""

    def __init__(self, state, hub, client) -> None:
        self._state, self._hub, self._client = state, hub, client
        self._journal = Journal(Path(state.root) / JOURNAL_RELPATH)
        self._from = SendFrom(state.root)
        self._lock = asyncio.Lock()
        self._seen = {r.get("trade_key") for r in self._journal.records()}

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
        """Journal whatever is new. Returns the rows journaled this pass."""
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

    async def _account(self, spec, account: str) -> list[dict]:
        account_key = f"{spec.id}\x1f{account}"
        now = time.time()
        entry = self._from.get(account_key)
        goal = target(entry, trade_history_days(self._state, spec.id), now)
        # A send-from that moved back this sweep must read past rows already
        # journaled to reach the older ones it just let in.
        moved = entry is None or goal["from"] < entry["from"]
        bound = datetime.fromtimestamp(goal["from"], tz=timezone.utc)
        cap = spec.capability(CAPABILITY)
        span = choose_span(cap, goal["from"], now)
        if span is None:
            log.warning("%s translates no span that reaches back far enough, "
                        "so its closed trades are not read", spec.id)
            return []

        def fresh(row: dict) -> bool:
            return at_or_after(row["closed_at"], bound) and (
                moved or row["trade_key"] not in self._seen)

        def enough(payload) -> bool:
            rows = self._rows(spec, account, payload)
            return rows is not None and not any(fresh(r) for r in rows)

        args = {"account": account, "span": span or None}
        payloads, truncated = await pages(cap, fetcher(self._hub, spec, CAPABILITY),
                                          args, limit=PAGE_LIMIT, enough=enough)
        rows: list[dict] = []
        for payload in payloads:
            read = self._rows(spec, account, payload)
            if read is None:
                log.warning("closed trades on %s account %s did not read as a "
                            "list, so none are sent this cycle", spec.id, account)
                return []
            rows.extend(read)
        if truncated:
            log.warning("closed trades on %s account %s hold more than %d pages; "
                        "older closed trades are not sent",
                        spec.id, account, PAGE_LIMIT)
        journaled = []
        for row in rows:
            if not at_or_after(row["closed_at"], bound):
                continue
            if row["trade_key"] in self._seen:
                continue
            self._seen.add(row["trade_key"])
            self._journal.append(row)       # the journal line is the wire row
            journaled.append(row)
        if goal != entry:
            self._from.set(account_key, goal["from"], goal["days"])
        return journaled

    @staticmethod
    def _rows(spec, account: str, payload) -> list[dict] | None:
        """One payload read into wire rows, or None when it is not the list
        the map declares. Unreadable rows are dropped: with no instant they
        cannot be placed against send-from, and with no symbol they mean
        nothing."""
        cap = spec.capability(CAPABILITY)
        node = first(payload, cap.items) if cap.items else payload
        if not isinstance(node, list):
            return None
        out = []
        for raw in node:
            read = read_row(CAPABILITY, cap, raw)
            if read is None:
                continue
            row = {"connector_id": spec.id, "account": account,
                   **{f: read.get(f) for f in ROW_FIELDS}}
            row["trade_key"] = trade_key(row)
            out.append(row)
        return out
