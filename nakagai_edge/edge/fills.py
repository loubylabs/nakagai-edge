"""The fill journal: what the broker's order history says, recorded locally,
reconciled against submitted entries, and shipped to the platform.

This is the one path that declares an order filled. A submitted entry must
match its exact broker order id here before supervision starts. An order id no
approval claims remains an owner-placed trade when the rows reach the platform.

Three rules govern this module, and each of them inverts a rule that holds
elsewhere in the edge. They are worth reading before changing anything.

**First sight anchors; it does not backfill, except inside a window the owner
grants.** The first sweep of an account records the order ids it sees and
ships NOTHING. Only what appears afterwards is journaled. The sweep asks for
the broker's default page with no `since`, so without this it would upload
however much personal trading history the broker chooses to return. The anchor
is never evicted: losing it re-ships that history, and that is the single
outcome this design exists to prevent.

The one exception is a history window the owner turned on for that broker on
the platform (the bundle's `trade_history`). The edge then asks for orders
created since the window opened, through the map's `since`, and journals each
one once, anchored or not, then records the window in `fills-history.json` so
it is read once. Anything older than the window stays anchored and unsent. A
map with no `since` never backfills.

**Rows are read with `read_row`, not `read_partial`.** portfolio._positions
does the opposite, deliberately, because a position dropped for an unreadable
quantity looks CLOSED and releases the brake off a live position. A fill has no
such safety role. What it has instead is `order_id`, which is the only thing
that can dedupe it and the only thing that can join it to an approval, so a row
without one would re-ship on every sweep forever and could never be attributed.
Different job, opposite rule.

**The seen-set is keyed on (order_id, status), not order_id.** An order seen
working today and filled tomorrow has to ship twice, or the journal records it
queued forever and never learns it filled. This is also what lets the sweep run
with no status filter at all, which is what makes a broker whose word for
"filled" nobody has verified a non-problem rather than a guess.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from nakagai_edge.capability import (CapabilityError, first, pages,
                                     read_partial, read_row, resolve)
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.journal import Journal
from nakagai_edge.edge.portfolio import (broker_specs, listed_accounts,
                                         tiered_accounts)
from nakagai_edge.edge.remote import intents
from nakagai_edge.edge.sync import trade_history_days

FILLS_INTERVAL_S = 300          # the timer loop's cadence
SHIP_LIMIT = 200                # rows per POST
HISTORY_PAGES = 25              # pages one history backfill may read

ANCHOR_RELPATH = Path("cache") / "fills-anchor.json"
HISTORY_RELPATH = Path("cache") / "fills-history.json"
JOURNAL_RELPATH = Path("results") / "fills.jsonl"

log = logging.getLogger("nakagai.edge")


def filled_status(cap) -> str:
    """The broker's own word for a filled order, or "" when it declares none.

    Read off the map here rather than translated by capability._outbound, and
    the distinction matters. `_outbound` translates by canonical field, so
    teaching it `status` would also translate the AGENT-facing `list_orders`
    tool, whose docstring promises the opposite: a status passed there goes to
    the broker in its own spelling. An agent naming a state the connector had
    not enumerated would start getting a CapabilityError where it used to get
    an answer.

    The edge still knows no broker's vocabulary. The map holds the word; this
    reads it.
    """
    return next(iter((cap.values.get("status") or {}).get("filled") or []), "")


def key(row: dict) -> str:
    """One row's identity for the seen-set. See the module docstring for why
    `status` is part of it and not just `order_id`."""
    return f"{row.get('order_id', '')}\x1f{row.get('status', '')}"


class Anchor:
    """Order ids present the first time an account was ever swept.

    Written once per (connector, account) and never evicted, because eviction
    means re-shipping the owner's pre-Nakagai history. Kept separate from the
    journal so that clearing a shipped journal cannot silently un-anchor an
    account.
    """

    def __init__(self, root) -> None:
        self.path = Path(root) / ANCHOR_RELPATH

    def _load(self) -> dict:
        try:
            doc = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return doc if isinstance(doc, dict) else {}

    def known(self, account_key: str) -> set | None:
        """The anchored keys for an account, or None if it was never anchored.

        None and an empty set mean different things: never swept, against swept
        and the broker returned nothing. Only the first suppresses shipping.
        """
        doc = self._load()
        if account_key not in doc:
            return None
        return set(doc.get(account_key) or [])

    def anchor(self, account_key: str, keys) -> None:
        doc = self._load()
        if account_key in doc:
            return                    # first sight happens exactly once
        doc[account_key] = sorted(keys)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        tmp.replace(self.path)


class HistoryMarker:
    """The largest history window already sent, per (connector, account).

    Moved only after a backfill completes, so a failed or partial read is
    retried on the next sweep. A larger grant later reads again with the larger
    window; the seen-set keeps rows already journaled from journaling twice.
    """

    def __init__(self, root) -> None:
        self.path = Path(root) / HISTORY_RELPATH

    def _load(self) -> dict:
        try:
            doc = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return doc if isinstance(doc, dict) else {}

    def days(self, account_key: str) -> int:
        value = self._load().get(account_key)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    def move(self, account_key: str, days: int) -> None:
        doc = self._load()
        doc[account_key] = days
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        tmp.replace(self.path)


def _fetcher(hub, spec, cap):
    """One list_orders call, canonical arguments in, the payload out."""
    async def fetch(args: dict):
        tool, resolved = resolve("list_orders", cap, args)
        return (await hub.call(
            spec.id, tool, resolved, account_key=hub.account_key
        )).get("data")
    return fetch


async def _order_rows(hub, spec, account: str, *, status: str = "",
                      retain_unknown: bool = False) -> list[dict] | None:
    """One list_orders call and parse path for fills and exposure.

    `None` means the connector answered with something that was not the list
    its map declares. An empty list means the broker authoritatively answered
    that it has no matching orders. Exposure retains malformed rows as explicit
    unknown evidence; the fill journal still drops them because without an id
    it cannot deduplicate or attribute a historical fill.
    """
    cap = spec.capability("list_orders")
    args: dict = {"account": account}
    if status:
        args["status"] = status
    payload = await _fetcher(hub, spec, cap)(args)
    return _read_orders(cap, payload, retain_unknown=retain_unknown)


def _read_orders(cap, payload, *, retain_unknown: bool = False) -> list[dict] | None:
    """One list_orders payload read into canonical rows. See _order_rows."""
    rows = first(payload, cap.items) if cap.items else payload
    if not isinstance(rows, list):
        return None
    out = []
    for row in rows:
        if not isinstance(row, dict):
            if retain_unknown:
                out.append({"unknown": True})
            continue
        read = read_row("list_orders", cap, row)
        if read is not None:
            out.append(read)
            continue
        if retain_unknown:
            out.append({**read_partial("list_orders", cap, row), "unknown": True})
    return out


async def account_rows(hub, spec, account: str) -> list[dict]:
    """One account's filled order rows, canonical, unreadable ones dropped.

    `status` is only sent when the connector declares a word for it; a broker
    that declares none is asked for its default and journals whatever that is.
    """
    wanted = filled_status(spec.capability("list_orders"))
    return await _order_rows(hub, spec, account, status=wanted) or []


async def open_order_rows(hub, spec, account: str) -> list[dict] | None:
    """One account's broker-default order book, canonical and fail closed.

    The broker's default is intentionally requested without a status filter:
    the connector's own working-order semantics remain its authority. A
    malformed row is retained with `unknown: true` so it can block a later
    autonomous entry instead of vanishing into an apparently empty book.
    """
    return await _order_rows(hub, spec, account, retain_unknown=True)


def history_since(days: int, now: float) -> str:
    """The window's lower bound as an ISO 8601 UTC instant ending in `Z`."""
    start = datetime.fromtimestamp(now - days * 86400, tz=timezone.utc)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _before(filled_at, since: str) -> bool:
    """True only when `filled_at` provably predates the window.

    A second check behind the broker's own filter, which is on creation time:
    an order that filled before the window opened was created before it too.
    A timestamp this cannot read proves nothing and is left to the broker's
    filter, which is the authority here.
    """
    if not isinstance(filled_at, str):
        return False
    try:
        moment = datetime.fromisoformat(filled_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if moment.tzinfo is None:
        return False
    return moment < datetime.fromisoformat(since.replace("Z", "+00:00"))


async def history_rows(hub, spec, account: str, since: str) -> list[dict] | None:
    """Every order on one account created since `since`, any status.

    Follows the map's `next` for up to HISTORY_PAGES pages. None when any page
    is not the list the map declares: a partial history must not read as a
    complete one, or the marker would move past orders never read.
    """
    cap = spec.capability("list_orders")
    payloads = await pages(cap, _fetcher(hub, spec, cap),
                           {"account": account, "since": since},
                           limit=HISTORY_PAGES)
    if cap.next and len(payloads) == HISTORY_PAGES:
        log.warning("order history on %s account %s filled %d pages; older "
                    "orders inside the window are not sent",
                    spec.id, account, HISTORY_PAGES)
    out: list[dict] = []
    for payload in payloads:
        rows = _read_orders(cap, payload)
        if rows is None:
            return None
        out.extend(row for row in rows if not _before(row.get("filled_at"), since))
    return out


async def connector_accounts(hub, spec) -> tuple[str, list[tuple[str, list[dict]]]]:
    """(error, [(account, rows), ...]) for one connector.

    An account that ANSWERED is in the list even when it holds no orders, with
    an empty row list, and that distinction is the whole reason this returns
    accounts rather than one flat list of rows. A brand-new brokerage account
    is empty the first time it is swept. If "no rows" meant "no account", it
    would never anchor, and its first ever trade would arrive at an unanchored
    account and be recorded as history instead of being journaled: the owner's
    very first fill, silently swallowed.

    An account that FAILED is absent, because a failed read proves nothing
    about what the broker holds. Degrades per account exactly like the
    portfolio sweep: a dead account keeps its siblings, and a dead connector
    keeps the others, which the caller does.
    """
    try:
        listed = await listed_accounts(hub, spec)
    except Exception as e:  # noqa: BLE001 (per-connector degradation, by design)
        return f"{type(e).__name__}: {e}", []
    out = []
    for account, _tier in tiered_accounts(spec, listed):
        number = str(account.get("account", ""))
        if not number:
            continue                  # nothing to ask the broker about
        try:
            rows = await account_rows(hub, spec, number)
        except Exception as e:  # noqa: BLE001 (per-account degradation, by design)
            log.warning("order history unreadable for %s account %s, so its "
                        "fills are not journaled this cycle: %s",
                        spec.id, number, e)
            continue
        out.append((number, [{**row, "connector_id": spec.id, "account": number}
                             for row in rows]))
    return "", out


class FillsReporter:
    """One sweep: read order history, journal what is new, ship what is
    unshipped. Never raises; a broker or platform failure costs a cycle."""

    def __init__(self, state, hub, client) -> None:
        self._state, self._hub, self._client = state, hub, client
        self._journal = Journal(Path(state.root) / JOURNAL_RELPATH)
        self._anchor = Anchor(state.root)
        self._history = HistoryMarker(state.root)
        self._lock = asyncio.Lock()
        self._seen = {key(r) for r in self._journal.records()}

    def _specs(self) -> list:
        """Enabled brokers that can actually answer. A connector declaring no
        `list_orders` has no history to read and is skipped in silence: it is
        not misconfigured, it simply does not offer this."""
        out = []
        for spec in broker_specs(self._state.root):
            try:
                spec.capability("list_orders")
            except CapabilityError:
                continue
            out.append(spec)
        return out

    async def sweep(self) -> list[dict]:
        """Journal whatever is new. Returns the rows journaled this pass."""
        async with self._lock:
            journaled = []
            for spec in self._specs():
                error, accounts = await connector_accounts(self._hub, spec)
                if error:
                    # A failed read proves nothing about what the broker holds.
                    # Anchoring on it would anchor an empty set and ship the
                    # whole history next cycle; journaling on it would journal
                    # nothing while marking the account as swept.
                    log.warning("order history unreadable for %s, so its fills "
                                "are not journaled this cycle: %s", spec.id, error)
                    continue
                for account, rows in accounts:
                    from nakagai_edge.edge.executor import (
                        declared_terminal_order_values,
                        reconcile_submitted_fills,
                    )
                    await reconcile_submitted_fills(
                        self._hub, self._state, self._client,
                        EdgeAudit(self._state), spec, account, rows)
                    submitted = any(
                        isinstance(intent, dict)
                        and intent.get("phase") == "submitted"
                        and intent.get("connector_id") == spec.id
                        and intent.get("account") == account
                        for intent in intents(self._state).values()
                    )
                    if submitted:
                        for status in declared_terminal_order_values(spec):
                            try:
                                terminal_rows = await _order_rows(
                                    self._hub, spec, account, status=status)
                            except Exception as error:  # noqa: BLE001 (retry next sweep)
                                log.warning(
                                    "terminal order status %s unreadable for %s "
                                    "account %s: %s", status, spec.id, account, error)
                                continue
                            await reconcile_submitted_fills(
                                self._hub, self._state, self._client,
                                EdgeAudit(self._state), spec, account,
                                terminal_rows or [])
                            submitted = any(
                                isinstance(intent, dict)
                                and intent.get("phase") == "submitted"
                                and intent.get("connector_id") == spec.id
                                and intent.get("account") == account
                                for intent in intents(self._state).values()
                            )
                            if not submitted:
                                break
                    journaled.extend(self._absorb(spec.id, account, rows))
                    journaled.extend(await self._backfill(spec, account))
            await self._ship()
            return journaled

    async def _backfill(self, spec, account: str) -> list[dict]:
        """Send the history window the owner granted, once per window.

        The one exception to first-sight anchoring: inside a granted window
        every row is journaled, anchored or not. Nothing runs without a grant
        larger than the window already sent, or for a map with no `since`.
        """
        days = trade_history_days(self._state, spec.id)
        account_key = f"{spec.id}\x1f{account}"
        if days <= self._history.days(account_key):
            return []
        if "since" not in spec.capability("list_orders").args:
            return []
        try:
            rows = await history_rows(self._hub, spec, account,
                                      history_since(days, time.time()))
        except Exception as e:  # noqa: BLE001 (retry next sweep)
            log.warning("order history unreadable for %s account %s, so its "
                        "%d-day history is not sent this cycle: %s",
                        spec.id, account, days, e)
            return []
        if rows is None:
            log.warning("order history on %s account %s did not read as a "
                        "list, so its %d-day history is not sent this cycle",
                        spec.id, account, days)
            return []
        journaled = self._journal_new(
            [{**row, "connector_id": spec.id, "account": account} for row in rows])
        self._history.move(account_key, days)
        log.info("sent %d orders of %d-day history on %s account %s",
                 len(journaled), days, spec.id, account)
        return journaled

    def _journal_new(self, rows: list[dict], skip=frozenset()) -> list[dict]:
        """Journal each row not journaled before and not in `skip`."""
        journaled = []
        for row in rows:
            k = key(row)
            if k in self._seen or k in skip:
                continue
            self._seen.add(k)
            self._journal.append({**row, "reported_at": time.time()})
            journaled.append(row)
        return journaled

    def _absorb(self, connector_id: str, account: str,
                rows: list[dict]) -> list[dict]:
        """Anchor an account seen for the first time, journal the rest.

        Reached for every account that answered, including an empty one, which
        is what anchors a brand-new brokerage account before it has any orders
        rather than at its first trade. See connector_accounts.
        """
        account_key = f"{connector_id}\x1f{account}"
        anchored = self._anchor.known(account_key)
        if anchored is None:
            # The seen-set stays "journaled", not "seen": a history backfill
            # must still be able to journal an anchored row inside its window.
            self._anchor.anchor(account_key, (key(r) for r in rows))
            log.info("anchored %d existing orders on %s account %s; only what "
                     "happens from here, or inside a history window the owner "
                     "grants, is journaled", len(rows), connector_id, account)
            return []
        return self._journal_new(rows, skip=anchored)

    async def _ship(self) -> None:
        """Send unshipped rows in batches until none wait, marking LINES
        consumed rather than rows sent so an unreadable line cannot strand the
        watermark behind it forever. A history backfill can journal many
        batches at once; it drains in one sweep rather than one per cycle."""
        while batch := self._journal.pending(SHIP_LIMIT):
            rows = [row for row in batch if row is not None]
            try:
                if rows:
                    await asyncio.to_thread(self._client.report_fills, rows)
                self._journal.mark_shipped(len(batch))
            except Exception as e:  # noqa: BLE001 (a down platform must not hurt the edge)
                log.warning("fill journal not shipped to the platform this "
                            "cycle, so %d rows wait for the next: %s",
                            len(batch), e)
                return
