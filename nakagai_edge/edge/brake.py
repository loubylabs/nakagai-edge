"""The brake: the live execution of the exit half of a strategy.

Every out-of-sample number in the platform's evidence store was measured on a
strategy that EXITS. Live, nothing exits: the agent places an entry and goes to
sleep, the platform holds no broker credential, and the stop survives only as a
number in an order payload nobody reads again. This module is what reads it.

Authority comes from a pre-signed, reduce-only warrant rather than from cached
policy, which is why the brake fires on stale policy and through a dark
platform while every other path in the edge refuses. See
docs/superpowers/specs/2026-07-26-the-brake-design.md.

This half of the file is pure: no I/O, no clock of its own. Firing on a bad
print sells a good position at a fictional number, so the judgment about when a
price may be believed lives here where it can be tested exhaustively.
"""

import json
import logging

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import claim, load, mark
from nakagai_edge.edge.sync import cached_bundle, public_key
from nakagai_edge.warrant import authorizes, breached, exit_order_args

log = logging.getLogger("nakagai.edge")

QUOTE_MAX_AGE_S = 60.0        # four watcher ticks
MAX_SPREAD_PCT = 5.0          # of the mid
MAX_JUMP_PCT = 20.0           # from the prior observation
BREACH_CONFIRMATIONS = 2      # consecutive sane breaches before firing
BRAKE_INTERVAL_S = 15.0

_PRICE_KEYS = ("price", "last_trade_price", "last_price", "mark_price", "last")
_BID_KEYS = ("bid", "bid_price")
_ASK_KEYS = ("ask", "ask_price")


def _num(payload: dict, keys) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def normalize_quote(payload, received_at: float) -> dict | None:
    """A broker's quote payload as {price, bid, ask, ts}, or None.

    `received_at` is required, and it is the moment WE got this payload rather
    than any timestamp the broker supplied: brokers rarely stamp a quote, and a
    default here would hand `usable()` a value that silently disables its own
    freshness check.
    """
    if not isinstance(payload, dict):
        return None
    price = _num(payload, _PRICE_KEYS)
    if price is None:
        return None
    return {"price": price, "bid": _num(payload, _BID_KEYS),
            "ask": _num(payload, _ASK_KEYS), "ts": float(received_at)}


def usable(quote: dict, prior_price, now: float, *,
           max_age_s: float = QUOTE_MAX_AGE_S,
           max_spread_pct: float = MAX_SPREAD_PCT,
           max_jump_pct: float = MAX_JUMP_PCT) -> str:
    """Empty string when this observation may be believed, else why not.

    An observation that fails here is DISCARDED, not counted: it can neither
    trigger the brake nor reset a breach run already underway.
    """
    try:
        price = float(quote["price"])
        ts = float(quote.get("ts") or 0.0)
    except (TypeError, ValueError, KeyError):
        return "unreadable quote"
    if not price > 0:
        return "price is not positive"
    if not ts > 0:
        return "unstamped quote"
    if (now - ts) > max_age_s:
        return f"stale quote ({round(now - ts)}s old)"
    bid, ask = quote.get("bid"), quote.get("ask")
    if bid is not None and ask is not None:
        mid = (float(bid) + float(ask)) / 2
        if not mid > 0:
            return "book is unusable (non-positive mid)"
        if ((float(ask) - float(bid)) / mid) * 100 > max_spread_pct:
            return "spread is too wide to price an exit"
    # Only None means "no prior observation". A non-positive prior cannot come
    # from this function (it refuses such prices above), so if a caller ever
    # supplies one it means their state was never populated: treat it as absent
    # and say so here, because the alternative is a jump check that silently
    # never runs.
    if prior_price is not None and float(prior_price) > 0:
        move = abs(price - float(prior_price)) / float(prior_price) * 100
        if move > max_jump_pct:
            return f"implausible jump ({round(move)}%) from the prior quote"
    return ""


def advance(run: int, is_breach: bool) -> int:
    return run + 1 if is_breach else 0


def confirmed(run: int, needed: int = BREACH_CONFIRMATIONS) -> bool:
    return run >= needed


# ---- arming -------------------------------------------------------------
#
# Effective state is `platform_armed and not local_off`. The local half is a
# plain file the loop reads every tick, so `nakagai-edge brake off` works with
# no network at all: a brake you cannot release because the platform is down is
# not a safety feature.


def _disarm_doc(state: EdgeState) -> dict:
    if not state.brake_off_path.exists():
        return {}
    try:
        doc = json.loads(state.brake_off_path.read_text())
    except (json.JSONDecodeError, OSError):
        # Fail SAFE, not closed: an unreadable file means the owner's intent is
        # unknown, and firing on an unknown intent is worse than not firing.
        return {"all": True}
    return doc if isinstance(doc, dict) else {"all": True}


def armed(state: EdgeState) -> bool:
    if _disarm_doc(state).get("all"):
        return False
    mandate = (cached_bundle(state) or {}).get("mandate") or {}
    # Absent means armed: the brake ships on, and a platform that predates the
    # dial must not silently leave every position unguarded.
    return bool(mandate.get("brake_armed", True))


def disarmed_positions(state: EdgeState) -> set:
    return set(_disarm_doc(state).get("positions") or [])


def set_local_disarm(state: EdgeState, *, all_positions: bool = False,
                     position_id: str = "") -> None:
    doc = _disarm_doc(state)
    if all_positions:
        doc["all"] = True
    if position_id:
        doc["positions"] = sorted(set(doc.get("positions") or []) | {position_id})
    state.brake_off_path.parent.mkdir(parents=True, exist_ok=True)
    state.brake_off_path.write_text(json.dumps(doc, indent=2))


def clear_local_disarm(state: EdgeState) -> None:
    state.brake_off_path.unlink(missing_ok=True)


# ---- firing -------------------------------------------------------------

_POSITION_QTY_KEYS = ("quantity", "qty", "shares")


def _sent_value(args: dict, keys: list[str]):
    """The first of `keys` present in `args` with a scalar value, or None.

    None means the field is absent from the payload actually going to the
    broker. Verification must fail closed on that, not silently treat it as
    matching an unset warrant field.
    """
    for key in keys:
        if key in args:
            value = args[key]
            if value is not None and not isinstance(value, (dict, list)):
                return value
    return None


def _sent_qty(args: dict, keys: list[str]) -> float | None:
    """`_sent_value` coerced to a float, or None if absent/unparseable."""
    value = _sent_value(args, keys)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class Brake:
    """Places the exit a supervised position's warrant authorizes.

    Deliberately NOT gated on policy freshness or on the kill switch. Its
    authority came from a signed artifact that was fresh when issued, and
    firing can only reduce exposure. The guardrails still re-run inside
    hub.call, which is a second, independent check.
    """

    def __init__(self, state: EdgeState, hub, client, audit) -> None:
        self.state, self.hub, self.client, self.audit = state, hub, client, audit
        self._runs: dict = {}       # position_id -> consecutive breach count
        # position_id -> last believed price. A plain dict with .get() yields
        # None for a symbol never seen, which is exactly what usable() treats
        # as "no prior observation". Do NOT seed this with 0.0: usable()
        # treats a non-positive prior as absent BY DESIGN, so a zero default
        # would look like a real prior and silently disable jump detection
        # for every position, forever.
        self._prior: dict = {}

    def quote_symbols(self) -> list[str]:
        """Symbols worth a quote this tick: armed, warranted, not disarmed."""
        off = disarmed_positions(self.state)
        return sorted({r["symbol"] for r in load(self.state).values()
                       if r.get("state") == "armed" and r.get("warrant")
                       and r["position_id"] not in off})

    async def tick(self, quotes: dict, now: float) -> list[str]:
        """One evaluation pass. Returns the position ids it fired.

        `quotes` maps symbol to the FULL normalized quote `normalize_quote`
        produced (price, bid, ask, ts), never a bare price. usable() needs the
        real receipt time and the book to judge freshness and spread; an
        observation restated by its consumer -- re-stamped with this
        function's own `now`, stripped of its book -- is an observation whose
        provenance has been erased, and a reused or stale quote would then
        look fresh every time. This is the same mistake already found and
        fixed in Brake.fire(), where verification checked a restatement of
        the ledger instead of the payload actually being sent.
        """
        if not armed(self.state):
            return []
        off = disarmed_positions(self.state)
        fired = []
        for rec in list(load(self.state).values()):
            pid = rec["position_id"]
            if rec.get("state") != "armed" or not rec.get("warrant"):
                continue
            if pid in off:
                continue
            quote = quotes.get(rec["symbol"])
            if quote is None:
                continue
            why_not = usable(quote, self._prior.get(pid), now)
            if why_not:
                # Discarded, NOT counted: a bad print may neither fire the
                # brake nor reset a run already underway.
                log.warning("brake discarded a quote for %s: %s",
                            rec["symbol"], why_not)
                continue
            price = quote["price"]
            self._prior[pid] = price
            run = advance(self._runs.get(pid, 0),
                          breached((rec["warrant"] or {}).get("trigger") or {},
                                   price))
            self._runs[pid] = run
            if confirmed(run):
                self._runs[pid] = 0
                if await self.fire(rec) == "":
                    fired.append(pid)
        return fired

    async def _held_now(self, rec: dict) -> float | None:
        """What the broker says is held THIS INSTANT, or None if it will not say.

        The ledger's figure is up to one portfolio cadence old. Overselling a
        cash account is a real violation, not a rounding error, so the order is
        sized from a fresh read and never from the ledger. None must never be
        read as "no position": it means the answer could not be parsed, and
        the caller treats that as "do not fire blind", not as a release. 0.0
        is returned only when a genuinely readable position list simply does
        not contain the symbol.
        """
        try:
            out = await self.hub.call(rec["connector_id"], "get_equity_positions",
                                      {"account_number": rec["account"]})
        except Exception as e:  # noqa: BLE001
            log.warning("brake could not re-read %s before firing: %s",
                        rec["symbol"], e)
            return None
        payload = (out or {}).get("data")
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        if isinstance(payload, dict):
            payload = payload.get("positions") or payload.get("results") or []
        if not isinstance(payload, list):
            # No position list to search at all: an unreadable answer, not
            # evidence the symbol is gone.
            return None
        for row in payload:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol", "")).upper() != rec["symbol"]:
                continue
            for key in _POSITION_QTY_KEYS:
                if row.get(key) not in (None, ""):
                    try:
                        return float(row[key])
                    except (TypeError, ValueError):
                        return None
            # The row that names our symbol carries no key we recognize: we
            # found the position but cannot read its size, which is "I don't
            # know", not "it's gone".
            return None
        return 0.0

    def _notify(self, text: str) -> None:
        try:
            self.client.send_message(text)
        except Exception:  # noqa: BLE001 (never let a message failure matter)
            pass

    async def fire(self, rec: dict) -> str:
        """Place the exit. Returns "" on success, else why it did not fire."""
        pid = rec["position_id"]
        if rec.get("state") != "armed":
            return f"position {pid} is already {rec.get('state')}"

        held = await self._held_now(rec)
        if held is None:
            # Over-refusal here is real harm, not caution: the stop was just
            # touched and the brake is declining to act on it, so the owner
            # must hear that exactly as loudly as any other unguarded moment.
            why_not = "the broker would not confirm the position; not firing blind"
            self.audit.record("denial", rec["connector_id"], "brake",
                              {"position_id": pid, "reason": why_not})
            self._notify(f"The brake could not confirm {rec['symbol']} with the "
                         f"broker and did NOT fire. That position is unguarded "
                         f"until the next pass can read it.")
            return why_not
        if held <= 0:
            mark(self.state, pid, "released", confirmed_qty=0.0)
            return "the position is no longer held"

        spec = self.hub.spec(rec["connector_id"])
        warrant = rec.get("warrant") or {}
        qty = min(float(held), float(warrant.get("max_qty") or 0.0))
        args = exit_order_args(spec.guardrails.order_shape,
                               rec.get("entry_args") or {}, qty)
        if args is None:
            why_not = "this connector cannot express a market exit"
            self.audit.record("denial", rec["connector_id"], "brake",
                              {"position_id": pid, "reason": why_not})
            self._notify(f"The brake could not build an exit for {rec['symbol']}: "
                         f"{why_not}. That position is unguarded.")
            return why_not

        # Verify the order about to reach the broker, not a restatement of
        # the ledger's own assumptions. Reading the fields back out of `args`
        # through the connector's own declared keys is what proves the
        # warrant covers the bytes actually being sent; checking against
        # rec["symbol"] or warrant["side"] instead would let a bug in
        # exit_order_args sail straight through verification untouched.
        shape = spec.guardrails.order_shape
        sent = {
            "connector_id": rec["connector_id"],
            "tool": warrant.get("tool"),
            "symbol": _sent_value(args, shape.symbol_keys),
            "side": _sent_value(args, shape.side_keys),
            "qty": _sent_qty(args, shape.quantity_keys),
            "account": _sent_value(args, spec.guardrails.accounts.arg_names),
        }

        agent = self.state.agent() or {}
        why_not = authorizes(
            public_key(self.state), warrant, sent,
            agent_id=agent.get("agent_id"), held_qty=held, spent=False)
        if why_not:
            self.audit.record("denial", rec["connector_id"], "brake",
                              {"position_id": pid, "reason": why_not})
            self._notify(f"The brake could not exit {rec['symbol']}: {why_not}. "
                         f"That position is unguarded.")
            return why_not

        # One shot: spend the warrant BEFORE the call, so a crash mid-flight
        # can never produce a second exit on restart. A compare-and-swap, not
        # a plain mark(): two concurrent passes can both load this record as
        # `armed` and both pass authorizes() above (spent=False either way),
        # so only an atomic claim on the state transition itself stops a
        # second live order. After verification, not before it: claiming
        # first would strand the record in `firing` whenever verification
        # then failed.
        if not claim(self.state, pid, expected="armed", new="firing"):
            return f"position {pid} was already claimed by another pass"
        try:
            result = await self.hub.call(rec["connector_id"], warrant["tool"],
                                         args, approved=True)
        except Exception as e:  # noqa: BLE001 (the record must reflect reality)
            denied = type(e).__name__ == "GuardrailDenied"
            self.audit.record("error", rec["connector_id"], "brake",
                              {"position_id": pid, "error": str(e)})
            if denied:
                # Never left the machine, so nothing happened and this is safe
                # to retry once the owner fixes the configuration.
                mark(self.state, pid, "armed")
                self._notify(f"The brake was refused on {rec['symbol']}: {e}. "
                             f"Nothing was placed and the position is unguarded.")
            else:
                mark(self.state, pid, "outcome_unknown", error=str(e))
                self._notify(
                    f"The brake tried to exit {rec['symbol']} and the outcome is "
                    f"UNKNOWN: {e}. Check the broker directly; it will not retry.")
                try:
                    self.client.report_execution(pid, ok=False, error=str(e),
                                                 outcome_unknown=True)
                except Exception:  # noqa: BLE001
                    pass
            return str(e)

        mark(self.state, pid, "fired", fired_qty=qty)
        self.audit.record("execution", rec["connector_id"], "brake",
                          {"position_id": pid, "qty": qty, "ok": True})
        try:
            self.client.report_execution(pid, ok=True, result=result)
        except Exception:  # noqa: BLE001 (never re-arm a fired brake)
            pass
        self._notify(f"The brake exited {rec['symbol']}: {qty:g} at the market, "
                     f"its stop of {rec['stop']:g} was touched.")
        return ""
