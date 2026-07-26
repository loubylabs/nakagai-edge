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


def normalize_quote(payload) -> dict | None:
    """A broker's quote payload as {price, bid, ask, ts}, or None.

    `ts` is left to the caller: brokers rarely stamp a quote, and inventing a
    timestamp here would defeat the freshness check that reads it.
    """
    if not isinstance(payload, dict):
        return None
    price = _num(payload, _PRICE_KEYS)
    if price is None:
        return None
    return {"price": price, "bid": _num(payload, _BID_KEYS),
            "ask": _num(payload, _ASK_KEYS), "ts": 0.0}


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
    if ts and (now - ts) > max_age_s:
        return f"stale quote ({round(now - ts)}s old)"
    bid, ask = quote.get("bid"), quote.get("ask")
    if bid and ask:
        mid = (float(bid) + float(ask)) / 2
        if mid > 0 and ((float(ask) - float(bid)) / mid) * 100 > max_spread_pct:
            return "spread is too wide to price an exit"
    if prior_price:
        move = abs(price - float(prior_price)) / float(prior_price) * 100
        if move > max_jump_pct:
            return f"implausible jump ({round(move)}%) from the prior quote"
    return ""


def advance(run: int, is_breach: bool) -> int:
    return run + 1 if is_breach else 0


def confirmed(run: int, needed: int = BREACH_CONFIRMATIONS) -> bool:
    return run >= needed
