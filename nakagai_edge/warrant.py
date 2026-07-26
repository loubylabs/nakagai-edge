"""The exit warrant: a standing, reduce-only authority to close one position.

An entry grant authorizes exactly one order, because `args_hash` covers the
exact arguments. A warrant cannot work that way: the exit quantity is not
knowable at entry time, because the position may be reduced before the stop is
ever touched. So a warrant authorizes a narrow SET of orders instead, bounded
by connector, account, symbol, closing side, a quantity ceiling, single use,
and an expiry. That is a deliberate weakening of the one-approval-one-order
guarantee, and these bounds are the entire thing that replaces it.

Pure: no I/O, no state, no clock of its own. Same posture as guardrails.py and
the platform's envelope.py, so the safety story is testable without a network.
"""

import time

from nakagai_edge.signing import verify_artifact

WARRANT_KIND = "exit_warrant"

TRIGGER_BELOW = "at_or_below"      # a long's stop
TRIGGER_ABOVE = "at_or_above"      # a short's stop


def build_warrant_payload(*, grant_id: str, agent_id: str, connector_id: str,
                          account: str, symbol: str, tool: str, side: str,
                          max_qty: float, trigger_kind: str, level: float,
                          ttl_s: int, now: float | None = None) -> dict:
    """The unsigned payload. The platform signs this beside the entry grant;
    the edge only ever verifies it."""
    now = time.time() if now is None else now
    return {"grant_id": grant_id, "kind": WARRANT_KIND, "agent_id": agent_id,
            "connector_id": connector_id, "account": account,
            "symbol": str(symbol).upper(), "tool": tool,
            "side": str(side).lower(), "max_qty": float(max_qty),
            "trigger": {"kind": trigger_kind, "level": float(level)},
            "reduce_only": True, "one_shot": True, "expires_at": now + ttl_s}


def breached(trigger: dict, price: float) -> bool:
    """Has the level been touched? An unreadable trigger never fires: a level
    we cannot interpret must not produce an arbitrary exit."""
    try:
        kind = (trigger or {}).get("kind")
        level = float((trigger or {}).get("level"))
        price = float(price)
    except (TypeError, ValueError, AttributeError):
        return False
    if kind == TRIGGER_BELOW:
        return price <= level
    if kind == TRIGGER_ABOVE:
        return price >= level
    return False


def authorizes(public_key: str, warrant: dict, exit_order: dict, *,
               agent_id: str, held_qty: float, spent: bool,
               now: float | None = None) -> str:
    """Empty string when `warrant` authorizes exactly `exit_order`, else why not.

    Signature first: every field below is only meaningful once the artifact is
    known to be the platform's.
    """
    now = time.time() if now is None else now
    if not isinstance(warrant, dict):
        return "no warrant"
    if not isinstance(exit_order, dict):
        return "no exit order"
    if not verify_artifact(public_key, warrant):
        return "signature verification failed"
    if warrant.get("kind") != WARRANT_KIND:
        return "artifact is not an exit warrant"
    if warrant.get("agent_id") != agent_id:
        return "agent_id mismatch"
    try:
        if float(warrant.get("expires_at", 0)) <= now:
            return "warrant expired"
    except (TypeError, ValueError):
        return "warrant expired"
    if not warrant.get("reduce_only"):
        return "warrant is not reduce-only"
    if spent:
        return "warrant already spent"

    if exit_order.get("connector_id") != warrant.get("connector_id"):
        return "connector mismatch"
    if exit_order.get("account") != warrant.get("account"):
        return "account mismatch"
    if exit_order.get("tool") != warrant.get("tool"):
        return "tool mismatch"
    if str(exit_order.get("symbol", "")).upper() != warrant.get("symbol"):
        return "symbol mismatch"
    if str(exit_order.get("side", "")).lower() != warrant.get("side"):
        return "side mismatch"

    try:
        qty = float(exit_order.get("qty"))
        ceiling = float(warrant.get("max_qty"))
        held = float(held_qty)
    except (TypeError, ValueError):
        return "exit quantity is not a number"
    if not qty > 0:
        return "exit quantity is not positive"
    if qty > ceiling:
        return "exit quantity exceeds the warrant ceiling"
    if qty > held:
        return "exit quantity exceeds the position held"
    return ""


# ---- reading an entry, deriving its exit --------------------------------
#
# One boundary governs both functions: the order fields must sit at the TOP
# level of `args`. The platform's envelope walks one level down to READ an
# order, but constructing one INTO a nested payload is a different problem and
# guessing wrong places a real trade. A nested entry is declined here and the
# position records as unguarded.


def _at(args: dict, keys: list[str]) -> str:
    """The first of `keys` present in `args` with a scalar value, or ""."""
    for key in keys:
        if key in args:
            value = args[key]
            if value is not None and not isinstance(value, (dict, list)):
                return key
    return ""


def read_entry(shape, args: dict) -> dict | None:
    """The five fields the ledger needs, or None if this entry cannot be
    supervised. A missing stop is disqualifying here (unlike the envelope,
    which reports it and lets policy decide): with no stop there is no level,
    and with no level there is nothing to watch."""
    if not shape.configured or not isinstance(args, dict):
        return None
    keys = {name: _at(args, getattr(shape, f"{name}_keys"))
            for name in ("symbol", "side", "quantity", "price", "stop")}
    if not all(keys.values()):
        return None
    try:
        return {"symbol": str(args[keys["symbol"]]).upper(),
                "side": str(args[keys["side"]]).lower(),
                "qty": float(args[keys["quantity"]]),
                "price": float(args[keys["price"]]),
                "stop": float(args[keys["stop"]])}
    except (TypeError, ValueError):
        return None


def closing_side(shape, entry_side: str) -> str:
    """The side that reduces a position opened on `entry_side`, or "" when the
    connector never declared one."""
    entry = str(entry_side).lower()
    if entry in [v.lower() for v in shape.buy_values]:
        return shape.sell_values[0] if shape.sell_values else ""
    if entry in [v.lower() for v in shape.sell_values]:
        return shape.buy_values[0] if shape.buy_values else ""
    return ""


def exit_order_args(shape, entry_args: dict, qty: float) -> dict | None:
    """The market exit for a position opened by `entry_args`, or None.

    Derived from the payload the broker already accepted rather than built from
    scratch, so account ids and any broker-required extras carry over without
    this module needing to know they exist.
    """
    if not shape.market_order_args:
        return None
    entry = read_entry(shape, entry_args)
    if entry is None:
        return None
    side = closing_side(shape, entry["side"])
    if not side:
        return None
    # A market declaration may not touch the fields that identify or size the
    # position: symbol, side, and quantity. A connector that declares one of
    # those is misconfigured, and resolving the collision either way would be
    # a guess about an order bound for a real brokerage, so refuse. Price and
    # stop keys are deliberately excluded from this set: they are already
    # stripped below, so a collision there is expected and harmless.
    aliases = (set(shape.symbol_keys) | set(shape.side_keys)
               | set(shape.quantity_keys))
    if aliases & set(shape.market_order_args):
        return None
    symbol_key = _at(entry_args, shape.symbol_keys)
    side_key = _at(entry_args, shape.side_keys)
    qty_key = _at(entry_args, shape.quantity_keys)
    # Strip EVERY symbol, side, and quantity alias, not just the one `_at`
    # picked: an unused alias sitting in the entry payload would otherwise
    # ride along beside the canonical one and leave the broker two answers.
    # `aliases` is now safe to reuse for `drop` because every field it names
    # gets written back below, unlike round 2's version of this set.
    drop = set(shape.price_keys) | set(shape.stop_keys) | aliases
    args = {k: v for k, v in entry_args.items() if k not in drop}
    args.update(shape.market_order_args)
    # The RAW value, not entry["symbol"]: read_entry uppercases for
    # comparison, but the exit reuses the payload the broker already
    # accepted, so a lowercase ticker on the way in stays lowercase here.
    args[symbol_key] = entry_args[symbol_key]
    args[side_key] = side
    args[qty_key] = float(qty)
    return args
