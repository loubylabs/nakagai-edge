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
