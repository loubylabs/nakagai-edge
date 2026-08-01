"""What an action MEANS, separated from what one broker CALLS it.

The vocabulary here owns the meaning and the type of every canonical field.
A connector's `capabilities:` map owns location only: which downstream tool,
which argument keys, which response paths. That split is the safety property.
A wrong map points at the wrong field and produces a visibly wrong number or
an extraction failure; it can never make `quantity` mean notional, and it can
never turn a verbatim display string into something the envelope computes on.

Pure: no I/O, no state, no clock of its own. Same posture as guardrails.py and
warrant.py, so the whole translation story is testable without a broker.

This module must not import from config.py. config.py imports Capability and
CAPABILITIES from here, and the dependency stays one-directional.
"""

import math
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

SCALARS = (str, int, float, bool)

# How a canonical field is read. Declared by the vocabulary, never by a
# connector: a map may say WHERE a value lives, never what it means.
VERBATIM, FLOAT, UPPER_STR, SIDE = "verbatim", "float", "upper_str", "side"

COERCIONS = {
    "symbol": UPPER_STR,
    "side": SIDE,
    "quantity": FLOAT, "price": FLOAT, "stop": FLOAT,
    "bid": FLOAT, "ask": FLOAT, "avg_price": FLOAT,
}
# Everything absent from COERCIONS is VERBATIM: equity, cash, buying_power,
# market_value, currency, nickname, type, status, order_id, account. The edge
# relays those figures and never does arithmetic on them.


class CapabilityError(Exception):
    """A capability could not be resolved against this connector."""


@dataclass(frozen=True)
class CapabilitySpec:
    """One entry in the closed vocabulary."""
    args: tuple[str, ...]
    required: tuple[str, ...]
    optional: tuple[str, ...]
    is_list: bool
    is_write: bool


CAPABILITIES: dict[str, CapabilitySpec] = {
    "list_accounts": CapabilitySpec(
        args=(), required=("account",), optional=("nickname", "type"),
        is_list=True, is_write=False),
    "get_balance": CapabilitySpec(
        args=("account",), required=("equity",),
        optional=("cash", "buying_power", "currency"),
        is_list=False, is_write=False),
    "list_positions": CapabilitySpec(
        args=("account",), required=("symbol", "quantity"),
        optional=("avg_price", "market_value"),
        is_list=True, is_write=False),
    "get_quote": CapabilitySpec(
        args=("symbols",), required=("symbol", "price"), optional=("bid", "ask"),
        is_list=True, is_write=False),
    "list_orders": CapabilitySpec(
        args=("account", "status"), required=("order_id", "symbol"),
        optional=("side", "quantity", "status"),
        is_list=True, is_write=False),
    "place_order": CapabilitySpec(
        args=("symbol", "side", "quantity", "price", "stop", "account"),
        required=(), optional=(), is_list=False, is_write=True),
    "cancel_order": CapabilitySpec(
        args=("order_id", "account"), required=(), optional=(),
        is_list=False, is_write=True),
}


class Capability(BaseModel):
    """One connector's answer to "where does this action live on you?".

    Location only. No expression language, so a map cannot compute.
    """
    tool: str
    args: dict[str, str] = Field(default_factory=dict)
    items: list[str] = Field(default_factory=list)
    fields: dict[str, list[str]] = Field(default_factory=dict)
    values: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    market_args: dict = Field(default_factory=dict)


def walk(payload: Any, path: str) -> Any:
    """Follow dotted dict keys. Anything non-dict on the way down yields None.

    Deliberately not an expression language: no indexing, no wildcards, no
    arithmetic. A map that cannot compute cannot surprise the audit trail.
    """
    node = payload
    for segment in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(segment)
    return node


def first(payload: Any, paths: list[str]) -> Any:
    """The earliest path yielding a non-None value, or None."""
    for path in paths:
        value = walk(payload, path)
        if value is not None:
            return value
    return None


def coerce(field: str, value: Any, cap: Capability) -> Any | None:
    """The canonical value for `field`, or None when it cannot be read.

    None is always "unreadable", never "zero" and never "absent-so-default".
    Callers drop the field, and a required field dropping means the whole row
    is dropped. That is supervision.unreadable()'s rule, generalized.
    """
    kind = COERCIONS.get(field, VERBATIM)
    if kind is SIDE:
        aliases = cap.values.get("side") or {}
        wanted = str(value).strip().lower()
        for canonical in ("buy", "sell"):
            if wanted in [a.lower() for a in aliases.get(canonical) or []]:
                return canonical
        return None
    if not isinstance(value, SCALARS):
        return None
    if kind is UPPER_STR:
        return str(value).strip().upper() or None
    if kind is FLOAT:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed):
            # float() accepts "nan", "inf", "-inf", "infinity" (and json.loads
            # accepts bare NaN/Infinity as an extension), so a malformed
            # broker payload can hand us a real non-finite float here, and it
            # already passed the SCALARS check above. A NaN price or quantity
            # would sail through coerce and reach warrant.py's breached(),
            # where every comparison against NaN is False: the stop is never
            # seen as broken, the brake never fires, and the display keeps
            # reporting the position as guarded. Refuse it here instead, the
            # same fail-closed posture warrant.py's max_qty check uses.
            return None
        return parsed
    return value
