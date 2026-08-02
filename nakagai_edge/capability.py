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


# The five fields that identify and size one order. A statement about the
# `place_order` capability, so it belongs here beside the vocabulary rather
# than in the modules that consume it: config.py validates a declared map
# against this tuple at parse time, and warrant.py reads an executed entry
# through it. Two copies would let those two drift, and a `place_order` map the
# validator accepts but `read_entry` cannot read is a live position with no
# brake on it.
ORDER_FIELDS = ("symbol", "side", "quantity", "price", "stop")

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
        args=ORDER_FIELDS + ("account",),
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
    # Where this capability's result sits inside the response, first path
    # winning. `fields` are then read relative to it: relative to each element
    # for a list capability, relative to the object itself for a scalar one.
    # One meaning for one word, so a map author never has to remember which
    # paths are rooted and which are absolute. Empty means the response root,
    # which is how a broker that wraps nothing at all maps.
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


def _outbound(field: str, value: Any, cap: Capability) -> Any:
    """A canonical argument value in the broker's own spelling.

    Only `side` needs translating. The first alias a connector lists is the
    spelling it gets, which is why the order of `values.side.buy` matters.
    """
    if COERCIONS.get(field) is not SIDE:
        return value
    aliases = (cap.values.get("side") or {}).get(str(value).lower()) or []
    if not aliases:
        raise CapabilityError(
            f"connector declares no {value!r} spelling for `side`")
    return aliases[0]


def resolve(name: str, cap: Capability, args: dict) -> tuple[str, dict]:
    """The downstream tool and arguments for one capability call.

    A canonical argument the connector never declared is refused rather than
    dropped: silently discarding an account or a stop would send the broker a
    materially different order than the caller asked for.
    """
    vocab = CAPABILITIES.get(name)
    if vocab is None:
        raise CapabilityError(f"unknown capability {name!r}")
    out: dict = {}
    for canonical, value in (args or {}).items():
        if value is None:
            continue
        if canonical not in vocab.args:
            raise CapabilityError(
                f"capability {name!r} takes no argument {canonical!r}")
        key = cap.args.get(canonical)
        if not key:
            raise CapabilityError(
                f"connector does not declare where {canonical!r} goes for "
                f"{name!r}")
        out[key] = _outbound(canonical, value, cap)
    return cap.tool, out


def read_partial(name: str, cap: Capability, node: Any) -> dict:
    """Every canonical field that coerced, with no required-field check.

    The portfolio sweep needs this: a position whose quantity is unreadable
    must still be keyable by symbol so supervision.unreadable() can name it,
    rather than vanishing and reading as a position that closed.
    """
    vocab = CAPABILITIES[name]
    out: dict = {}
    for field in vocab.required + vocab.optional:
        paths = cap.fields.get(field)
        if not paths:
            continue
        raw = first(node, paths)
        if raw is None:
            continue
        value = coerce(field, raw, cap)
        if value is None:
            continue
        out[field] = value
    return out


def read_row(name: str, cap: Capability, node: Any) -> dict | None:
    """One node read into canonical fields, or None if a required one is
    missing. None means unreadable, never zero and never absent-so-default."""
    out = read_partial(name, cap, node)
    if any(field not in out for field in CAPABILITIES[name].required):
        return None
    return out


def extract(name: str, cap: Capability, payload: Any) -> dict | list[dict] | None:
    """A broker response read back into canonical fields.

    `cap.items` roots both kinds of capability, because a broker that wraps its
    lists in an envelope wraps its scalars in the same one. Robinhood's
    {"data": ..., "guide": ...} is exactly that, and naming the node once per
    capability is what lets the portfolio sweep's hardcoded peel be deleted
    rather than generalized into a second rule.

    A list capability drops rows it cannot read rather than folding them in as
    zero; a scalar capability returns None for the same reason. See coerce().

    None means UNREADABLE for both kinds, and an empty list means the broker
    answered with nothing. Those are different facts and the caller has to keep
    them apart: "the account holds nothing" is what an agent reads before
    buying, so an unreadable answer counted as flat is how one position becomes
    two on a real account.
    """
    vocab = CAPABILITIES.get(name)
    if vocab is None:
        raise CapabilityError(f"unknown capability {name!r}")
    node = first(payload, cap.items) if cap.items else payload
    if not vocab.is_list:
        return read_row(name, cap, node)
    if not isinstance(node, list):
        # Not an empty list: the map named a node that is not a list at all, so
        # nothing here was read. Returning [] made "this connector's map does
        # not fit what the broker sent" indistinguishable from "the broker
        # holds nothing", under an answer that carried no error either.
        return None
    read = (read_row(name, cap, row) for row in node)
    return [row for row in read if row is not None]
