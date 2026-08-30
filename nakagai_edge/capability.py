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

SCALARS = (str, int, float)

# How a canonical field is read. Declared by the vocabulary, never by a
# connector: a map may say WHERE a value lives, never what it means.
VERBATIM, FLOAT, UPPER_STR, SIDE = "verbatim", "float", "upper_str", "side"

COERCIONS = {
    "symbol": UPPER_STR,
    "side": SIDE,
    "quantity": FLOAT, "price": FLOAT, "stop": FLOAT, "day_pnl": FLOAT,
    "bid": FLOAT, "ask": FLOAT, "avg_price": FLOAT, "fill_price": FLOAT,
    "notional": FLOAT,
}
# `notional` is FLOAT beside `quantity` and `fill_price`: it is an order's SIZE
# and a reader does arithmetic on it. That is the opposite call from `equity`
# and `cash` below, which are display figures relayed exactly as the broker
# worded them.
#
# Everything absent from COERCIONS is VERBATIM: equity, cash, buying_power,
# market_value, currency, nickname, type, status, order_id, filled_at, account.
# The edge relays those figures and never does arithmetic on them.
#
# `filled_at` is verbatim for the same reason `status` is. Brokers spell
# timestamps a dozen ways, and a moment this module parsed into a float would
# be a moment it had done arithmetic on. The platform reads it; the edge only
# carries it.


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


# A broker's accepted entry is read through these fields. `order_type` is not
# a position-read requirement. An old broker result can still establish brake
# supervision when it does not echo the type it accepted.
ENTRY_FIELDS = ("symbol", "side", "quantity", "limit_price", "stop_price")

# Every field a version-one equity entry sends to a broker. This is deliberately
# distinct from ENTRY_FIELDS: writes need explicit type, time-in-force, and
# account declarations; position reads must not pretend those are facts a
# broker necessarily returns.
OUTBOUND_ORDER_FIELDS = (
    "symbol", "side", "order_type", "quantity", "limit_price",
    "stop_price", "time_in_force", "account")

OUTBOUND_TYPES = frozenset(("string", "number"))
CANONICAL_ORDER_ENUMS = {
    "side": frozenset(("buy", "sell")),
    "order_type": frozenset(("limit", "market")),
}

CAPABILITIES: dict[str, CapabilitySpec] = {
    "list_accounts": CapabilitySpec(
        args=(), required=("account",), optional=("nickname", "type"),
        is_list=True, is_write=False),
    "get_balance": CapabilitySpec(
        args=("account",), required=("equity",),
        optional=("cash", "buying_power", "currency", "day_pnl"),
        is_list=False, is_write=False),
    "list_positions": CapabilitySpec(
        args=("account",), required=("symbol", "quantity"),
        optional=("avg_price", "market_value"),
        is_list=True, is_write=False),
    "get_quote": CapabilitySpec(
        args=("symbols",), required=("symbol", "price"), optional=("bid", "ask"),
        is_list=True, is_write=False),
    # `quantity` and `notional` are two ways to say how big an order is, and a
    # broker may answer in either. Robinhood sizes a dollar-based order in
    # money and leaves the share count null until it fills, so a journal with
    # only `quantity` records that a trade happened and not how large it was.
    #
    # They are separate fields rather than one, deliberately. Falling back
    # (`quantity: [quantity, dollar_based_amount]`) would populate the field
    # and put DOLLARS in a share count, which `first()` would do happily and
    # nothing downstream could detect. This module's own docstring states the
    # invariant: a wrong map "can never make `quantity` mean notional". Two
    # words is what makes that true rather than aspirational.
    #
    # Neither is required, so an order sized either way still journals.
    "list_orders": CapabilitySpec(
        args=("account", "status"), required=("order_id", "symbol"),
        optional=("side", "quantity", "notional", "status", "fill_price",
                  "filled_at"),
        is_list=True, is_write=False),
    # `optional` here is the RESULT of placing an order, not its arguments:
    # `args` above is what goes out, these are what can be read back. The fill
    # journal joins on `order_id`, and `fill_price` is what retired the
    # guess-list of broker price keys that used to live in executor.py. A
    # connector declaring neither still places orders exactly as before.
    "place_order": CapabilitySpec(
        args=OUTBOUND_ORDER_FIELDS,
        required=(), optional=("order_id", "fill_price"),
        is_list=False, is_write=True),
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
    outbound_types: dict[str, str] = Field(default_factory=dict)


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
    if isinstance(value, bool) or not isinstance(value, SCALARS):
        # `bool` is deliberately not a scalar here, and the check has to be
        # explicit because bool subclasses int in Python: isinstance(True, int)
        # is True, so `true` would otherwise reach float() and coerce to 1.0. A
        # broker answering `true` for a quantity or a stop has said nothing
        # numeric at all, and inventing one share, or a stop at $1.00, out of
        # that is the exact "unreadable becomes a number" failure this whole
        # module refuses. No canonical field is a boolean, so nothing legible
        # is lost.
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


def _outbound_enum(field: str, value: Any, cap: Capability) -> Any:
    """A canonical argument value in the broker's own spelling.

    The first declared alias is the spelling the broker receives.
    """
    aliases = (cap.values.get(field) or {}).get(str(value).lower()) or []
    if not aliases:
        raise CapabilityError(
            f"connector declares no {value!r} spelling for {field!r}")
    return aliases[0]


def _normalize_order_args(args: dict) -> dict:
    """Return one canonical spelling before any order validation runs."""
    normalized = dict(args)
    for field, allowed in CANONICAL_ORDER_ENUMS.items():
        value = normalized.get(field)
        if value is None:
            continue
        if not isinstance(value, str):
            raise CapabilityError(f"{field!r} must be a canonical string")
        value = value.strip().lower()
        if value not in allowed:
            raise CapabilityError(f"unsupported canonical {field!r}: {value!r}")
        normalized[field] = value
    return normalized


def _validate_order_map(cap: Capability) -> None:
    """Refuse ambiguous keys and connector-expanded canonical enums."""
    mapped: dict[str, str] = {}
    for field in OUTBOUND_ORDER_FIELDS:
        key = cap.args.get(field)
        if not key:
            continue
        other = mapped.get(key)
        if other is not None:
            raise CapabilityError(
                f"place_order maps {other!r} and {field!r} to broker argument "
                f"{key!r}")
        mapped[key] = field
    for field, allowed in CANONICAL_ORDER_ENUMS.items():
        unknown = sorted(set(cap.values.get(field) or {}) - allowed)
        if unknown:
            raise CapabilityError(
                f"connector adds unsupported canonical {field!r} values: "
                f"{', '.join(unknown)}")


def _finite_number(field: str, value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CapabilityError(f"{field!r} must be a finite number")
    if not math.isfinite(value):
        raise CapabilityError(f"{field!r} must be a finite number")
    return value


def _outbound_value(field: str, value: Any, cap: Capability) -> Any:
    """Translate and exactly type one outgoing canonical argument."""
    if field in ("side", "order_type"):
        value = _outbound_enum(field, value, cap)
    outbound_type = cap.outbound_types.get(field)
    if outbound_type not in OUTBOUND_TYPES:
        raise CapabilityError(
            f"connector declares no supported outbound type for {field!r}")
    if field == "quantity":
        number = _finite_number(field, value)
        if number <= 0:
            raise CapabilityError("'quantity' must be positive")
        if int(number) != number:
            raise CapabilityError("'quantity' must be a whole-share integer")
        value = int(number)
    elif field in ("limit_price", "stop_price"):
        value = _finite_number(field, value)
        if value <= 0:
            raise CapabilityError(f"{field!r} must be positive")
    elif isinstance(value, bool):
        raise CapabilityError(f"{field!r} must not be a boolean")
    if outbound_type == "number":
        return _finite_number(field, value)
    if isinstance(value, (dict, list, tuple, set)):
        raise CapabilityError(f"{field!r} cannot convert to string")
    if isinstance(value, float) and not math.isfinite(value):
        raise CapabilityError(f"{field!r} must be a finite number")
    return str(value)


def resolve(name: str, cap: Capability, args: dict) -> tuple[str, dict]:
    """The downstream tool and arguments for one capability call.

    A canonical argument the connector never declared is refused rather than
    dropped: silently discarding an account or a stop would send the broker a
    materially different order than the caller asked for.
    """
    vocab = CAPABILITIES.get(name)
    if vocab is None:
        raise CapabilityError(f"unknown capability {name!r}")
    if not isinstance(args, dict):
        raise CapabilityError(f"capability {name!r} arguments must be a dictionary")
    if name == "place_order":
        args = _normalize_order_args(args)
        _validate_order_map(cap)
        missing = [field for field in ("symbol", "side", "order_type", "quantity",
                                       "time_in_force", "account")
                   if args.get(field) is None]
        if args.get("order_type") == "limit":
            missing.extend(field for field in ("limit_price", "stop_price")
                           if args.get(field) is None)
        elif args.get("order_type") == "market" and any(
                args.get(field) is not None
                for field in ("limit_price", "stop_price")):
            raise CapabilityError(
                "market orders must not carry limit_price or stop_price")
        if missing:
            raise CapabilityError(
                f"place_order is missing required fields: {', '.join(missing)}")
    out: dict = {}
    for canonical, value in args.items():
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
        if name == "place_order":
            out[key] = _outbound_value(canonical, value, cap)
        else:
            out[key] = _outbound_enum(canonical, value, cap) if canonical == "side" else value
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
