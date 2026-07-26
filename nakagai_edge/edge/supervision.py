"""The ledger of positions the brake watches.

Built by joining two things the edge already produces: executed approvals,
which carry the order the broker accepted, and the portfolio sweep, which is
broker truth about what is actually held.

One rule governs every reconciliation path here: **broker truth wins, and
drift always resolves toward a smaller exit.** A position that shrank clamps.
A position that grew keeps the old ceiling and reports the surplus as
unguarded. A position that vanished is released. And a connector whose
snapshot FAILED reconciles nothing at all, because a brief broker outage looks
exactly like "the position was closed", and releasing on that would disarm the
brake at the worst possible moment.
"""

import json
import math
import time

from nakagai_edge.edge.state import EdgeState

# States a reconciliation may never move a record out of: the brake has acted or
# is acting, and only a human (or recover_interrupted) closes those out.
TERMINAL = ("firing", "fired", "outcome_unknown", "released")

_SYMBOL_KEYS = ("symbol", "ticker", "instrument_symbol")
_QUANTITY_KEYS = ("quantity", "qty", "shares", "position_size")


def load(state: EdgeState) -> dict:
    if not state.supervised_path.exists():
        return {}
    try:
        doc = json.loads(state.supervised_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    return doc if isinstance(doc, dict) else {}


def save(state: EdgeState, doc: dict) -> None:
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state.supervised_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2, default=str))
    tmp.replace(state.supervised_path)


def record(state: EdgeState, rec: dict) -> None:
    doc = load(state)
    doc[rec["position_id"]] = rec
    save(state, doc)


def mark(state: EdgeState, position_id: str, new_state: str, **extra) -> None:
    doc = load(state)
    rec = doc.get(position_id)
    if rec is None:
        return
    rec["state"] = new_state
    rec.update(extra)
    save(state, doc)


def claim(state: EdgeState, position_id: str, *, expected: str, new: str) -> bool:
    """Atomically move a record from `expected` to `new`, or refuse.

    Synchronous on purpose, with no await points anywhere inside it: asyncio
    cannot interleave two callers within a synchronous function, so a second
    concurrent claim on the same position loses the race and is refused rather
    than placing a second live order. `mark()` cannot do this job, because
    load-mutate-save lets both callers read `armed` before either writes.
    """
    doc = load(state)
    rec = doc.get(position_id)
    if rec is None or rec.get("state") != expected:
        return False
    rec["state"] = new
    save(state, doc)
    return True


def recover_interrupted(state: EdgeState) -> list[str]:
    """Close out any exit that was in flight when the process died.

    The brake spends its warrant BEFORE calling the broker, so a crash between
    those two moments leaves a record in `firing`. That is exactly the
    `outcome_unknown` case: the order may or may not have reached the broker,
    and nothing here may guess. Called once at startup; never retries.
    """
    doc = load(state)
    recovered = []
    for rec in doc.values():
        if rec.get("state") == "firing":
            rec["state"] = "outcome_unknown"
            rec["error"] = ("the edge stopped while this exit was in flight; "
                            "check the broker directly")
            recovered.append(rec["position_id"])
    if recovered:
        save(state, doc)
    return recovered


def _scalar(row: dict, keys) -> str:
    for key in keys:
        if row.get(key) not in (None, ""):
            return str(row[key])
    return ""


def held_quantities(portfolio_doc: dict) -> dict:
    """(connector, account, symbol) -> quantity, from connectors that ANSWERED.

    A connector or account carrying an error contributes nothing rather than
    contributing zeros. See the module docstring: zeros would read as "closed".
    A row whose quantity does not parse is likewise left OUT of this mapping
    rather than folded in as 0.0; see `unreadable()`, whose whole job is to
    keep that distinction visible to `reconcile`.
    """
    out: dict = {}
    for entry in (portfolio_doc or {}).get("connectors") or []:
        if entry.get("error"):
            continue
        cid = entry.get("id", "")
        for account in entry.get("accounts") or []:
            if account.get("error"):
                continue
            number = str(account.get("account_number", ""))
            for row in account.get("positions") or []:
                if not isinstance(row, dict):
                    continue
                symbol = _scalar(row, _SYMBOL_KEYS).upper()
                if not symbol:
                    continue
                try:
                    qty = float(_scalar(row, _QUANTITY_KEYS))
                except (TypeError, ValueError):
                    continue              # unreadable(), not zero
                out[(cid, number, symbol)] = qty
    return out


def answered(portfolio_doc: dict) -> set:
    """(connector, account) pairs whose snapshot succeeded. Absence from a
    successful snapshot is what proves a position is gone."""
    out = set()
    for entry in (portfolio_doc or {}).get("connectors") or []:
        if entry.get("error"):
            continue
        for account in entry.get("accounts") or []:
            if account.get("error"):
                continue
            out.add((entry.get("id", ""), str(account.get("account_number", ""))))
    return out


def unreadable(portfolio_doc: dict) -> set:
    """(connector, account, symbol) triples that appeared in a SUCCESSFUL
    snapshot but whose quantity could not be parsed.

    A row we cannot read is not a row reporting zero. Treating it as zero
    releases the position and disarms the brake: the same mistake as trusting a
    failed snapshot, arriving through a different door.
    """
    out = set()
    for entry in (portfolio_doc or {}).get("connectors") or []:
        if entry.get("error"):
            continue
        cid = entry.get("id", "")
        for account in entry.get("accounts") or []:
            if account.get("error"):
                continue
            number = str(account.get("account_number", ""))
            for row in account.get("positions") or []:
                if not isinstance(row, dict):
                    continue
                symbol = _scalar(row, _SYMBOL_KEYS).upper()
                if not symbol:
                    continue
                try:
                    float(_scalar(row, _QUANTITY_KEYS))
                except (TypeError, ValueError):
                    out.add((cid, number, symbol))
    return out


def reconcile(state: EdgeState, portfolio_doc: dict) -> dict:
    """Fold broker truth into the ledger. Returns the updated ledger."""
    doc = load(state)
    held = held_quantities(portfolio_doc)
    seen = answered(portfolio_doc)
    unread = unreadable(portfolio_doc)
    now = time.time()
    for rec in doc.values():
        if rec.get("state") in TERMINAL:
            continue
        direction = rec.get("direction", "long")
        if direction not in ("long", "short"):
            # An uninterpretable direction cannot tell us how to read the
            # broker's sign, so leave the record alone rather than guess.
            continue
        key = (rec["connector_id"], rec["account"], rec["symbol"])
        if key[:2] not in seen:
            continue                      # this account did not answer
        if key in unread:
            continue                      # quantity present but unparseable, not a release
        raw = held.get(key, 0.0)
        expected = 1.0 if direction == "long" else -1.0
        rec["last_confirmed_at"] = now
        if raw == 0:
            # Absent from a successful snapshot, or genuinely flat: closed.
            rec["state"] = "released"
            rec["confirmed_qty"] = 0.0
            rec["unguarded_qty"] = 0.0
            rec.pop("anomaly", None)
            continue
        if raw * expected < 0:
            # The broker reports the OPPOSITE direction to what we recorded, so
            # the position we would exit is not the position we are watching.
            # Exiting on the warrant's side would ADD exposure, which is the one
            # thing a reduce-only brake may never do. Leave it alone and say so.
            rec["anomaly"] = f"broker reports {raw:g} against a recorded {direction} position"
            continue
        actual = abs(raw)
        ceiling = float(rec["entry_qty"])
        rec["confirmed_qty"] = min(actual, ceiling)
        rec["unguarded_qty"] = max(0.0, actual - ceiling)
        rec.pop("anomaly", None)
    save(state, doc)
    return doc


def renewal_request(state: EdgeState) -> list[dict]:
    """What the platform needs to re-sign this edge's live warrants.

    Excludes any record the edge could never act on no matter what warrant came
    back: a direction that is not exactly "long" or "short" (the entry side
    could not be classified), or an empty account. Both halves matter, and the
    account half is the sharper one: `brake.fire()` asks the broker about
    `rec["account"]`, and a broker asked about account "" can answer with a
    readable list that does not name the symbol, which reads as "flat" and
    releases a fully-held position. Asking the platform to re-sign a warrant
    that can never be used is pointless traffic; PROMOTING a record on the
    answer is a live position reported as guarded.
    """
    return [{"position_id": rec["position_id"],
             "connector_id": rec["connector_id"], "account": rec["account"],
             "symbol": rec["symbol"],
             "confirmed_qty": float(rec.get("confirmed_qty", 0.0)),
             "signal_id": rec.get("signal_id", "")}
            for rec in load(state).values()
            if rec.get("state") in ("armed", "unguarded")
            and rec.get("direction") in ("long", "short")
            and rec.get("account")]


def apply_renewals(state: EdgeState, warrants: dict) -> dict:
    """Fold refreshed warrants into the ledger.

    A renewal may narrow a warrant or extend its clock. It may NEVER widen the
    ceiling: the entry quantity is the hard bound, and a platform bug (or a
    compromised platform) must not be able to hand this edge authority to sell
    more than the entry ever bought.
    """
    doc = load(state)
    if not isinstance(warrants, dict):
        # A malformed batch (a list, a string, ...) must cost every position
        # nothing: this is the same posture as one bad entry below, just at
        # the level of the whole payload instead of one item in it.
        return doc
    for position_id, warrant in warrants.items():
        rec = doc.get(position_id)
        if rec is None or rec.get("state") in TERMINAL:
            continue
        if rec.get("direction") not in ("long", "short") or not rec.get("account"):
            # Unguarded because the edge cannot act on this record at all (see
            # renewal_request above), not because a warrant was missing: no
            # warrant, however well-formed, makes closing_side() stop
            # returning "" for an unclassifiable side, or makes account "" a
            # question a broker can answer. Promoting it to "armed" would make
            # open_risk report guarded: True for a position that can never
            # actually be exited.
            continue
        if not isinstance(warrant, dict):
            continue          # one malformed entry must cost only itself
        try:
            ceiling = float(warrant.get("max_qty"))
        except (TypeError, ValueError):
            continue
        # NaN and the infinities parse cleanly through float() and are truthy,
        # and every NaN comparison is False, so a bare `> entry_qty` bound lets
        # them through and then silently disables every downstream ceiling
        # check (brake.fire()'s clamp, warrant.authorizes()'s comparison).
        # gateway/envelope.py rejects them at the read for the same reason. A
        # ceiling must be a real, non-negative number no larger than what the
        # entry actually bought.
        if not (math.isfinite(ceiling) and 0 <= ceiling <= float(rec["entry_qty"])):
            continue
        rec["warrant"] = warrant
        if rec.get("state") == "unguarded":
            rec["state"] = "armed"
    save(state, doc)
    return doc


def is_guarded(rec: dict, *, brake_armed: bool, disarmed) -> bool:
    """Will anything actually exit this position if its stop is touched?

    Three facts have to agree, and the last two live in brake.py, which imports
    this module: a record can be perfectly armed and warranted while the owner
    has locally disarmed the brake globally or released that one position. The
    caller supplies those two, because supervision cannot import brake without
    a cycle, and a `guarded` that ignores a disarm the owner just performed is
    worse than no field at all.
    """
    return (bool(rec.get("warrant")) and rec.get("state") == "armed"
            and brake_armed and rec.get("position_id") not in disarmed)


def open_risk(state: EdgeState, prices: dict, *, brake_armed: bool = True,
             disarmed=frozenset()) -> list[dict]:
    """Every supervised position, with its risk expressed in R.

    R is the entry-to-stop distance, which is the only unit in which one
    position's risk is comparable to another's. `brake_armed`/`disarmed`
    default permissive so a caller that predates the disarm switches (or
    never touches them) sees the same `guarded` it always did.
    """
    out = []
    for rec in load(state).values():
        price = prices.get(rec["symbol"])
        entry, stop = float(rec["entry_price"]), float(rec["stop"])
        one_r = abs(entry - stop)
        direction = rec.get("direction", "long")
        unrealized_r = distance = None
        # An unrecognized direction cannot say which way "unrealized" points,
        # so report it as unknown rather than silently reading it as short.
        if direction in ("long", "short") and price is not None and one_r > 0:
            long_ = direction == "long"
            move = (price - entry) if long_ else (entry - price)
            unrealized_r = move / one_r
            distance = (price - stop) if long_ else (stop - price)
        out.append({
            "position_id": rec["position_id"], "symbol": rec["symbol"],
            "connector_id": rec["connector_id"], "account": rec["account"],
            "direction": direction,
            "signal_id": rec.get("signal_id", ""),
            "qty": rec.get("confirmed_qty", rec["entry_qty"]),
            "unguarded_qty": rec.get("unguarded_qty", 0.0),
            "entry_price": entry, "stop": stop, "price": price,
            "unrealized_r": unrealized_r, "distance_to_stop": distance,
            "open_risk": (one_r * float(rec.get("confirmed_qty", 0.0))),
            "state": rec.get("state", "armed"),
            "guarded": is_guarded(rec, brake_armed=brake_armed, disarmed=disarmed),
            "warrant_expires_at": (rec.get("warrant") or {}).get("expires_at"),
        })
    return out
