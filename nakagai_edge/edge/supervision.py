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
                try:
                    qty = float(_scalar(row, _QUANTITY_KEYS) or 0.0)
                except (TypeError, ValueError):
                    continue
                if symbol:
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


def reconcile(state: EdgeState, portfolio_doc: dict) -> dict:
    """Fold broker truth into the ledger. Returns the updated ledger."""
    doc = load(state)
    held = held_quantities(portfolio_doc)
    seen = answered(portfolio_doc)
    now = time.time()
    for rec in doc.values():
        if rec.get("state") in TERMINAL:
            continue
        key = (rec["connector_id"], rec["account"], rec["symbol"])
        if key[:2] not in seen:
            continue                      # this account did not answer
        actual = held.get(key, 0.0)
        rec["last_confirmed_at"] = now
        if actual <= 0:
            rec["state"] = "released"
            rec["confirmed_qty"] = 0.0
            rec["unguarded_qty"] = 0.0
            continue
        ceiling = float(rec["entry_qty"])
        rec["confirmed_qty"] = min(actual, ceiling)
        rec["unguarded_qty"] = max(0.0, actual - ceiling)
    save(state, doc)
    return doc


def open_risk(state: EdgeState, prices: dict) -> list[dict]:
    """Every supervised position, with its risk expressed in R.

    R is the entry-to-stop distance, which is the only unit in which one
    position's risk is comparable to another's.
    """
    out = []
    for rec in load(state).values():
        price = prices.get(rec["symbol"])
        entry, stop = float(rec["entry_price"]), float(rec["stop"])
        one_r = abs(entry - stop)
        long_ = rec.get("direction", "long") == "long"
        unrealized_r = distance = None
        if price is not None and one_r > 0:
            move = (price - entry) if long_ else (entry - price)
            unrealized_r = move / one_r
            distance = (price - stop) if long_ else (stop - price)
        out.append({
            "position_id": rec["position_id"], "symbol": rec["symbol"],
            "connector_id": rec["connector_id"], "account": rec["account"],
            "direction": rec.get("direction", "long"),
            "signal_id": rec.get("signal_id", ""),
            "qty": rec.get("confirmed_qty", rec["entry_qty"]),
            "unguarded_qty": rec.get("unguarded_qty", 0.0),
            "entry_price": entry, "stop": stop, "price": price,
            "unrealized_r": unrealized_r, "distance_to_stop": distance,
            "open_risk": (one_r * float(rec.get("confirmed_qty", 0.0))),
            "state": rec.get("state", "armed"),
            "guarded": bool(rec.get("warrant")) and rec.get("state") == "armed",
            "warrant_expires_at": (rec.get("warrant") or {}).get("expires_at"),
        })
    return out
