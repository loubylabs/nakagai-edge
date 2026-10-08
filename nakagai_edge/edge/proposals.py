"""Adopt platform-granted order proposals as local frozen intents.

A proposal's order was written by an outside agent and sealed by the owner on
the platform. The edge never trusts that: it verifies the signed artifact,
re-resolves the canonical order through its OWN place_order map, and requires
its own sole order-capable broker and account, before it writes the intent the
unchanged executor then verifies again and executes.

Protocol 1 carries one order shape: a buy limit with a protective stop below
the limit. Anything else is refused here even when the signature is good.

One grant becomes at most one local intent, ever. The executor drops an intent
once it has reported, and a report the platform never received leaves that
approval listed as granted; a durable ledger of adopted approval ids is what
stops the next pass from adopting, and so submitting, the same order twice."""

import json
import logging
import math
import time

from nakagai_edge.capability import OUTBOUND_ORDER_FIELDS, CapabilityError, resolve
from nakagai_edge.edge.remote import _write_intents, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import policy_fresh, public_key
from nakagai_edge.signing import args_hash, verify_artifact

log = logging.getLogger("nakagai.edge")

_ITEM = {"approval_id", "proposal_id", "connector_id", "tool", "args",
         "args_hash", "canonical_order", "account", "expires_at", "artifact"}

# An adopted id is kept until its artifact has been expired this long. An
# expired artifact can never pass verification, so forgetting it after that
# cannot let it execute.
_LEDGER_RETAIN_S = 86_400


class _LedgerUnreadable(Exception):
    pass


def _adopted(state: EdgeState) -> dict:
    """approval_id -> artifact expiry. Unreadable means adopt nothing."""
    path = state.proposals_adopted_path
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as error:
        raise _LedgerUnreadable(str(error)) from error
    if not isinstance(doc, dict):
        raise _LedgerUnreadable("ledger is not an object")
    return doc


def _expiry(art: dict) -> float | None:
    value = art.get("expires_at")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _is_price(value) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value) and value > 0)


def _refusal(state: EdgeState, item: dict, sole) -> str:
    """Empty when the item may become a local intent, else why not."""
    if set(item) != _ITEM:
        return "granted proposal has missing or additional fields"
    art = item["artifact"]
    pub = public_key(state)
    if not isinstance(art, dict) or not pub or not verify_artifact(pub, art):
        return "signature verification failed"
    for field in ("proposal_id", "connector_id", "tool", "args_hash", "account"):
        if not isinstance(item[field], str) or not item[field]:
            return f"{field} must be a nonempty string"
    agent = state.agent() or {}
    canonical = item["canonical_order"]
    if not isinstance(canonical, dict) or set(canonical) != set(OUTBOUND_ORDER_FIELDS):
        return "canonical_order is not the exact outbound shape"
    if canonical.get("side") != "buy" or canonical.get("order_type") != "limit":
        return "proposal protocol 1 carries only a buy limit order"
    limit, stop = canonical.get("limit_price"), canonical.get("stop_price")
    if not (_is_price(limit) and _is_price(stop) and stop < limit):
        return "a proposed buy limit needs a protective stop below the limit"
    if sole is None:
        return "no sole order-capable broker with one allowed account"
    spec, account = sole
    try:
        tool, args = resolve("place_order", spec.capability("place_order"), canonical)
    except CapabilityError as error:
        return f"canonical_order failed the local capability map: {error}"
    expires_at = _expiry(art)
    checks = (
        (item["connector_id"] == spec.id, "connector mismatch"),
        (item["account"] == account == str(canonical.get("account")),
         "account mismatch"),
        (item["tool"] == tool, "tool mismatch"),
        (item["args"] == args, "args do not match the local capability map"),
        (item["args_hash"] == args_hash(args), "args_hash mismatch"),
        (art.get("approval_id") == item["approval_id"], "approval_id mismatch"),
        (art.get("proposal_id") == item["proposal_id"], "proposal_id mismatch"),
        (art.get("candidate_id", "") == "", "a proposal grant cannot name a candidate"),
        (art.get("agent_id") == agent.get("agent_id"), "agent_id mismatch"),
        (art.get("connector_id") == item["connector_id"], "connector_id mismatch"),
        (art.get("tool") == item["tool"], "tool mismatch"),
        (art.get("args_hash") == item["args_hash"], "args_hash mismatch"),
        (art.get("account") == item["account"], "account mismatch"),
        (expires_at is not None and expires_at > time.time(), "artifact expired"),
    )
    return next((why for ok, why in checks if not ok), "")


def adopt_granted_proposals(hub, state: EdgeState, client, audit) -> int:
    """Turn new owner-granted proposals into local intents. Returns how many."""
    from nakagai_edge.edge.brake import armed
    from nakagai_edge.edge.runtime import _candidate_broker_account

    if not policy_fresh(state):
        return 0                       # retry next pass, report nothing
    try:
        adopted = _adopted(state)
    except _LedgerUnreadable as error:
        log.warning("proposal ledger is unreadable, so no proposal is adopted: %s",
                    error)
        return 0
    items = client.granted_proposals()
    held = intents(state)
    try:
        sole = _candidate_broker_account(state)
    except ValueError:
        sole = None
    now = time.time()
    count = 0
    for item in items:
        approval_id = item.get("approval_id") if isinstance(item, dict) else None
        if (not isinstance(approval_id, str) or not approval_id
                or approval_id in held or approval_id in adopted):
            continue
        why = _refusal(state, item, sole) or (
            "" if armed(state) else "local brake is disarmed")
        if why:
            try:
                audit.record("denial", str(item.get("connector_id", "")),
                             str(item.get("tool", "")),
                             {"approval_id": approval_id, "error": why})
            except Exception:  # noqa: BLE001 (journal is best effort)
                pass
            try:
                client.report_execution(approval_id, ok=False,
                                        error=f"proposal grant refused: {why}")
            except Exception:  # noqa: BLE001 (the broker was never contacted)
                pass
            continue
        # The ledger is written first: a crash between the two writes leaves a
        # grant that never executes, which the platform expires, rather than
        # one that could be adopted twice.
        adopted = {aid: exp for aid, exp in adopted.items()
                   if not isinstance(exp, (int, float))
                   or exp + _LEDGER_RETAIN_S > now}
        adopted[approval_id] = _expiry(item["artifact"])
        state._write_private(state.proposals_adopted_path, adopted)
        held[approval_id] = {
            "connector_id": item["connector_id"], "tool": item["tool"],
            "args": item["args"], "args_hash": item["args_hash"],
            "signal_id": "", "candidate_id": "",
            "proposal_id": item["proposal_id"],
            "account": item["account"], "created_at": now}
        _write_intents(state, held)
        try:
            audit.record("adopted", item["connector_id"], item["tool"],
                         {"approval_id": approval_id,
                          "proposal_id": item["proposal_id"]})
        except Exception:  # noqa: BLE001 (journal is best effort)
            pass
        count += 1
    return count
