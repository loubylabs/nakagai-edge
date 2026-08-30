"""Resolve pending write intents: verify the platform's signed grant against
the LOCAL copy of the intent, execute at the broker, report back. Verification
is fail-closed: any mismatch reports an error and never touches the broker."""

import logging
import time

import httpx

from nakagai_edge.capability import CapabilityError, first, read_partial
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.candidate import (
    candidate_entries_armed,
    deliver_candidate_outcome,
    disarm_candidate_entries,
    flush_candidate_outcomes,
)
from nakagai_edge.edge.portfolio import position_rows
from nakagai_edge.edge.remote import drop_intent, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (is_guarded, load as load_positions,
                                           record as record_position)
from nakagai_edge.edge.sync import policy_fresh, public_key
from nakagai_edge.signing import verify_artifact
from nakagai_edge.warrant import exit_order_args, read_entry

DEAD_STATUSES = ("denied", "expired", "error", "executed")

log = logging.getLogger("nakagai.edge")


def _verify(state: EdgeState, approval_id: str, intent: dict, artifact) -> str:
    """Empty string when the artifact authorizes exactly this intent, else why not."""
    if not isinstance(artifact, dict):
        return "no artifact on a granted approval"
    pub = public_key(state)
    if not pub:
        return "no signing public key in the cached bundle"
    if not verify_artifact(pub, artifact):
        return "signature verification failed"
    agent = state.agent() or {}
    checks = (
        (artifact.get("approval_id") == approval_id, "approval_id mismatch"),
        (artifact.get("agent_id") == agent.get("agent_id"), "agent_id mismatch"),
        (artifact.get("connector_id") == intent["connector_id"],
         "connector_id mismatch"),
        (artifact.get("tool") == intent["tool"], "tool mismatch"),
        (artifact.get("args_hash") == intent["args_hash"], "args_hash mismatch"),
        (artifact.get("candidate_id", "") == intent.get("candidate_id", ""),
         "candidate_id mismatch"),
        (not intent.get("account")
         or artifact.get("account") == intent["account"], "account mismatch"),
        (float(artifact.get("expires_at", 0)) > time.time(), "artifact expired"),
    )
    for ok, why in checks:
        if not ok:
            return why
    return ""


def _placed(cap, result) -> dict:
    """What the broker said back about an order it accepted, canonically.

    This used to be a hardcoded list of candidate price keys plus a hand-rolled
    peel of a nested `data` envelope: exactly the two things the capability
    layer exists to abolish, surviving in the one module nobody re-read when it
    landed. The map now says where the envelope sits (`items`) and where the
    fields live (`fields`), the same as every other read in the edge.

    Never raises. A connector that declares no `place_order` map at all, or one
    that declares no result fields, yields {} and every caller falls back to
    what it knew before. Bookkeeping must not relabel a really-executed trade.
    """
    try:
        payload = (result or {}).get("data")
        node = first(payload, cap.items) if cap.items else payload
        return read_partial("place_order", cap, node) if isinstance(node, dict) else {}
    except Exception:  # noqa: BLE001 (the broker already executed; see supervise)
        return {}


def placed_order_id(hub, intent: dict, result) -> str:
    """The broker's own id for the order this intent just placed, or "".

    The left side of the attribution join: the fill journal records every order
    the broker reports, and one that no approval claims by id is a trade the
    OWNER placed by hand. Empty is the honest answer for a connector declaring
    no path to it, and an empty id matches nothing rather than matching wrongly.

    Never raises, for the same reason `supervise` never does: the broker has
    already executed by the time this runs, and no bookkeeping failure may
    relabel a real trade.
    """
    try:
        cap = hub.spec(intent["connector_id"]).capability("place_order")
    except Exception:  # noqa: BLE001 (nothing declared, nothing to read)
        return ""
    return str(_placed(cap, result).get("order_id", "") or "")


def _fill_price(placed: dict, fallback: float) -> float:
    """What the position actually cost, when the broker says so.

    R is measured from the entry, so a fill 6 cents from the limit is 6 cents
    of error in every R this position ever reports. A zero or negative value is
    what a broker plausibly returns for an order accepted but not yet filled,
    not a real price, so only a strictly positive number may displace the
    order's own price.
    """
    value = placed.get("fill_price")
    return value if isinstance(value, float) and value > 0 else fallback


def supervise(hub, state: EdgeState, approval_id: str, intent: dict,
              record_doc: dict, result) -> dict | None:
    """Turn a just-executed entry into a supervised position.

    Never raises. This runs after the broker already succeeded, and a
    bookkeeping failure must never relabel a really-executed trade.

    `approval_id` is the caller's own key from its intent store, not
    something read back off `record_doc`: a platform that omitted or
    duplicated the field must never make two positions collide under the
    same ledger key.
    """
    try:
        if not approval_id:
            return None                  # nothing to key the ledger on
        spec = hub.spec(intent["connector_id"])
        try:
            cap = spec.capability("place_order")
        except CapabilityError:
            return None                  # nothing declared, nothing to supervise
        entry = read_entry(cap, intent.get("args") or {})
        if entry is None:
            return None                  # no stop, or an incomplete map
        warrant = record_doc.get("exit_warrant")
        account = ""
        for name in spec.guardrails.accounts.arg_names:
            if intent["args"].get(name):
                account = str(intent["args"][name])
                break
        aliases = cap.values.get("side") or {}
        buys = [v.lower() for v in aliases.get("buy") or []]
        sells = [v.lower() for v in aliases.get("sell") or []]
        direction = ("long" if entry["side"] in buys
                     else "short" if entry["side"] in sells else "")
        # Anything on this list leaves a position we can SEE but must never act
        # on: the direction decides how reconcile reads the broker's sign, the
        # account is what the warrant is scoped to and the only thing the
        # broker can be asked about, and without a resolvable canonical market
        # order there is no exit order to build at all (spec section 9). Recording
        # those unguarded keeps them visible; recording a guess would put a
        # wrong R multiple on the owner's screen beside a `guarded: True` that
        # is not true. Every reason is named rather than just the first: the
        # anomaly is where the owner reads what to fix, and one hidden behind
        # another costs a second cycle with a live position unprotected.
        blocked = "; ".join(why for why, ok in (
            ("unclassifiable order side", direction),
            ("no account on the order", account),
            ("connector cannot express a canonical market exit",
             exit_order_args(cap, intent.get("args") or {}, entry["qty"])),
        ) if not ok)
        rec = {
            "position_id": approval_id,
            "connector_id": intent["connector_id"],
            "account": account, "symbol": entry["symbol"],
            "direction": direction,
            "signal_id": record_doc.get("signal_id", ""),
            "entry_args": intent.get("args") or {},
            "entry_qty": entry["qty"],
            "entry_price": _fill_price(_placed(cap, result), entry["price"]),
            "stop": entry["stop"],
            "confirmed_qty": entry["qty"], "last_confirmed_at": 0.0,
            "unguarded_qty": 0.0,
            # Durable, unlike `anomaly` below, which reconciliation clears on
            # its next clean sweep. Nothing a broker says later can change what
            # the entry order said, so every gate that could re-arm this record
            # reads this field instead of re-deriving the judgement. See the
            # supervision module docstring.
            "blocked": blocked,
            "warrant": None if blocked else warrant,
            "state": "unguarded" if (blocked or not warrant) else "armed",
            "opened_at": time.time()}
        if blocked:
            rec["anomaly"] = blocked
        record_position(state, rec)
        return load_positions(state).get(approval_id)
    except Exception as e:  # noqa: BLE001 (bookkeeping must never break execution)
        # The swallow is deliberate and the log is not decoration: the broker
        # already executed, so raising would relabel a real trade, but a lost
        # record means a LIVE position nothing is watching and no trace of why.
        log.warning("a just-executed entry was not recorded for supervision, "
                    "so %s is unsupervised: %s", approval_id, e)
        return None


async def _verify_candidate_supervision(
        hub, state: EdgeState, approval_id: str, intent: dict,
        record_doc: dict, result) -> None:
    rec = supervise(hub, state, approval_id, intent, record_doc, result)
    if rec is None:
        raise ValueError("supervision record was not durably persisted")
    from nakagai_edge.edge.brake import armed, disarmed_positions
    if not is_guarded(
            rec, brake_armed=armed(state), disarmed=disarmed_positions(state),
            now=time.time()):
        raise ValueError("supervision record is not armed and warranted")
    spec = hub.spec(intent["connector_id"])
    status, rows, error = await position_rows(hub, spec, rec["account"])
    if status != "ok":
        raise ValueError(error or "broker positions are unreadable")
    symbol = str(rec["symbol"]).upper()
    direction = rec.get("direction")
    visible = any(
        str(row.get("symbol") or "").upper() == symbol
        and isinstance(row.get("quantity"), (int, float))
        and not isinstance(row.get("quantity"), bool)
        and ((direction == "long" and row["quantity"] > 0)
             or (direction == "short" and row["quantity"] < 0))
        for row in rows if isinstance(row, dict))
    if not visible:
        raise ValueError("broker positions do not show the supervised entry")
    if load_positions(state).get(approval_id) != rec:
        raise ValueError("supervision record changed before broker verification completed")


def _candidate_outcome(state: EdgeState, client: PlatformClient,
                       candidate_id: str, *, mechanical_status: str,
                       reason: str, approval_id: str, urgent: bool = False,
                       outcome_unknown: bool = False) -> bool:
    return deliver_candidate_outcome(
        state, client, candidate_id,
        mechanical_status=mechanical_status,
        mechanical_reason=reason,
        approval_id=approval_id,
        urgent=urgent,
        outcome_unknown=outcome_unknown,
    )


def _alert_candidate(client: PlatformClient, reason: str) -> None:
    try:
        client.agent_checkin("alert", reason)
    except Exception:  # noqa: BLE001 (the durable outcome remains queued)
        pass


async def poll_once(hub, state: EdgeState, client: PlatformClient,
                    audit: EdgeAudit) -> int:
    """One pass over pending intents. Returns how many reached a terminal state."""
    try:
        flush_candidate_outcomes(state, client)
    except Exception:  # noqa: BLE001 (one corrupt report cannot stop the executor)
        pass
    resolved = 0
    for approval_id, intent in list(intents(state).items()):
        try:
            record = client.get_approval(approval_id)
        except (EdgeClientError, httpx.HTTPError, ValueError):
            continue                      # platform unreachable; try next pass
        status = record.get("status", "")
        if status == "pending":
            continue
        if status in DEAD_STATUSES:
            audit.record("denial" if status == "denied" else status,
                         intent["connector_id"], intent["tool"],
                         {"approval_id": approval_id})
            drop_intent(state, approval_id)
            resolved += 1
            continue
        if status != "granted":
            continue

        why_not = _verify(state, approval_id, intent, record.get("artifact"))
        if why_not:
            audit.record("error", intent["connector_id"], intent["tool"],
                         {"approval_id": approval_id,
                          "error": f"artifact verification failed: {why_not}"})
            try:
                client.report_execution(approval_id, ok=False,
                                        error=f"artifact verification failed: {why_not}")
            except EdgeClientError:
                pass
            if intent.get("candidate_id"):
                _candidate_outcome(
                    state, client, intent["candidate_id"],
                    mechanical_status="blocked",
                    reason=f"artifact verification failed: {why_not}",
                    approval_id=approval_id)
            drop_intent(state, approval_id)
            resolved += 1
            continue

        if not policy_fresh(state):
            # Grant looks good, but our guardrails/account-pins may be stale, so
            # refuse to execute against them. Leave the intent in place so a
            # fresh sync re-arms this pass; don't report anything upstream,
            # since nothing final happened.
            try:
                audit.record("deferred", intent["connector_id"], intent["tool"],
                             {"approval_id": approval_id,
                              "reason": "policy stale; deferring granted intent"})
            except Exception:  # noqa: BLE001 (journal is best-effort here)
                pass
            if intent.get("candidate_id"):
                reason = "local policy is stale; candidate grant refused"
                _candidate_outcome(
                    state, client, intent["candidate_id"],
                    mechanical_status="blocked", reason=reason,
                    approval_id=approval_id)
                drop_intent(state, approval_id)
                resolved += 1
            continue

        if intent.get("candidate_id"):
            from nakagai_edge.edge.brake import armed
            local_refusal = (
                "local candidate entries are disarmed; candidate grant refused"
                if not candidate_entries_armed(state)
                else "local brake is disarmed; candidate grant refused"
                if not armed(state) else "")
            if local_refusal:
                error = local_refusal
                try:
                    audit.record("denial", intent["connector_id"], intent["tool"],
                                 {"approval_id": approval_id, "error": error,
                                  "candidate_id": intent["candidate_id"]})
                except Exception:  # noqa: BLE001 (journal is best-effort here)
                    pass
                try:
                    client.report_execution(approval_id, ok=False, error=error)
                except Exception:  # noqa: BLE001 (the broker was never contacted)
                    pass
                _candidate_outcome(
                    state, client, intent["candidate_id"],
                    mechanical_status="blocked", reason=error,
                    approval_id=approval_id)
                drop_intent(state, approval_id)
                resolved += 1
                continue

        try:
            result = await hub.call(intent["connector_id"], intent["tool"],
                                    intent["args"], account_key=hub.account_key,
                                    approved=True)
        except Exception as e:  # noqa: BLE001 (the report must reflect reality)
            # Anything past the guardrails may have reached the broker.
            unknown = type(e).__name__ != "GuardrailDenied"
            try:
                audit.record("error", intent["connector_id"], intent["tool"],
                             {"approval_id": approval_id, "error": str(e)})
            except Exception:  # noqa: BLE001 (journal is best-effort here)
                pass
            try:
                client.report_execution(approval_id, ok=False,
                                        error=f"{type(e).__name__}: {e}",
                                        outcome_unknown=unknown)
            except Exception:  # noqa: BLE001 (never re-arm an attempted intent)
                pass
            if intent.get("candidate_id"):
                reason = f"{type(e).__name__}: {e}"
                if unknown:
                    disarm_candidate_entries(
                        state, candidate_id=intent["candidate_id"],
                        approval_id=approval_id, reason=reason)
                    _alert_candidate(client, reason)
                _candidate_outcome(
                    state, client, intent["candidate_id"],
                    mechanical_status="blocked", reason=reason,
                    approval_id=approval_id, urgent=unknown,
                    outcome_unknown=unknown)
        else:
            # The broker call already succeeded. A failure to audit or report
            # it must not relabel a really-executed trade as unknown/failed,
            # and it must never keep the intent alive for a second execution.
            if intent.get("candidate_id"):
                try:
                    await _verify_candidate_supervision(
                        hub, state, approval_id, intent, record, result)
                except Exception as error:  # noqa: BLE001 (a fill may already exist)
                    reason = (
                        "candidate supervision failed after broker result: "
                        f"{type(error).__name__}: {error}")
                    disarm_candidate_entries(
                        state, candidate_id=intent["candidate_id"],
                        approval_id=approval_id, reason=reason)
                    try:
                        client.report_execution(
                            approval_id, ok=False, result=result, error=reason,
                            outcome_unknown=True)
                    except Exception:  # noqa: BLE001 (the candidate outcome is durable)
                        pass
                    _candidate_outcome(
                        state, client, intent["candidate_id"],
                        mechanical_status="blocked", reason=reason,
                        approval_id=approval_id, urgent=True,
                        outcome_unknown=True)
                    _alert_candidate(client, reason)
                    try:
                        audit.record(
                            "error", intent["connector_id"], intent["tool"],
                            {"approval_id": approval_id, "error": reason,
                             "outcome_unknown": True})
                    except Exception:  # noqa: BLE001 (journal is best-effort here)
                        pass
                else:
                    try:
                        client.report_execution(
                            approval_id, ok=True, result=result,
                            order_id=placed_order_id(hub, intent, result))
                    except Exception as error:  # noqa: BLE001 (never retry the broker)
                        reason = (
                            "candidate execution report failed after broker result: "
                            f"{type(error).__name__}: {error}")
                        disarm_candidate_entries(
                            state, candidate_id=intent["candidate_id"],
                            approval_id=approval_id, reason=reason)
                        _candidate_outcome(
                            state, client, intent["candidate_id"],
                            mechanical_status="blocked", reason=reason,
                            approval_id=approval_id, urgent=True)
                        _alert_candidate(client, reason)
                        try:
                            audit.record(
                                "error", intent["connector_id"], intent["tool"],
                                {"approval_id": approval_id, "error": reason})
                        except Exception:  # noqa: BLE001 (journal is best-effort here)
                            pass
                    else:
                        try:
                            audit.record(
                                "execution", intent["connector_id"], intent["tool"],
                                {"approval_id": approval_id, "ok": True})
                        except Exception:  # noqa: BLE001 (journal is best-effort here)
                            pass
                        _candidate_outcome(
                            state, client, intent["candidate_id"],
                            mechanical_status="submitted",
                            reason=("broker execution reported and supervision "
                                    "verified"),
                            approval_id=approval_id)
            else:
                try:
                    audit.record(
                        "execution", intent["connector_id"], intent["tool"],
                        {"approval_id": approval_id, "ok": True})
                except Exception:  # noqa: BLE001 (journal is best-effort here)
                    pass
                try:
                    client.report_execution(
                        approval_id, ok=True, result=result,
                        order_id=placed_order_id(hub, intent, result))
                except Exception:  # noqa: BLE001 (never re-arm an executed intent)
                    pass
                supervise(hub, state, approval_id, intent, record, result)
        drop_intent(state, approval_id)
        resolved += 1
    return resolved
