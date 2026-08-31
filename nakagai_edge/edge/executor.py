"""Resolve pending write intents: verify the platform's signed grant against
the LOCAL copy of the intent, execute at the broker, report back. Verification
is fail-closed: any mismatch reports an error and never touches the broker."""

import asyncio
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
from nakagai_edge.edge.remote import drop_intent, intents, mark_submitted
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    apply_renewals,
    is_guarded,
    load as load_positions,
    record as record_position,
    renewal_request,
)
from nakagai_edge.edge.sync import policy_fresh, public_key
from nakagai_edge.signing import verify_artifact
from nakagai_edge.warrant import exit_order_args, read_entry

DEAD_STATUSES = ("denied", "expired", "error", "executed")
TERMINAL_ORDER_STATUSES = ("cancelled", "rejected")

log = logging.getLogger("nakagai.edge")


def _verify(
        state: EdgeState, approval_id: str, intent: dict,
        record: dict, artifact) -> str:
    """Empty string when the artifact authorizes exactly this intent, else why not."""
    if not isinstance(artifact, dict):
        return "no artifact on a granted approval"
    pub = public_key(state)
    if not pub:
        return "no signing public key in the cached bundle"
    if not verify_artifact(pub, artifact):
        return "signature verification failed"
    agent = state.agent() or {}
    if intent.get("candidate_id"):
        signal_id = intent.get("signal_id")
        if not isinstance(signal_id, str) or not signal_id.strip():
            return "candidate signal_id is missing or empty"
        if signal_id != signal_id.strip():
            return "candidate signal_id is not whitespace-exact"
        if record.get("signal_id") != signal_id:
            return "signal_id mismatch"
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


def _fill_price(fill: dict, fallback: float) -> float:
    """What the position actually cost, when the broker says so.

    R is measured from the entry, so a fill 6 cents from the limit is 6 cents
    of error in every R this position ever reports. A zero or negative value is
    what a broker plausibly returns for an order accepted but not yet filled,
    not a real price, so only a strictly positive number may displace the
    order's own price.
    """
    value = fill.get("fill_price")
    return value if isinstance(value, float) and value > 0 else fallback


def supervise(hub, state: EdgeState, approval_id: str, intent: dict,
              record_doc: dict, fill: dict) -> dict | None:
    """Turn one broker-reconciled fill into a supervised position.

    Never raises. This runs only after the fill journal matched the broker's
    order id. A bookkeeping failure must never hide that filled exposure.

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
            "entry_price": _fill_price(fill, entry["price"]),
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
        log.warning("a reconciled fill was not recorded for supervision, "
                    "so %s is unsupervised: %s", approval_id, e)
        return None


async def _verify_supervision(
        hub, state: EdgeState, client: PlatformClient,
        approval_id: str, intent: dict,
        record_doc: dict, fill: dict) -> None:
    spec = hub.spec(intent["connector_id"])
    status, rows, error = await position_rows(hub, spec, intent["account"])
    if status != "ok":
        raise ValueError(error or "broker positions are unreadable")
    cap = spec.capability("place_order")
    entry = read_entry(cap, intent.get("args") or {})
    if entry is None:
        raise ValueError("submitted entry is no longer readable through its capability")
    symbol = str(entry["symbol"]).upper()
    aliases = cap.values.get("side") or {}
    buys = [str(value).lower() for value in aliases.get("buy") or []]
    sells = [str(value).lower() for value in aliases.get("sell") or []]
    direction = ("long" if entry["side"] in buys
                 else "short" if entry["side"] in sells else "")
    visible = any(
        str(row.get("symbol") or "").upper() == symbol
        and isinstance(row.get("quantity"), (int, float))
        and not isinstance(row.get("quantity"), bool)
        and ((direction == "long" and row["quantity"] > 0)
             or (direction == "short" and row["quantity"] < 0))
        for row in rows if isinstance(row, dict))
    if not visible:
        raise ValueError("broker positions do not show the supervised entry")
    rec = supervise(hub, state, approval_id, intent, record_doc, fill)
    if rec is None:
        raise ValueError("supervision record was not durably persisted")
    from nakagai_edge.edge.brake import armed, disarmed_positions
    guarded = {
        "brake_armed": armed(state),
        "disarmed": disarmed_positions(state),
        "now": time.time(),
    }
    if not is_guarded(rec, **guarded):
        ask = [position for position in renewal_request(state)
               if position.get("position_id") == approval_id]
        if ask:
            renewed = await asyncio.to_thread(client.renew_warrants, ask)
            apply_renewals(state, renewed.get("warrants") or {})
            rec = load_positions(state).get(approval_id)
    if not is_guarded(
            rec or {}, brake_armed=armed(state),
            disarmed=disarmed_positions(state), now=time.time()):
        raise ValueError("supervision record is not armed and warranted")
    if load_positions(state).get(approval_id) != rec:
        raise ValueError("supervision record changed before broker verification completed")


def _declared_fill(spec, row: dict) -> bool:
    return _declared_order_status(spec, row, "filled")


def declared_terminal_order_values(spec) -> tuple[str, ...]:
    """Broker values explicitly mapped as terminal without a fill."""
    try:
        cap = spec.capability("list_orders")
    except CapabilityError:
        return ()
    values = cap.values.get("status") or {}
    return tuple(dict.fromkeys(
        str(value).strip()
        for canonical in TERMINAL_ORDER_STATUSES
        for value in values.get(canonical) or []
        if str(value).strip()
    ))


def _declared_order_status(spec, row: dict, canonical: str) -> bool:
    try:
        cap = spec.capability("list_orders")
    except CapabilityError:
        return False
    declared = {
        str(value).strip().lower()
        for value in (cap.values.get("status") or {}).get(canonical) or []
    }
    return bool(declared) and str(row.get("status") or "").strip().lower() in declared


def _terminal_order_status(spec, row: dict) -> str:
    return next((status for status in TERMINAL_ORDER_STATUSES
                 if _declared_order_status(spec, row, status)), "")


def _matching_fill(spec, intent: dict, row: dict) -> str:
    """Empty for the exact submitted order, otherwise the safe refusal."""
    try:
        entry = read_entry(spec.capability("place_order"), intent.get("args") or {})
    except CapabilityError as error:
        return str(error)
    if entry is None:
        return "submitted entry is unreadable through its capability"
    if str(row.get("symbol") or "").upper() != str(entry["symbol"]).upper():
        return "matched broker order id returned a different symbol"
    if row.get("side") not in ("buy", "sell"):
        return "matched broker order id returned an unreadable side"
    aliases = spec.capability("place_order").values.get("side") or {}
    expected_side = next((canonical for canonical in ("buy", "sell")
                          if entry["side"] in [
                              str(value).lower()
                              for value in aliases.get(canonical) or []]), "")
    if row["side"] != expected_side:
        return "matched broker order id returned a different side"
    quantity = row.get("quantity")
    if (not isinstance(quantity, (int, float)) or isinstance(quantity, bool)
            or float(quantity) != float(entry["qty"])):
        return "matched broker order id returned a different quantity"
    return ""


async def reconcile_submitted_fills(
        hub, state: EdgeState, client: PlatformClient, audit: EdgeAudit,
        spec, account: str, rows: list[dict]) -> int:
    """Supervise submitted entries only from their exact broker fill rows."""
    matched = 0
    for approval_id, intent in list(intents(state).items()):
        if (not isinstance(intent, dict) or intent.get("phase") != "submitted"
                or intent.get("connector_id") != spec.id
                or intent.get("account") != account):
            continue
        fill = next((row for row in rows
                     if isinstance(row, dict)
                     and str(row.get("order_id") or "")
                     == str(intent.get("broker_order_id") or "")
                     and _declared_fill(spec, row)), None)
        terminal = next((
            _terminal_order_status(spec, row)
            for row in rows
            if isinstance(row, dict)
            and str(row.get("order_id") or "")
            == str(intent.get("broker_order_id") or "")
            and _terminal_order_status(spec, row)
        ), "")
        if fill is None and terminal:
            matched += 1
            reason = f"broker order reached terminal status {terminal}"
            try:
                audit.record(
                    "denial", intent["connector_id"], intent["tool"],
                    {"approval_id": approval_id, "error": reason,
                     "order_id": intent["broker_order_id"]})
            except Exception:  # noqa: BLE001 (the durable outcome owns retry)
                pass
            drop_intent(state, approval_id)
            continue
        if fill is None:
            continue
        matched += 1
        reason = _matching_fill(spec, intent, fill)
        try:
            if reason:
                raise ValueError(reason)
            await _verify_supervision(
                hub, state, client, approval_id, intent,
                intent.get("approval") or {}, fill)
        except Exception as error:  # noqa: BLE001 (a matched fill now exists)
            reason = reason or (
                "supervision failed after reconciled broker fill: "
                f"{type(error).__name__}: {error}")
            if intent.get("candidate_id"):
                disarm_candidate_entries(
                    state, candidate_id=intent["candidate_id"],
                    approval_id=approval_id, reason=reason)
                _candidate_outcome(
                    state, client, intent["candidate_id"],
                    mechanical_status="submitted", reason=reason,
                    approval_id=approval_id, urgent=True, outcome_unknown=True)
            _alert_candidate(client, reason)
            try:
                audit.record(
                    "error", intent["connector_id"], intent["tool"],
                    {"approval_id": approval_id, "error": reason,
                     "outcome_unknown": True})
            except Exception:  # noqa: BLE001 (the durable outcome owns retry)
                pass
        else:
            try:
                audit.record(
                    "execution", intent["connector_id"], intent["tool"],
                    {"approval_id": approval_id, "ok": True,
                     "order_id": intent["broker_order_id"], "filled": True})
            except Exception:  # noqa: BLE001 (journal is best effort)
                pass
        drop_intent(state, approval_id)
    return matched


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
        if isinstance(intent, dict) and intent.get("phase") == "submitted":
            continue
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

        why_not = _verify(
            state, approval_id, intent, record, record.get("artifact"))
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
            # Broker acceptance is a submitted order, not a fill. Candidate
            # entries stay durable under their exact broker id until the fill
            # journal sees that same order in a declared filled state.
            if intent.get("candidate_id"):
                order_id = placed_order_id(hub, intent, result)
                try:
                    mark_submitted(
                        state, approval_id, order_id=order_id,
                        approval=record, result=result)
                except Exception as error:  # noqa: BLE001 (the broker accepted)
                    reason = (
                        "candidate submission could not be persisted after broker result: "
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
                    if not order_id:
                        reason = (
                            "candidate broker result has no declared order id; "
                            "fill attribution is impossible")
                        disarm_candidate_entries(
                            state, candidate_id=intent["candidate_id"],
                            approval_id=approval_id, reason=reason)
                        _candidate_outcome(
                            state, client, intent["candidate_id"],
                            mechanical_status="blocked", reason=reason,
                            approval_id=approval_id, urgent=True,
                            outcome_unknown=True)
                        _alert_candidate(client, reason)
                    try:
                        client.report_execution(
                            approval_id, ok=True, result=result,
                            order_id=order_id)
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
                            approval_id=approval_id, urgent=True,
                            outcome_unknown=True)
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
                                {"approval_id": approval_id, "ok": True,
                                 "order_id": order_id, "submitted": True})
                        except Exception:  # noqa: BLE001 (journal is best-effort here)
                            pass
            else:
                order_id = placed_order_id(hub, intent, result)
                if order_id:
                    try:
                        mark_submitted(
                            state, approval_id, order_id=order_id,
                            approval=record, result=result)
                    except Exception as error:  # noqa: BLE001 (the broker accepted)
                        try:
                            audit.record(
                                "error", intent["connector_id"], intent["tool"],
                                {"approval_id": approval_id,
                                 "error": ("submission could not be persisted: "
                                           f"{type(error).__name__}: {error}"),
                                 "outcome_unknown": True})
                        except Exception:  # noqa: BLE001 (journal is best effort)
                            pass
                try:
                    audit.record(
                        "execution", intent["connector_id"], intent["tool"],
                        {"approval_id": approval_id, "ok": True,
                         "order_id": order_id, "submitted": True})
                except Exception:  # noqa: BLE001 (journal is best-effort here)
                    pass
                try:
                    client.report_execution(
                        approval_id, ok=True, result=result,
                        order_id=order_id)
                except Exception:  # noqa: BLE001 (never re-arm an executed intent)
                    pass
        current = intents(state).get(approval_id)
        if not isinstance(current, dict) or current.get("phase") != "submitted":
            drop_intent(state, approval_id)
        resolved += 1
    return resolved
