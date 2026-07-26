"""Resolve pending write intents: verify the platform's signed grant against
the LOCAL copy of the intent, execute at the broker, report back. Verification
is fail-closed: any mismatch reports an error and never touches the broker."""

import time

import httpx

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.remote import drop_intent, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import record as record_position
from nakagai_edge.edge.sync import policy_fresh, public_key
from nakagai_edge.signing import verify_artifact
from nakagai_edge.warrant import read_entry

DEAD_STATUSES = ("denied", "expired", "error", "executed")


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
        (artifact.get("args_hash") == intent["args_hash"], "args_hash mismatch"),
        (float(artifact.get("expires_at", 0)) > time.time(), "artifact expired"),
    )
    for ok, why in checks:
        if not ok:
            return why
    return ""


_FILL_PRICE_KEYS = ("average_price", "filled_price", "execution_price", "price")


def _fill_price(result, fallback: float) -> float:
    """What the position actually cost, when the broker says so.

    R is measured from the entry, so a fill 6 cents from the limit is 6 cents
    of error in every R this position ever reports. Best effort: brokers differ
    in what they return, and the order's own price is an honest fallback.
    """
    payload = (result or {}).get("data")
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    if isinstance(payload, dict):
        for key in _FILL_PRICE_KEYS:
            try:
                if payload.get(key) not in (None, ""):
                    return float(payload[key])
            except (TypeError, ValueError):
                continue
    return fallback


def supervise(hub, state: EdgeState, intent: dict, record_doc: dict,
              result) -> None:
    """Turn a just-executed entry into a supervised position.

    Never raises. This runs after the broker already succeeded, and a
    bookkeeping failure must never relabel a really-executed trade.
    """
    try:
        spec = hub.spec(intent["connector_id"])
        entry = read_entry(spec.guardrails.order_shape, intent.get("args") or {})
        if entry is None:
            return                       # no stop, or no declared shape
        warrant = record_doc.get("exit_warrant")
        account = ""
        for name in spec.guardrails.accounts.arg_names:
            if intent["args"].get(name):
                account = str(intent["args"][name])
                break
        long_ = entry["side"] in [v.lower()
                                  for v in spec.guardrails.order_shape.buy_values]
        record_position(state, {
            "position_id": record_doc.get("approval_id", ""),
            "connector_id": intent["connector_id"],
            "account": account, "symbol": entry["symbol"],
            "direction": "long" if long_ else "short",
            "signal_id": record_doc.get("signal_id", ""),
            "entry_args": intent.get("args") or {},
            "entry_qty": entry["qty"],
            "entry_price": _fill_price(result, entry["price"]),
            "stop": entry["stop"],
            "confirmed_qty": entry["qty"], "last_confirmed_at": 0.0,
            "unguarded_qty": 0.0,
            "warrant": warrant,
            "state": "armed" if warrant else "unguarded",
            "opened_at": time.time()})
    except Exception:  # noqa: BLE001 (bookkeeping must never break execution)
        pass


async def poll_once(hub, state: EdgeState, client: PlatformClient,
                    audit: EdgeAudit) -> int:
    """One pass over pending intents. Returns how many reached a terminal state."""
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
            continue

        try:
            result = await hub.call(intent["connector_id"], intent["tool"],
                                    intent["args"], approved=True)
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
        else:
            # The broker call already succeeded. A failure to audit or report
            # it must not relabel a really-executed trade as unknown/failed,
            # and it must never keep the intent alive for a second execution.
            try:
                audit.record("execution", intent["connector_id"], intent["tool"],
                             {"approval_id": approval_id, "ok": True})
            except Exception:  # noqa: BLE001 (journal is best-effort here)
                pass
            try:
                client.report_execution(approval_id, ok=True, result=result)
            except Exception:  # noqa: BLE001 (never re-arm an executed intent)
                pass
            supervise(hub, state, intent, record, result)
        drop_intent(state, approval_id)
        resolved += 1
    return resolved
