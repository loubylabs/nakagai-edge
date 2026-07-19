"""Resolve pending write intents: verify the platform's signed grant against
the LOCAL copy of the intent, execute at the broker, report back. Verification
is fail-closed: any mismatch reports an error and never touches the broker."""

import time

import httpx

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.remote import drop_intent, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import policy_fresh, public_key
from nakagai_edge.signing import verify_artifact

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
        drop_intent(state, approval_id)
        resolved += 1
    return resolved
