"""A hub-compatible approval queue whose backing store is the platform, and
the local intent store the executor works from.

ConnectorHub calls `queue.enqueue(...)` when a guardrail verdict is `approve`;
here that is refused, because orders reach the broker only as owner-approved
order proposals. Adopted proposals become local intents (edge/proposals.py);
the executor verifies the platform's signed artifact against OUR copy of the
args, then retains a broker-accepted order until the fill journal matches its
exact order id."""

import json
import time

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.approvals import Approval, _require_account_key
from nakagai_edge.hub import GuardrailDenied

PROPOSALS_ONLY = ("orders reach your broker only as an order proposal your owner "
                  "approves on the platform (propose_order)")


def intents(state: EdgeState) -> dict:
    if not state.intents_path.exists():
        return {}
    try:
        return json.loads(state.intents_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_intents(state: EdgeState, doc: dict) -> None:
    state._write_private(state.intents_path, doc)


def drop_intent(state: EdgeState, approval_id: str) -> None:
    doc = intents(state)
    doc.pop(approval_id, None)
    _write_intents(state, doc)


def mark_submitted(
        state: EdgeState, approval_id: str, *, order_id: str,
        approval: dict, result: dict, execution_report: dict) -> None:
    """Freeze a broker acceptance and its exact pending platform report."""
    expected = {"ok", "result", "error", "outcome_unknown", "order_id"}
    if set(execution_report) != expected:
        raise ValueError("execution report does not have the exact wire fields")
    doc = intents(state)
    intent = doc.get(approval_id)
    if not isinstance(intent, dict):
        raise ValueError(f"local intent {approval_id!r} disappeared before submission")
    doc[approval_id] = {
        **intent,
        "phase": "submitted",
        "broker_order_id": order_id,
        "approval": approval,
        "broker_result": result,
        "pending_execution_report": execution_report,
        "submitted_at": time.time(),
    }
    _write_intents(state, doc)


def acknowledge_execution_report(
        state: EdgeState, approval_id: str, execution_report: dict) -> None:
    """Clear only the exact report marker after platform acknowledgement."""
    doc = intents(state)
    intent = doc.get(approval_id)
    if not isinstance(intent, dict):
        raise ValueError(f"local intent {approval_id!r} disappeared before report ack")
    if intent.get("pending_execution_report") != execution_report:
        raise ValueError(
            f"local intent {approval_id!r} changed before exact report ack"
        )
    settled = dict(intent)
    settled.pop("pending_execution_report")
    doc[approval_id] = settled
    _write_intents(state, doc)


def _to_approval(account_key: str, payload: dict) -> Approval:
    fields = {k: payload[k] for k in Approval._FIELDS if k in payload}
    fields.setdefault("id", payload.get("approval_id", ""))
    fields["account_key"] = account_key
    return Approval(**fields)


class RemoteApprovalQueue:
    def __init__(self, client: PlatformClient, state: EdgeState, agent_id: str) -> None:
        self.client = client
        self.state = state
        self.agent_id = agent_id

    def enqueue(self, account_key: str, connector_id: str, tool: str, args: dict, *,
                ttl_s: int, requested_by: str = "",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        # The platform takes no local write to wait for a human: an order
        # reaches the broker only as a proposal the owner approves there, which
        # the executor adopts from the platform's granted list. A write the
        # local guardrails send for approval is refused before it leaves here.
        raise GuardrailDenied(PROPOSALS_ONLY)

    def get(self, account_key: str, approval_id: str) -> Approval | None:
        from nakagai_edge.edge.client import EdgeClientError
        _require_account_key(account_key)
        try:
            return _to_approval(account_key, self.client.get_approval(approval_id))
        except EdgeClientError:
            return None
