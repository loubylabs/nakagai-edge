"""A hub-compatible approval queue whose backing store is the platform.

ConnectorHub calls `queue.enqueue(...)` when a guardrail verdict is `approve`;
here that posts the intent to the platform (where a human sees it) and records
it locally with its args_hash. The executor later verifies the platform's
signed artifact against OUR copy of the args, then retains a broker-accepted
candidate until the fill journal matches its exact order id."""

import json
import time

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.approvals import Approval, _require_account_key
from nakagai_edge.signing import args_hash


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
        approval: dict, result: dict) -> None:
    """Freeze a broker-accepted intent until its exact order id fills."""
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
        "submitted_at": time.time(),
    }
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
                notional: float = 0.0, candidate_id: str = "",
                intent_account: str = "") -> Approval:
        # Forward `signal_id` to the platform. For a candidate it is the exact
        # binding returned with the frozen prepared order. We do not send
        # `signal` or `notional`: the edge holds no authority to vouch for
        # either, and the platform resolves them from its own store. The edge
        # independently verifies the signed grant before dispatch. `signal_id`
        # is retained locally so that verification uses the same frozen value.
        _require_account_key(account_key)
        if candidate_id:
            if not isinstance(signal_id, str) or not signal_id.strip():
                raise ValueError("candidate signal_id must be a nonempty string")
            if not isinstance(intent_account, str) or not intent_account.strip():
                raise ValueError("candidate account must be a nonempty string")
        out = self.client.enqueue_approval(
            connector_id, tool, args, signal_id, candidate_id)
        doc = intents(self.state)
        doc[out["approval_id"]] = {
            "connector_id": connector_id, "tool": tool, "args": args,
            "args_hash": args_hash(args), "created_at": time.time(),
            "signal_id": signal_id, "candidate_id": candidate_id,
            "account": intent_account}
        _write_intents(self.state, doc)
        return Approval(id=out["approval_id"], account_key=account_key,
                        connector_id=connector_id,
                        tool=tool, args=args, status=out["status"],
                        agent_id=self.agent_id, requested_by=requested_by,
                        created_at=time.time(), expires_at=out["expires_at"],
                        signal_id=signal_id, candidate_id=candidate_id)

    def get(self, account_key: str, approval_id: str) -> Approval | None:
        from nakagai_edge.edge.client import EdgeClientError
        _require_account_key(account_key)
        try:
            return _to_approval(account_key, self.client.get_approval(approval_id))
        except EdgeClientError:
            return None
