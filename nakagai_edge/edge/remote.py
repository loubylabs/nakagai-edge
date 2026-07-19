"""A hub-compatible approval queue whose backing store is the platform.

ConnectorHub calls `queue.enqueue(...)` when a guardrail verdict is `approve`;
here that posts the intent to the platform (where a human sees it) and records
it locally with its args_hash. The executor later verifies the platform's
signed artifact against OUR copy of the args, so neither side can substitute
an order after the human read it."""

import json
import time

from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.approvals import Approval
from nakagai_edge.signing import args_hash


def intents(state: EdgeState) -> dict:
    if not state.intents_path.exists():
        return {}
    try:
        return json.loads(state.intents_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_intents(state: EdgeState, doc: dict) -> None:
    state.intents_path.parent.mkdir(parents=True, exist_ok=True)
    state.intents_path.write_text(json.dumps(doc, indent=2))


def drop_intent(state: EdgeState, approval_id: str) -> None:
    doc = intents(state)
    doc.pop(approval_id, None)
    _write_intents(state, doc)


def _to_approval(payload: dict) -> Approval:
    fields = {k: payload[k] for k in Approval._FIELDS if k in payload}
    fields.setdefault("id", payload.get("approval_id", ""))
    return Approval(**fields)


class RemoteApprovalQueue:
    def __init__(self, client: PlatformClient, state: EdgeState, agent_id: str) -> None:
        self.client = client
        self.state = state
        self.agent_id = agent_id

    def enqueue(self, connector_id: str, tool: str, args: dict, *, ttl_s: int,
                requested_by: str = "", workspace: str = "default",
                signal_id: str = "", signal: dict | None = None,
                notional: float = 0.0) -> Approval:
        # Forward `signal_id` to the platform: it is what the platform resolves
        # to a frozen signal + notional and checks against the autopilot envelope,
        # so an in-envelope order comes back `granted` (signed) for the edge to
        # execute. We do NOT send `signal`/`notional`: the edge holds no
        # authority to vouch for a signal it did not itself emit; the platform
        # recomputes both from the id against its own signal store. The edge
        # stays a dumb executor of a granted artifact it independently verifies
        # (see nakagai_edge/edge/executor.py); the platform decides, the edge never
        # does. `signal_id` is also carried onto the local record below so it is
        # honest about what the agent claimed.
        out = self.client.enqueue_approval(connector_id, tool, args, signal_id)
        doc = intents(self.state)
        doc[out["approval_id"]] = {
            "connector_id": connector_id, "tool": tool, "args": args,
            "args_hash": args_hash(args), "created_at": time.time()}
        _write_intents(self.state, doc)
        return Approval(id=out["approval_id"], connector_id=connector_id,
                        tool=tool, args=args, status=out["status"],
                        agent_id=self.agent_id, requested_by=requested_by,
                        created_at=time.time(), expires_at=out["expires_at"],
                        signal_id=signal_id)

    def get(self, approval_id: str, workspace: str | None = None) -> Approval | None:
        from nakagai_edge.edge.client import EdgeClientError
        try:
            return _to_approval(self.client.get_approval(approval_id))
        except EdgeClientError:
            return None
