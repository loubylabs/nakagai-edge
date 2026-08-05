"""Local-first audit: every call, denial, execution, and error is journaled on
the edge before it ships. Offline decisions reach the platform on reconnect;
secrets never do; scrub() runs on the way into the journal.

The append/watermark/ship mechanics live in edge/journal.py, shared with the
fill journal. What stays here is what is genuinely audit's own: scrubbing, the
event shape, and the decision that an unreadable line ships as a visible
`corrupt` marker rather than vanishing. A line lost out of the record of what
the agent did is exactly the thing an audit trail must not swallow.
"""

import time

from nakagai_edge.edge.journal import Journal
from nakagai_edge.edge.state import EdgeState

SECRET_MARKERS = ("token", "authorization", "secret", "password")


class EdgeAudit:
    def __init__(self, state: EdgeState) -> None:
        self.state = state
        self._journal = Journal(state.audit_path)

    def scrub(self, detail: dict) -> dict:
        out = {}
        for k, v in (detail or {}).items():
            if any(m in k.lower() for m in SECRET_MARKERS):
                continue
            out[k] = self._scrub_value(v)
        return out

    def _scrub_value(self, v):
        if isinstance(v, dict):
            return self.scrub(v)
        if isinstance(v, (list, tuple)):
            return [self._scrub_value(item) for item in v]
        return v

    def record(self, kind: str, connector_id: str = "", tool: str = "",
               detail: dict | None = None) -> None:
        self._journal.append({"ts": time.time(), "kind": kind,
                              "connector_id": connector_id, "tool": tool,
                              "detail": self.scrub(detail or {})})

    def pending(self, limit: int = 200) -> list[dict]:
        """The next unshipped events, an unreadable line standing in as a
        `corrupt` marker rather than being dropped.

        The returned count still matches the lines consumed, which is what lets
        the caller `mark_shipped(len(batch))` without stranding the bad line and
        re-reading it forever.
        """
        return [event if event is not None
                else {"ts": time.time(), "kind": "corrupt", "detail": {}}
                for event in self._journal.pending(limit)]

    def mark_shipped(self, n: int) -> None:
        self._journal.mark_shipped(n)
