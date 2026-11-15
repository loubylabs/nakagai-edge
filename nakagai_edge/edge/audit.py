"""Local-first audit: every call, denial, execution, and error is journaled on
this machine. The journal never leaves it; scrub() runs on the way in all the
same, so a secret never lands in a file the owner may share.

The append mechanics live in edge/journal.py, shared with the fill journal.
What stays here is what is genuinely audit's own: scrubbing and the event
shape. The audit journal keeps no watermark, because nothing ships it.
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

    def events(self) -> list[dict]:
        """Every readable event in the journal, oldest first. An unreadable
        line is skipped here and stays in the file, where the owner can see it."""
        return list(self._journal.records())
