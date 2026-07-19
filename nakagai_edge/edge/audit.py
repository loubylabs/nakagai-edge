"""Local-first audit: every call, denial, execution, and error is journaled on
the edge before it ships. Offline decisions reach the platform on reconnect;
secrets never do; scrub() runs on the way into the journal."""

import json
import time

from nakagai_edge.edge.state import EdgeState

SECRET_MARKERS = ("token", "authorization", "secret", "password")


class EdgeAudit:
    def __init__(self, state: EdgeState) -> None:
        self.state = state
        self._watermark = state.audit_path.with_suffix(".shipped")

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
        event = {"ts": time.time(), "kind": kind, "connector_id": connector_id,
                 "tool": tool, "detail": self.scrub(detail or {})}
        self.state.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.state.audit_path.open("a") as f:
            f.write(json.dumps(event) + "\n")

    def _shipped(self) -> int:
        try:
            return int(self._watermark.read_text())
        except (OSError, ValueError):
            return 0

    def pending(self, limit: int = 200) -> list[dict]:
        if not self.state.audit_path.exists():
            return []
        lines = self.state.audit_path.read_text().splitlines()
        out = []
        for line in lines[self._shipped():self._shipped() + limit]:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                out.append({"ts": time.time(), "kind": "corrupt", "detail": {}})
        return out

    def mark_shipped(self, n: int) -> None:
        self._watermark.parent.mkdir(parents=True, exist_ok=True)
        self._watermark.write_text(str(self._shipped() + n))
