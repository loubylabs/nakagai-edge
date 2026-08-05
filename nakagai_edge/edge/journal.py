"""An append-only JSONL with a watermark saying how much of it has shipped.

Two journals share this: the audit trail (edge/audit.py) and the fill journal
(edge/fills.py). Both are local-first for the same reason. What happened on
this machine is written down before the platform is told, so a platform outage
costs latency and never a record, and a reconnect ships the gap rather than
skipping it.

No policy lives here. What a record looks like, and whether an unreadable line
is worth reporting upstream, are the callers' decisions: the audit trail ships
a `corrupt` marker so the owner can see a line was lost, while the fill journal
drops it, because a fill nobody can read cannot be joined to anything anyway.
This module only offers the distinction, by handing back `None` for a line it
could not parse.

The watermark counts LINES, not records, and that is what makes the `None`
above safe. A caller that drops unreadable lines must still mark them shipped,
or it re-reads them on every pass forever.
"""

import json
from pathlib import Path


class Journal:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._watermark = self.path.with_suffix(".shipped")

    def append(self, record: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    def shipped(self) -> int:
        """How many lines have been marked shipped. Zero when the watermark is
        missing or unreadable, which re-ships rather than skips: telling the
        platform something twice is recoverable, and never telling it is not."""
        try:
            return int(self._watermark.read_text())
        except (OSError, ValueError):
            return 0

    def pending(self, limit: int = 200) -> list[dict | None]:
        """The next unshipped lines, `None` for any that will not parse.

        `None` is deliberately not an empty dict: the caller has to decide what
        an unreadable line means, and a silently-empty record would let it skip
        that decision without noticing.
        """
        if not self.path.exists():
            return []
        lines = self.path.read_text().splitlines()
        start = self.shipped()
        out: list[dict | None] = []
        for line in lines[start:start + limit]:
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                out.append(None)
                continue
            out.append(parsed if isinstance(parsed, dict) else None)
        return out

    def mark_shipped(self, n: int) -> None:
        self._watermark.parent.mkdir(parents=True, exist_ok=True)
        self._watermark.write_text(str(self.shipped() + n))

    def records(self):
        """Every readable record in the journal, shipped or not.

        The fill journal rebuilds its seen-set from this at startup, which is
        why it ignores the watermark: an order already shipped must not be
        journaled a second time just because the platform has confirmed it.
        """
        if not self.path.exists():
            return
        for line in self.path.read_text().splitlines():
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                yield parsed
