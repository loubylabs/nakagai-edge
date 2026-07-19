"""Where the edge keeps its things: ~/.nakagai/edge by default.

    agent.json          platform URL + agent token (0600)
    config/connectors.yaml   synced from the bundle (secret-free)
    secrets/tokens/     broker OAuth tokens (the gateway's own layout)
    cache/bundle.json   last bundle + fetch metadata
    cache/intents.json  write intents awaiting a platform grant
    results/audit.jsonl local audit journal, shipped in batches
"""

import json
import os
import stat
from pathlib import Path


def default_root() -> Path:
    override = os.environ.get("NAKAGAI_EDGE_ROOT", "")
    return Path(override) if override else Path.home() / ".nakagai" / "edge"


class EdgeState:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def agent_path(self) -> Path:
        return self.root / "agent.json"

    @property
    def bundle_path(self) -> Path:
        return self.root / "cache" / "bundle.json"

    @property
    def meta_path(self) -> Path:
        return self.root / "cache" / "meta.json"

    @property
    def intents_path(self) -> Path:
        return self.root / "cache" / "intents.json"

    @property
    def audit_path(self) -> Path:
        return self.root / "results" / "audit.jsonl"

    def _write_private(self, path: Path, doc: dict) -> None:
        # The root itself holds secrets (agent.json, broker tokens), so keep it
        # 0700. Only ever tighten: never loosen a dir an operator widened on
        # purpose.
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if stat.S_IMODE(self.root.stat().st_mode) & (stat.S_IRWXG | stat.S_IRWXO):
            os.chmod(self.root, 0o700)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        # Open with the restrictive mode from creation: no world/group-readable
        # window between write and chmod.
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(doc, indent=2))
        os.replace(tmp, path)

    def agent(self) -> dict | None:
        if not self.agent_path.exists():
            return None
        try:
            doc = json.loads(self.agent_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        return doc if doc.get("token") else None

    def save_agent(self, platform_url: str, agent_id: str, token: str) -> None:
        self._write_private(self.agent_path, {
            "platform_url": platform_url.rstrip("/"),
            "agent_id": agent_id, "token": token})
