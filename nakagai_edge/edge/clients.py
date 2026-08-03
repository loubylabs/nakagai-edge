"""Which agent clients we can wire up for the owner, and how.

The contract is the URL, not this table. Any MCP client can consume
`http://127.0.0.1:8330/mcp/` by hand, and `snippet()` exists so a client nobody
has written an entry for still takes ten seconds to connect. The table is
convenience on top, kept deliberately table-shaped so adding a client is a data
entry rather than a new code path.
"""

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

MCP_URL_TEMPLATE = "http://127.0.0.1:{port}/mcp/"


def url(port: int = 8330) -> str:
    return MCP_URL_TEMPLATE.format(port=port)


def snippet(port: int = 8330) -> str:
    """A paste-able entry for any client that speaks the common MCP config
    shape. Carries no credential: the edge holds the platform token, which is
    the entire point of routing through it."""
    return json.dumps(
        {"mcpServers": {"nakagai": {"type": "http", "url": url(port)}}}, indent=2)


@dataclass(frozen=True)
class AgentClient:
    key: str
    label: str
    detect: Callable[[], bool]
    skills_dir: Callable[[], Path]
    register: Callable[[str], bool]


def _claude_detect() -> bool:
    return shutil.which("claude") is not None


def _claude_skills_dir() -> Path:
    return Path.home() / ".claude" / "skills"


def _claude_register(server_url: str) -> bool:
    """User scope on purpose. A project-scoped .mcp.json needs an approval that
    nobody gives, which is the defect this whole change exists to remove.

    Bounded, because `setup` calls this and a hung subprocess would hang the
    onboarding path. Failing to register prints an instruction the owner can
    follow; hanging prints nothing and looks like a broken install.
    """
    try:
        proc = subprocess.run(
            ["claude", "mcp", "add", "--scope", "user", "nakagai",
             "--transport", "http", server_url],
            capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        return False
    return proc.returncode == 0


KNOWN_CLIENTS: list[AgentClient] = [
    AgentClient(key="claude-code", label="Claude Code", detect=_claude_detect,
                skills_dir=_claude_skills_dir, register=_claude_register),
]


def detected() -> list[AgentClient]:
    return [c for c in KNOWN_CLIENTS if c.detect()]
