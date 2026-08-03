import json

from nakagai_edge.edge import clients


def test_snippet_is_valid_json_and_carries_no_secret():
    body = clients.snippet(8330)
    parsed = json.loads(body)
    assert "127.0.0.1:8330" in body
    assert "Authorization" not in body and "Bearer" not in body
    assert parsed["mcpServers"]["nakagai"]["url"].endswith("/mcp/")


def test_registration_uses_user_scope(monkeypatch):
    """Project scope requires an approval prompt. That prompt not being given is
    the original defect, so registering at project scope would ship the bug."""
    seen = {}

    def fake_run(cmd, **kw):
        seen["cmd"] = cmd
        seen["kw"] = kw

        class R:
            returncode = 0
            stdout = stderr = ""

        return R()

    monkeypatch.setattr(clients.subprocess, "run", fake_run)
    claude = next(c for c in clients.KNOWN_CLIENTS if c.key == "claude-code")
    claude.register("http://127.0.0.1:8330/mcp/")

    assert "--scope" in seen["cmd"] and "user" in seen["cmd"]
    assert "project" not in seen["cmd"]
    assert seen["kw"].get("timeout"), "an unbounded call would hang `setup`"


def test_a_hung_or_missing_claude_is_a_failed_registration(monkeypatch):
    """`setup` calls this, so neither a hang nor a missing binary may raise out
    of it. A False prints an instruction the owner can act on; an exception
    prints a traceback and looks like a broken install."""
    claude = next(c for c in clients.KNOWN_CLIENTS if c.key == "claude-code")

    def hang(cmd, **kw):
        raise clients.subprocess.TimeoutExpired(cmd, kw.get("timeout", 0))

    monkeypatch.setattr(clients.subprocess, "run", hang)
    assert claude.register("http://127.0.0.1:8330/mcp/") is False

    def missing(cmd, **kw):
        raise FileNotFoundError("claude")

    monkeypatch.setattr(clients.subprocess, "run", missing)
    assert claude.register("http://127.0.0.1:8330/mcp/") is False


def test_undetected_client_is_simply_absent(monkeypatch):
    monkeypatch.setattr(clients.shutil, "which", lambda _: None)
    assert clients.detected() == []
