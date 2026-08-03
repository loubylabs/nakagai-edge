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

        class R:
            returncode = 0
            stdout = stderr = ""

        return R()

    monkeypatch.setattr(clients.subprocess, "run", fake_run)
    claude = next(c for c in clients.KNOWN_CLIENTS if c.key == "claude-code")
    claude.register("http://127.0.0.1:8330/mcp/")

    assert "--scope" in seen["cmd"] and "user" in seen["cmd"]
    assert "project" not in seen["cmd"]


def test_undetected_client_is_simply_absent(monkeypatch):
    monkeypatch.setattr(clients.shutil, "which", lambda _: None)
    assert clients.detected() == []
