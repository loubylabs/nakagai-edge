"""Each sync cycle, the edge tells the platform what it can reach.

The edge holds the broker credential; the platform never does. What crosses
the wire is `hub.status()`'s connector list: id/name/kind/role/status/
tool_count/last_error/allow_writes/auth_mode. auth_mode is a label
(none/bearer/headers/oauth), never the token or header value itself.
"""

import asyncio
import contextlib
import json
import logging

import httpx
import pytest

pytest.importorskip("mcp")
pytest.importorskip("nakagai_platform")

from nakagai_platform.api.agent_routes import ConnectorReport
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.runtime import _loops, build_hub
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import apply_bundle
import nakagai_edge.edge.runtime as runtime

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_report_connectors_posts_the_status_list():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["auth"] = request.headers["authorization"]
        seen["body"] = request.read().decode()
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    c = PlatformClient("http://platform.test", "nk_agent_x",
                       transport=httpx.MockTransport(handler))
    out = c.report_connectors([{"id": "robinhood-trading", "status": "connected",
                                "tool_count": 47}])
    assert out["ok"] is True
    assert seen["path"] == "/api/agent/connectors"
    assert seen["auth"] == "Bearer nk_agent_x"
    assert "robinhood-trading" in seen["body"]


# ---- the syncer loop ----------------------------------------------------
#
# _loops is not exported, but it names no leading underscore convention the
# tests here need to respect: it is the one place the cadence (report after
# every sync, same 60s wake, no second interval) actually lives.

CONNECTORS_BUNDLE = {
    "bundle_version": "v1",
    "connectors": {"connectors": [
        {"id": "robinhood-trading", "name": "Robinhood", "kind": "mcp-http",
         "role": "broker", "url": "https://demo.test/mcp", "enabled": True},
    ]},
    "signing_public_key": "",
}


def _state(tmp_path):
    s = EdgeState(tmp_path)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(s, CONNECTORS_BUNDLE, "v1")
    return s


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the syncer loop, not
    the portfolio timer, so a no-op stand-in keeps their intent unchanged."""

    async def snapshot_and_push(self):
        return {"connectors": []}


async def _run_briefly_then_cancel(tasks, condition, tries=50):
    for _ in range(tries):
        if condition():
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t


async def test_syncer_reports_hub_status_connectors_each_cycle(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    seen = []

    def handler(req):
        if req.url.path == "/api/agent/connectors":
            seen.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True, "connectors": 1})
        if req.url.path == "/api/agent/bundle":
            return httpx.Response(200, json=CONNECTORS_BUNDLE, headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)

    tasks = await _loops(state, hub, client, audit, _Reporter())
    await _run_briefly_then_cancel(tasks, lambda: len(seen) >= 1)

    assert seen, "the syncer never reported connector status"
    assert seen[0] == {"connectors": hub.status()["connectors"]}
    assert seen[0]["connectors"][0]["id"] == "robinhood-trading"

    # Pin the wire contract: the edge's Connection.to_dict() and the
    # platform's ConnectorReport model must agree on shape. This catches renames
    # like tool_count -> tool_count_total that would otherwise silently drop the
    # field and leave ConnectorReport.tool_count at its default of 0. The edge
    # sends server_info as an extra key (deliberately dropped by extra="ignore"),
    # so we allow it as an exception to the field-set check.
    connector_on_wire = seen[0]["connectors"][0]
    expected_keys = set(ConnectorReport.model_fields)
    wire_keys = set(connector_on_wire)
    assert expected_keys <= wire_keys, (
        f"missing required fields: {expected_keys - wire_keys}")
    assert wire_keys <= expected_keys | {"server_info"}, (
        f"unexpected fields on wire: {wire_keys - expected_keys - {'server_info'}}")

    # No credential can ride along: only the label-shaped fields hub.status()
    # produces are on the wire, never a token, header, or auth value.
    body = json.dumps(seen[0])
    assert "token" not in body.lower()
    await hub.aclose()


async def test_syncer_survives_report_failure_and_logs_a_warning(
        tmp_path, monkeypatch, caplog):
    """A platform that is down or 500s on the report route must not stop the
    syncer loop: it is the edge's heartbeat, and status reporting rides on
    top of it as a nice-to-have, not the other way around."""
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    calls = {"bundle": 0, "connectors": 0}

    def handler(req):
        if req.url.path == "/api/agent/connectors":
            calls["connectors"] += 1
            return httpx.Response(500, json={"detail": "boom"})
        if req.url.path == "/api/agent/bundle":
            calls["bundle"] += 1
            return httpx.Response(200, json=CONNECTORS_BUNDLE, headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)

    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    tasks = await _loops(state, hub, client, audit, _Reporter())
    await _run_briefly_then_cancel(tasks, lambda: calls["connectors"] >= 2)

    # The loop kept syncing (and would keep serving MCP) despite every report
    # attempt failing.
    assert calls["connectors"] >= 2
    assert any("connector" in r.message.lower() for r in caplog.records), (
        "a failed report must be visible in the log, not silently swallowed")
    await hub.aclose()


async def test_syncer_survives_a_non_json_response_from_report_connectors(
        tmp_path, monkeypatch, caplog):
    """A malformed 200 (a captive portal, a misconfigured proxy) makes
    PlatformClient._check raise a bare ValueError from resp.json(), not
    EdgeClientError or httpx.HTTPError. If the syncer only caught those two,
    this would escape the `while True:` loop and kill it for good: every
    later sync_once call would stop running too, not just this one report."""
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    calls = {"bundle": 0, "connectors": 0}

    def handler(req):
        if req.url.path == "/api/agent/connectors":
            calls["connectors"] += 1
            return httpx.Response(200, text="<html>captive portal</html>")
        if req.url.path == "/api/agent/bundle":
            calls["bundle"] += 1
            return httpx.Response(200, json=CONNECTORS_BUNDLE, headers={"etag": "v1"})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "t", transport=httpx.MockTransport(handler))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)

    caplog.set_level(logging.WARNING, logger="nakagai.edge")
    tasks = await _loops(state, hub, client, audit, _Reporter())
    await _run_briefly_then_cancel(tasks, lambda: calls["connectors"] >= 2)

    assert calls["connectors"] >= 2, "the loop died after the first bad response"
    assert calls["bundle"] >= 2, "sync_once stopped running too, not just the report"
    assert any("connector" in r.message.lower() for r in caplog.records)
    await hub.aclose()
