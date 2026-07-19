"""The three portfolio triggers, wired: the timer loop, the refresh tool, and
the executor's after-a-write courtesy push. One code path (PortfolioReporter),
so these tests only prove the wiring fires it."""

import asyncio
import contextlib
import json

import httpx
import pytest

pytest.importorskip("mcp")

import nakagai_edge.edge.runtime as runtime
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.runtime import _loops, create_edge_mcp
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import apply_bundle

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


BUNDLE = {
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
    apply_bundle(s, BUNDLE, "v1")
    return s


class _Reporter:
    """Stands in for PortfolioReporter: counts pushes, returns a fixed doc."""

    def __init__(self):
        self.pushes = 0

    async def snapshot_and_push(self):
        self.pushes += 1
        return {"connectors": [{"id": "robinhood-trading", "error": "",
                                "accounts": []}]}


def _client(handler=None):
    handler = handler or (lambda r: httpx.Response(200, json={"ok": True}))
    return PlatformClient("http://platform.test", "nk_agent_t",
                          transport=httpx.MockTransport(handler))


async def _run_briefly_then_cancel(tasks, condition, tries=200):
    for _ in range(tries):
        if condition():
            break
        await asyncio.sleep(0.01)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t


async def test_the_timer_loop_pushes_on_its_own_cadence(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 3600)
    monkeypatch.setattr(runtime, "PORTFOLIO_INTERVAL_S", 0.01)
    state = _state(tmp_path)
    reporter = _Reporter()
    from nakagai_edge.hub import ConnectorHub
    hub = ConnectorHub(state.root)
    tasks = await _loops(state, hub, _client(), EdgeAudit(state), reporter)
    await _run_briefly_then_cancel(tasks, lambda: reporter.pushes >= 2)
    assert reporter.pushes >= 2


async def test_refresh_portfolio_tool_pokes_the_reporter_and_returns_the_doc(tmp_path):
    state = _state(tmp_path)
    reporter = _Reporter()
    from nakagai_edge.hub import ConnectorHub
    hub = ConnectorHub(state.root)
    mcp = create_edge_mcp(state, hub, _client(), EdgeAudit(state), reporter)

    result = await mcp.call_tool("refresh_portfolio", {})
    body = json.loads(result[0][0].text)
    assert body["connectors"][0]["id"] == "robinhood-trading"
    assert reporter.pushes == 1


async def test_a_resolved_intent_triggers_a_courtesy_push(tmp_path, monkeypatch):
    """poll_once counting ANY terminal resolution (executed or denied) is the
    trigger; a denial over-triggers harmlessly since the sweep is read-only
    and rate-limited at the reporter.

    Pins two things about the executor specifically, not about the reporter's
    total call count: it pushes once when poll_once resolves something, and
    it does NOT push again on the passes afterward where poll_once resolves
    nothing. The timer loop pushes once at boot too (a different trigger,
    by design), so a plain reporter.pushes count would conflate the two.
    _AttributingReporter tells them apart by the actual asyncio task each
    push runs under (_loops names all four loop tasks precisely so a test
    like this one can do that), not by any shared flag a wrong caller could
    also consume."""
    monkeypatch.setattr(runtime, "SYNC_INTERVAL_S", 3600)
    monkeypatch.setattr(runtime, "PORTFOLIO_INTERVAL_S", 3600)
    monkeypatch.setattr(runtime, "EXECUTOR_INTERVAL_S", 0.01)

    resolved = {"n": 1}

    async def fake_poll_once(hub, state, client, audit):
        n, resolved["n"] = resolved["n"], 0
        return n

    monkeypatch.setattr(runtime, "poll_once", fake_poll_once)

    class _AttributingReporter(_Reporter):
        """Counts pushes per calling task's name, so the executor's own
        contribution can be read separately from the timer loop's boot
        push even though both share this one reporter instance."""

        def __init__(self):
            super().__init__()
            self.pushes_by_task = {}

        async def snapshot_and_push(self):
            name = asyncio.current_task().get_name()
            self.pushes_by_task[name] = self.pushes_by_task.get(name, 0) + 1
            return await super().snapshot_and_push()

    state = _state(tmp_path)
    reporter = _AttributingReporter()
    from nakagai_edge.hub import ConnectorHub
    hub = ConnectorHub(state.root)
    tasks = await _loops(state, hub, _client(), EdgeAudit(state), reporter)

    # Wait for the executor's one resolution to land, then keep everything
    # running a while longer: if the executor's `if await poll_once(...)`
    # guard were ever removed so it pushed on every pass instead of only a
    # terminal resolution, the extra window gives that a chance to show up
    # as a second push before anything gets torn down.
    for _ in range(200):
        if reporter.pushes_by_task.get("executor", 0) >= 1:
            break
        await asyncio.sleep(0.01)
    for _ in range(30):
        await asyncio.sleep(0.01)

    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t

    # Once for the resolution, not once per pass; the timer loop's own boot
    # push (a different, legitimate trigger) is attributed to "portfolio_loop"
    # and does not count here.
    assert reporter.pushes_by_task.get("executor", 0) == 1
