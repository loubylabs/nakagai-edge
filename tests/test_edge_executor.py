"""The edge's write path: intent → platform grant → artifact verification →
execute → report. Tampered artifacts and stale grants must never execute."""

import json
import time

import httpx
import pytest

pytest.importorskip("cryptography")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.executor import poll_once
from nakagai_edge.edge.remote import RemoteApprovalQueue, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import apply_bundle
from nakagai_edge.signing import build_payload, generate_keypair, sign_artifact

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


PRIV, PUB = generate_keypair()
ARGS = {"account_number": "463605220", "qty": 1}


def _bundle():
    return {"bundle_version": "v1", "connectors": {"connectors": []},
            "watchlist": [], "mandate": {}, "strategy_configs": {},
            "signing_public_key": PUB}


def _artifact(approval_id, *, args=ARGS, agent_id="ag1", expires_in=900):
    return sign_artifact(PRIV, build_payload(
        approval_id=approval_id, agent_id=agent_id, connector_id="demo",
        tool="place_order", args=args,
        account_arg_names=["account_number"], ttl_s=expires_in))


class FakeHub:
    def __init__(self):
        self.calls = []

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, args, kw))
        return {"is_error": False, "data": {"order_id": "42"}}


def _setup(tmp_path, grant_status="granted", artifact=None,
           broken_execution=False):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, _bundle(), "v1")
    reports = []

    def handler(req):
        if req.url.path == "/api/agent/approvals" and req.method == "POST":
            return httpx.Response(200, json={"ok": True, "approval_id": "a1",
                                             "status": "pending",
                                             "expires_at": time.time() + 900})
        if req.url.path == "/api/agent/approvals/a1" and req.method == "GET":
            return httpx.Response(200, json={
                "id": "a1", "status": grant_status, "connector_id": "demo",
                "tool": "place_order", "args": ARGS, "agent_id": "ag1",
                "artifact": artifact, "expires_at": time.time() + 900})
        if req.url.path.endswith("/execution"):
            reports.append(json.loads(req.content))
            if broken_execution:
                return httpx.Response(200, text="<html>proxy</html>")
            return httpx.Response(200, json={"ok": True, "status": "executed"})
        if req.url.path == "/api/agent/audit":
            return httpx.Response(200, json={"ok": True, "accepted": 1})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(handler))
    queue = RemoteApprovalQueue(client, state, "ag1")
    return state, client, queue, reports


async def test_enqueue_records_local_intent(tmp_path):
    state, client, queue, _ = _setup(tmp_path)
    rec = queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    assert rec.id == "a1" and rec.status == "pending"
    assert intents(state)["a1"]["tool"] == "place_order"


async def test_granted_artifact_executes_and_reports(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"))
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    n = await poll_once(hub, state, client, EdgeAudit(state))
    assert n == 1
    assert hub.calls and hub.calls[0][3].get("approved") is True
    assert reports and reports[0]["ok"] is True
    assert intents(state) == {}


async def test_tampered_args_hash_never_executes(tmp_path):
    bad = _artifact("a1", args={"account_number": "463605220", "qty": 100})
    state, client, queue, reports = _setup(tmp_path, artifact=bad)
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == []
    assert reports and reports[0]["ok"] is False


async def test_expired_artifact_never_executes(tmp_path):
    stale = _artifact("a1", expires_in=-10)
    state, client, queue, reports = _setup(tmp_path, artifact=stale)
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == [] and reports[0]["ok"] is False


async def test_granted_intent_deferred_on_stale_policy(tmp_path, monkeypatch):
    # expires_in is generous so the artifact itself outlives the TTL patch below;
    # this test targets the policy-freshness gate, not artifact expiry.
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1", expires_in=5000))
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past 900s policy TTL
    n = await poll_once(hub, state, client, EdgeAudit(state))
    assert n == 0
    assert hub.calls == []
    assert reports == []
    assert "a1" in intents(state)  # not dropped; re-armed on next sync


async def test_denied_intent_is_dropped(tmp_path):
    state, client, queue, reports = _setup(tmp_path, grant_status="denied")
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    await poll_once(FakeHub(), state, client, EdgeAudit(state))
    assert intents(state) == {} and reports == []


def test_audit_scrub_drops_secretish_keys(tmp_path):
    audit = EdgeAudit(EdgeState(tmp_path))
    out = audit.scrub({"Authorization": "Bearer x", "nested": {"api_token": "t"},
                       "qty": 1})
    assert out == {"nested": {}, "qty": 1}


def test_scrub_recurses_into_lists(tmp_path):
    audit = EdgeAudit(EdgeState(tmp_path))
    out = audit.scrub({"orders": [{"api_token": "t", "qty": 1}], "note": ["plain"],
                       "batches": [[{"password": "x", "side": "buy"}]]})
    assert out == {"orders": [{"qty": 1}], "note": ["plain"],
                   "batches": [[{"side": "buy"}]]}


async def test_success_report_survives_audit_failure(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"))
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    audit = EdgeAudit(state)

    def _boom(*a, **kw):
        raise OSError("disk full")

    audit.record = _boom
    n = await poll_once(hub, state, client, audit)
    assert n == 1
    assert hub.calls and hub.calls[0][3].get("approved") is True
    assert reports and reports[0]["ok"] is True


async def test_intent_dropped_when_bookkeeping_raises(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"))
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    audit = EdgeAudit(state)

    def _boom(*a, **kw):
        raise RuntimeError("not even an OSError")

    audit.record = _boom
    await poll_once(hub, state, client, audit)     # must not raise
    assert intents(state) == {}                     # no second execution possible
    assert reports and reports[0]["ok"] is True


async def test_edge_enqueue_still_works_with_a_cited_signal_id(tmp_path):
    """ConnectorHub.call() now passes signal_id/signal/notional into every
    `queue.enqueue(...)` call, on every rung, including the edge, where the
    queue is a RemoteApprovalQueue, not the file-backed or Postgres one. If its
    signature were not updated too, this would raise
    `TypeError: enqueue() got an unexpected keyword argument 'signal_id'` and
    every edge order would die. It must not."""
    import contextlib

    import yaml
    from mcp.shared.memory import create_connected_server_and_client_session

    from nakagai_edge.hub import ConnectorHub
    from tests.fixtures.echo_mcp import mcp as echo_server

    state, client, queue, _ = _setup(tmp_path)

    (state.root / "config").mkdir(parents=True, exist_ok=True)
    (state.root / "config" / "connectors.yaml").write_text(yaml.safe_dump({"connectors": [{
        "id": "echo", "name": "Echo", "kind": "mcp-http", "role": "broker",
        "url": "https://echo.test/mcp", "enabled": True,
        "guardrails": {"read_only_tools": ["get_*", "echo", "search"],
                       "allow_writes": True,
                       "approvals": {"require_for": ["place_*"]}},
    }]}))

    async def connect(spec):
        @contextlib.asynccontextmanager
        async def _open():
            async with create_connected_server_and_client_session(
                    echo_server._mcp_server) as session:
                yield session
        return _open()

    hub = ConnectorHub(state.root, connect=connect, approvals=queue)
    out = await hub.call("echo", "place_equity_order",
                         {"symbol": "SPY", "account_number": "1"},
                         signal_id="abc123")
    assert out["approval_required"] is True and out["approval_id"] == "a1"
    await hub.aclose()


def test_remote_enqueue_carries_signal_id_onto_the_returned_record(tmp_path):
    """RemoteApprovalQueue forwards `signal_id` to the platform (the platform
    recomputes signal/notional from it) but never sends its own signal/notional,
    and the local record it hands back is honest about what the agent claimed."""
    state, client, queue, _ = _setup(tmp_path)
    rec = queue.enqueue("demo", "place_order", ARGS, ttl_s=900,
                        signal_id="abc123", signal={"strategy": "ict"},
                        notional=1184.0)
    assert rec.id == "a1" and rec.signal_id == "abc123"


def test_signal_id_travels_from_edge_to_platform(tmp_path):
    """RemoteApprovalQueue.enqueue(signal_id=...) makes the client POST a body
    carrying that id; it is what the platform checks against the autopilot
    envelope. signal/notional are NOT sent: the edge holds no authority to vouch
    for a signal, so the platform recomputes both from the id against its own
    store."""
    posted = {}

    def handler(req):
        if req.url.path == "/api/agent/approvals" and req.method == "POST":
            posted.update(json.loads(req.content))
            return httpx.Response(200, json={"ok": True, "approval_id": "a1",
                                             "status": "pending",
                                             "expires_at": time.time() + 900})
        return httpx.Response(404, json={"detail": "?"})

    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(handler))
    queue = RemoteApprovalQueue(client, state, "ag1")
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900, signal_id="abc123",
                  signal={"strategy": "ict"}, notional=1184.0)
    assert posted["signal_id"] == "abc123"
    assert "signal" not in posted and "notional" not in posted


async def test_full_edge_autopilot_loop_closes(tmp_path, monkeypatch):
    """The whole point of the task, end to end: the edge posts an intent citing a
    signal; the PLATFORM (holding the mandate + signing key) decides it is inside
    the armed autopilot envelope and returns a SIGNED grant; the edge's poll_once
    independently verifies that artifact and executes the trade at a real
    (memory-transport) broker.

    Nothing security-critical is stubbed. The platform really auto-grants and
    signs (real Ed25519); poll_once really verifies signature + args_hash +
    agent_id + expiry; the broker call really runs over the MCP ClientSession.
    Only the HTTP hop is a MockTransport, and it forwards to the real platform
    FastAPI app, so this is a genuine enqueue → grant → poll, not a canned reply.
    The mandate decides (`decided_by == "mandate:autopilot"`); the edge only
    executes an artifact it verified; the agent's token never grants its order.
    """
    pytest.importorskip("nakagai")

    import contextlib

    import pandas as pd
    import yaml as _yaml

    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from mcp.server.fastmcp import FastMCP
    from mcp.shared.memory import create_connected_server_and_client_session

    from nakagai.api.app import create_app
    from nakagai_edge.hub import ConnectorHub
    from nakagai_edge.signing import generate_keypair, public_key_for
    from nakagai.scan.signal import append_signals

    NOW = pd.Timestamp("2026-07-13T15:00:00+00:00")   # Monday 08:00 LA, inside RTH
    monkeypatch.setattr(pd.Timestamp, "now", staticmethod(lambda tz=None: NOW))
    monkeypatch.setattr(time, "time", lambda: NOW.timestamp())

    priv, pub = generate_keypair()
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", "chris@x.com")
    monkeypatch.setenv("NAKAGAI_APPROVAL_SIGNING_KEY", priv)

    order = {"symbol": "NVDA", "side": "buy", "quantity": 10,
             "limit_price": 118.40, "stop_price": 116.10}
    order_shape = {"symbol_keys": ["symbol"], "side_keys": ["side"],
                   "quantity_keys": ["quantity"], "price_keys": ["limit_price"],
                   "stop_keys": ["stop_price"],
                   "stock_tools": ["place_equity_order"]}

    # ---- platform: autopilot armed, a seeded signal, the signing key ----
    plat = tmp_path / "platform"
    (plat / "config").mkdir(parents=True)
    (plat / "config" / "scan.yaml").write_text(
        'expressions:\n  swing: true\nrth:\n  start: "06:45"\n'
        '  end: "13:00"\n  tz: America/Los_Angeles\n')
    (plat / "config" / "watchlist.yaml").write_text("symbols: [NVDA]\n")
    (plat / "config" / "mandate.yaml").write_text(_yaml.safe_dump({
        "preset": "autopilot",
        # Not a loss-dial test: turned off explicitly so the on-by-default
        # breaker doesn't decline this end-to-end grant on a missing equity
        # report instead of proving the enqueue -> grant -> poll loop closes.
        "overrides": {"rails": {"autopilot": {"daily_loss_pct_disarm": 0.0}}},
        "kill_switch": {"engaged": False, "engaged_at": None},
        "autopilot_state": {"armed": True, "disarmed_at": None,
                          "disarmed_reason": ""}}))
    (plat / "config" / "connectors.yaml").write_text(_yaml.safe_dump({"connectors": [{
        "id": "broker", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp", "enabled": True,
        "guardrails": {"allow_writes": True, "read_only_tools": ["get_*"],
                       "approvals": {"require_for": ["place_*"], "ttl_s": 900},
                       "order_shape": order_shape}}]}))
    append_signals(plat / "signals", [{
        "id": "abc123", "bar_ts": "2026-07-13T14:55:00+00:00",
        "detected_ts": "2026-07-13T14:55:00+00:00", "symbol": "NVDA",
        "strategy": "ict", "direction": "LONG", "entry": 118.4, "stop": 116.1,
        "target": 124.0, "evidence": {"status": "validated", "oos_windows": 6},
        "stale_data": False, "expressions": {"swing": {"instrument": "shares"}}}])

    platform = TestClient(create_app(plat, with_mcp=False))
    approver = {"Authorization": "Bearer api-secret", "X-User": "chris@x.com",
                "X-Approver-Token": "approver-secret"}
    code = platform.post("/api/agents", json={"name": "edge"},
                         headers=approver).json()["code"]
    paired = platform.post("/api/agents/pair", json={"code": code}).json()
    agent_id, token = paired["agent_id"], paired["token"]

    # ---- edge: its state + a bundle carrying the platform's public key ----
    state = EdgeState(tmp_path / "edge")
    state.save_agent("https://api.test", agent_id, token)
    apply_bundle(state, {"bundle_version": "v1", "connectors": {"connectors": []},
                         "signing_public_key": public_key_for(priv)}, "v1")

    def forward(req):
        headers = {"Authorization": f"Bearer {token}"}
        ct = req.headers.get("content-type")
        if ct:
            headers["content-type"] = ct
        resp = platform.request(req.method, req.url.path, content=req.content,
                                headers=headers)
        return httpx.Response(resp.status_code, content=resp.content,
                              headers={"content-type": "application/json"})

    edge_client = PlatformClient("https://api.test", token,
                                 transport=httpx.MockTransport(forward))
    queue = RemoteApprovalQueue(edge_client, state, agent_id)
    rec = queue.enqueue("broker", "place_equity_order", order, ttl_s=900,
                        signal_id="abc123")
    assert rec.status == "granted"          # the platform decided, in the envelope

    # ---- edge broker: a real downstream over the SDK memory transport ----
    placed: list = []
    broker = FastMCP("broker")

    @broker.tool()
    def place_equity_order(symbol: str, side: str, quantity: int,
                           limit_price: float, stop_price: float = 0.0) -> str:
        placed.append((symbol, side, quantity))
        return f"PLACED {side} {quantity} {symbol} @ {limit_price}"

    async def connect(spec):
        @contextlib.asynccontextmanager
        async def _open():
            async with create_connected_server_and_client_session(
                    broker._mcp_server) as session:
                yield session
        return _open()

    # The edge holds the broker credentials and executes. apply_bundle synced an
    # empty registry into state.root/config; write the broker entry its hub dials.
    (state.root / "config" / "connectors.yaml").write_text(_yaml.safe_dump({"connectors": [{
        "id": "broker", "kind": "mcp-http", "role": "broker",
        "url": "https://example.test/mcp", "enabled": True,
        "guardrails": {"allow_writes": True, "read_only_tools": ["get_*"],
                       "approvals": {"require_for": ["place_*"], "ttl_s": 900}}}]}))
    edge_hub = ConnectorHub(state.root, connect=connect, approvals=queue)

    n = await poll_once(edge_hub, state, edge_client, EdgeAudit(state))
    assert n == 1
    assert placed == [("NVDA", "buy", 10)]      # the trade really executed
    assert intents(state) == {}                 # nothing left to re-execute

    # the platform's own record: decided by the mandate, then executed by the edge
    from nakagai.gateway import get_hub
    plat_rec = get_hub(plat).approvals.get(rec.id)
    assert plat_rec.decided_by == "mandate:autopilot"
    assert plat_rec.status == "executed"
    await edge_hub.aclose()


async def test_nonjson_execution_response_cannot_rearm_intent(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"),
                                           broken_execution=True)
    queue.enqueue("demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    audit = EdgeAudit(state)
    await poll_once(hub, state, client, audit)     # must not raise
    assert len(hub.calls) == 1
    assert intents(state) == {}
    await poll_once(hub, state, client, audit)
    assert len(hub.calls) == 1                      # no duplicate broker order
