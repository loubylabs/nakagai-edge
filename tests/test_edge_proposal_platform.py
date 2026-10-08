"""Proposal protocol 1 end to end, against the real platform.

An outside agent proposes an order through the platform's one door, the owner
seals it, and this edge picks the grant up, re-derives it, and submits exactly
the frozen order once. A rulebook the owner changes after the grant means the
broker is never contacted and the platform hears why.

Runs only where the platform is installed (the platform's own CI job, or an
editable source override); the edge's standalone suite skips it."""

import copy
from datetime import datetime, timezone
import uuid

import httpx
import pytest

pytest.importorskip("cryptography")
pytest.importorskip("nakagai_platform")
pytest.importorskip("fastapi")
pytest.importorskip("psycopg")

from nakagai_edge.capability import resolve  # noqa: E402
from nakagai_edge.config import load_specs  # noqa: E402
from nakagai_edge.edge.audit import EdgeAudit  # noqa: E402
from nakagai_edge.edge.client import PlatformClient  # noqa: E402
from nakagai_edge.edge.executor import poll_once, reconcile_submitted_fills  # noqa: E402
from nakagai_edge.edge.remote import intents  # noqa: E402
from nakagai_edge.edge.state import EdgeState  # noqa: E402
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle  # noqa: E402
from nakagai_edge.signing import generate_keypair, public_key_for  # noqa: E402
from tests.fixtures.alien_registry import ROBINHOOD_CONNECTOR  # noqa: E402

pytestmark = pytest.mark.anyio

ACCOUNT = "463605220"
OTHER_ACCOUNT = "555000111"
RTH = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)        # Wed 11:00 ET


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Broker:
    """The edge's broker connector: records every call, accepts as B1."""

    def __init__(self, spec):
        self._spec = spec
        self.account_key = ""
        self.calls = []
        self.positions = []

    def spec(self, connector_id):
        assert connector_id == self._spec.id
        return self._spec

    async def call(self, connector_id, tool, args, **kw):
        if kw.get("approved"):
            self.calls.append((connector_id, tool, args, kw))
            return {"is_error": False, "data": {"order_id": "B1"}}
        assert tool == "get_equity_positions"
        return {"is_error": False, "data": {"data": {"positions": self.positions}}}


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A Pro owner on copilot with proposals on and one Robinhood rulebook, a
    paired edge that has picked up once (its protocol evidence), and an API
    key proposer."""
    from fastapi.testclient import TestClient

    from nakagai_platform.api.app import create_app
    from nakagai_platform.api.connectors import ConnectorStore
    from nakagai_platform.api.db import Database
    from nakagai_platform.api.tenancy import resolve_workspace_for_email
    from nakagai_platform.candidate_approval import CandidateApprovalQueue
    from nakagai_platform.gateway.hub import PlatformHub
    from nakagai_platform.mandate_store import MandateStore
    from nakagai_platform.order_proposals.models import Proposer
    from nakagai_platform.universe import master
    from nakagai_platform.universe.base import SecurityRecord

    marker = uuid.uuid4().hex[:12]
    email = f"edge-proposal-{marker}@nakag.ai"
    priv, _ = generate_keypair()
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", email)
    monkeypatch.setenv("NAKAGAI_APPROVAL_SIGNING_KEY", priv)

    plat = tmp_path / "platform"
    (plat / "config").mkdir(parents=True)
    (plat / "config" / "scan.yaml").write_text(
        'expressions:\n  swing: true\nrth:\n  start: "06:45"\n'
        '  end: "13:00"\n  tz: America/Los_Angeles\n')
    master.save(plat, [SecurityRecord(
        symbol="AAPL", name="Apple", exchange="NASDAQ", asset_type="stock",
        status="active", tradable=True, test_issue=False,
        sources=("nasdaq_trader",), first_seen="2026-01-01",
        last_seen="2026-10-01")])

    database = Database.from_env()
    database.workspace_id(f"edge-proposal-{marker}", email)
    database.grant_plan(email, plan="pro", reason="edge proposal fixture",
                        granted_by="edge-tests")
    ctx = resolve_workspace_for_email(database, email)
    wid = str(ctx.wid)
    mandate = MandateStore(plat, database, ctx)
    doc = mandate.load()
    doc["preset"] = "copilot"
    mandate.save(doc)
    with database.pool.connection() as connection:
        connection.execute(
            "update workspaces set order_proposals_enabled = true where id = %s",
            (wid,))

    connector_id = f"rh-{marker}"
    template = copy.deepcopy(ROBINHOOD_CONNECTOR)
    template.update(id=connector_id)
    for tier in ("allow", "read"):
        template["guardrails"]["accounts"].pop(tier, None)
    connectors = ConnectorStore(database)
    connectors.add(template)
    connectors.adopt(wid, connector_id)
    connectors.confirm(wid, connector_id, allow=[ACCOUNT], read=[], ttl_s=900)

    key_id = uuid.uuid4().hex
    with database.pool.connection() as connection:
        connection.execute(
            "insert into api_keys (id, user_email, name, token_hash, last4)"
            " values (%s, %s, 'script', %s, 'abcd')",
            (key_id, email, uuid.uuid4().hex + uuid.uuid4().hex))

    platform = TestClient(create_app(plat, with_mcp=False))
    approver = {"Authorization": "Bearer api-secret", "X-User": email,
                "X-Approver-Token": "approver-secret"}
    code = platform.post("/api/agents", json={"name": "edge"},
                         headers=approver).json()["code"]
    paired = platform.post("/api/agents/pair", json={"code": code}).json()
    agent_id, token = paired["agent_id"], paired["token"]

    state = EdgeState(tmp_path / "edge")
    state.save_agent("https://api.test", agent_id, token)
    lost_acks = []

    def forward(req):
        headers = {"Authorization": f"Bearer {token}"}
        for name, value in req.headers.items():
            if name.lower() == "content-type" or name.lower().startswith("x-nakagai-"):
                headers[name] = value
        resp = platform.request(req.method, req.url.path, content=req.content,
                                headers=headers)
        if req.url.path.endswith("/execution") and lost_acks:
            lost_acks.pop()
            raise httpx.ReadError("execution acknowledgement was lost")
        return httpx.Response(resp.status_code, content=resp.content,
                              headers={"content-type": "application/json"})

    client = PlatformClient("https://api.test", token,
                            transport=httpx.MockTransport(forward))

    def sync(version):
        """Ship the account's current merged rulebook to the edge."""
        rows = connectors.account_rows(wid)
        apply_bundle(state, {"bundle_version": version,
                             "schema_version": BUNDLE_SCHEMA,
                             "connectors": {"connectors": rows},
                             "signing_public_key": public_key_for(priv)}, version)
        spec = load_specs({"connectors": rows})[connector_id]
        return spec

    spec = sync("v1")
    broker = Broker(spec)
    broker.account_key = agent_id
    hub = PlatformHub(plat, database, CandidateApprovalQueue(database))
    try:
        yield {
            "database": database, "root": plat, "hub": hub, "email": email,
            "wid": wid, "ctx": ctx, "connectors": connectors,
            "connector_id": connector_id, "agent_id": agent_id,
            "state": state, "client": client, "broker": broker, "spec": spec,
            "sync": sync, "lost_acks": lost_acks, "audit": EdgeAudit(state),
            "proposer": Proposer(kind="api_key", id=key_id, label="script ···abcd"),
        }
    finally:
        database.close()


async def _propose_and_grant(world):
    """The edge picks up once (protocol evidence), the outside agent proposes,
    and the owner seals it. Returns (proposal view, approval id)."""
    from nakagai_platform.order_proposals.models import ProposedOrder
    from nakagai_platform.order_proposals.service import OrderProposalService

    # The first pickup lists nothing and records that this edge speaks
    # protocol 1, which is what lets the platform accept a proposal at all.
    assert await poll_once(world["broker"], world["state"], world["client"],
                           world["audit"]) == 0
    service = OrderProposalService(root=world["root"], database=world["database"],
                                   hub=world["hub"], now=lambda: RTH)
    view, replayed = service.submit(
        world["ctx"], world["proposer"], uuid.uuid4().hex,
        ProposedOrder(symbol="AAPL", side="buy", order_type="limit", quantity=4,
                      limit_price=230.5, stop_price=228.0,
                      rationale="Breakout over the range."))
    assert replayed is False
    approval_id = _row(world, "select approval_id from order_proposal where id = %s",
                       view["id"])[0]
    record = await world["hub"].decide(approval_id, "approve",
                                       account_key=world["wid"],
                                       decided_by=world["email"])
    assert record.status == "granted"
    return view, approval_id


def _row(world, sql, *params):
    with world["database"].pool.connection() as connection:
        return connection.execute(sql, params).fetchone()


def _settled(world, approval_id):
    return _row(world, "select status, order_id, error, outcome_unknown"
                       " from approvals where id = %s", approval_id)


async def test_a_granted_proposal_executes_once_and_reconciles(world):
    view, approval_id = await _propose_and_grant(world)
    broker, state = world["broker"], world["state"]
    frozen_args = world["hub"].approvals.get(world["wid"], approval_id).args
    _tool, local_args = resolve(
        "place_order", world["spec"].capability("place_order"),
        {"symbol": "AAPL", "side": "buy", "order_type": "limit", "quantity": 4,
         "limit_price": 230.5, "stop_price": 228.0, "time_in_force": "gtc",
         "account": ACCOUNT})
    assert frozen_args == local_args

    # The broker accepts, and the platform's acknowledgement is lost.
    world["lost_acks"].append(True)
    assert await poll_once(broker, state, world["client"], world["audit"]) == 1
    assert len(broker.calls) == 1
    connector_id, tool, args, kw = broker.calls[0]
    assert (connector_id, tool, args) == (world["connector_id"], _tool, frozen_args)
    assert kw["approved"] is True
    assert intents(state)[approval_id]["broker_order_id"] == "B1"
    assert "pending_execution_report" in intents(state)[approval_id]

    # The grant is still listed while the report is undelivered: it is neither
    # adopted nor submitted again, and the exact report is retried.
    assert await poll_once(broker, state, world["client"], world["audit"]) == 1
    assert await poll_once(broker, state, world["client"], world["audit"]) == 0
    assert len(broker.calls) == 1
    assert "pending_execution_report" not in intents(state)[approval_id]
    assert _settled(world, approval_id) == ("executed", "B1", "", False)
    assert world["client"].granted_proposals() == []

    # list_orders reports B1 filled: the frozen intent is settled and dropped.
    broker.positions = [{"symbol": "AAPL", "quantity": "4"}]
    matched = await reconcile_submitted_fills(
        broker, state, world["client"], world["audit"], world["spec"], ACCOUNT,
        [{"order_id": "B1", "symbol": "AAPL", "side": "buy", "quantity": 4.0,
          "status": "filled", "fill_price": 230.5}])
    assert matched == 1
    assert intents(state) == {}
    assert len(broker.calls) == 1
    assert await poll_once(broker, state, world["client"], world["audit"]) == 0
    assert len(broker.calls) == 1
    assert view["id"] == _row(world, "select proposal_id from approvals where id = %s",
                              approval_id)[0]


async def test_a_rulebook_changed_after_the_grant_never_reaches_the_broker(world):
    _view, approval_id = await _propose_and_grant(world)
    # The owner points the rulebook at another account after sealing; the
    # edge syncs it before its next pass.
    world["connectors"].confirm(world["wid"], world["connector_id"],
                                allow=[OTHER_ACCOUNT], read=[], ttl_s=900)
    world["sync"]("v2")

    await poll_once(world["broker"], world["state"], world["client"], world["audit"])
    await poll_once(world["broker"], world["state"], world["client"], world["audit"])

    assert world["broker"].calls == []
    assert intents(world["state"]) == {}
    status, order_id, error, unknown = _settled(world, approval_id)
    assert (status, order_id, unknown) == ("error", "", False)
    assert error.startswith("proposal grant refused: ")
    assert "account mismatch" in error
    assert world["client"].granted_proposals() == []
