"""The edge's write path: intent → platform grant → artifact verification →
execute → report. Tampered artifacts and stale grants must never execute."""

import json
import time

import httpx
import pytest

pytest.importorskip("cryptography")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.executor import poll_once, reconcile_submitted_fills
from nakagai_edge.edge.remote import RemoteApprovalQueue, intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle
from nakagai_edge.edge.supervision import load as load_supervision
from nakagai_edge.config import load_specs
from nakagai_edge.signing import args_hash, build_payload, generate_keypair, sign_artifact
from tests.fixtures.alien_registry import ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


PRIV, PUB = generate_keypair()
ARGS = {"account_number": "463605220", "qty": 1}
ENTRY_ARGS = {
    "account_number": "463605220", "symbol": "AAPL", "side": "buy",
    "type": "limit", "quantity": "3", "limit_price": "211.25",
    "stop_price": "207.0", "time_in_force": "day",
}
PROPOSAL = "proposal-1"


def _bundle():
    return {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
            "connectors": {"connectors": []},
            "mandate": {}, "strategy_configs": {},
            "signing_public_key": PUB}


def _artifact(approval_id, *, args=ARGS, agent_id="ag1", expires_in=900,
              proposal_id="", account=""):
    payload = build_payload(
        approval_id=approval_id, agent_id=agent_id, connector_id="demo",
        tool="place_order", args=args,
        account_arg_names=["account_number"], ttl_s=expires_in,
        proposal_id=proposal_id)
    if account:
        payload["account"] = account
    return sign_artifact(PRIV, payload)


def _seed(state, approval_id="a1", *, args=ARGS, proposal_id="", account=""):
    """A local intent exactly as the edge holds one: adopted from a granted
    proposal when `proposal_id` is given, otherwise a plain intent."""
    doc = intents(state)
    doc[approval_id] = {
        "connector_id": "demo", "tool": "place_order", "args": args,
        "args_hash": args_hash(args), "signal_id": "",
        "account": account, "created_at": time.time(),
        **({"proposal_id": proposal_id} if proposal_id else {})}
    state._write_private(state.intents_path, doc)


def _seed_proposal(state, approval_id="a1", *, args=ENTRY_ARGS):
    _seed(state, approval_id, args=args, proposal_id=PROPOSAL,
          account="463605220")


def _proposal_artifact(approval_id="a1", *, args=ENTRY_ARGS, **kw):
    return _artifact(approval_id, args=args, proposal_id=PROPOSAL,
                     account="463605220", **kw)


class FakeHub:
    def __init__(self):
        self.calls = []
        self.account_key = "ag1"

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, args, kw))
        return {"is_error": False, "data": {"order_id": "42"}}


def _setup(tmp_path, grant_status="granted", artifact=None,
           broken_execution=False, execution_failures=0,
           record_overrides=None):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, _bundle(), "v1")
    reports, alerts = [], []
    failures_left = execution_failures

    def handler(req):
        if req.url.path == "/api/agent/approvals/a1" and req.method == "GET":
            return httpx.Response(200, json={**{
                "id": "a1", "status": grant_status, "connector_id": "demo",
                "tool": "place_order", "args": ARGS, "agent_id": "ag1",
                "artifact": artifact, "expires_at": time.time() + 900,
                "signal_id": "signal-1"},
                **(record_overrides or {})})
        if req.url.path.endswith("/execution"):
            nonlocal failures_left
            reports.append(json.loads(req.content))
            if failures_left:
                failures_left -= 1
                return httpx.Response(
                    503,
                    json={
                        "detail": (
                            "retry this exact execution report, and do not "
                            "retry the broker order"
                        )
                    },
                )
            if broken_execution:
                return httpx.Response(200, text="<html>proxy</html>")
            return httpx.Response(200, json={"ok": True, "status": "executed"})
        if req.url.path == "/api/agent/proposals/granted":
            return httpx.Response(200, json={"proposals": []})
        if req.url.path == "/api/agent/checkin":
            alerts.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True})
        if req.url.path == "/api/agent/audit":
            return httpx.Response(200, json={"ok": True, "accepted": 1})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(handler))
    queue = RemoteApprovalQueue(client, state, "ag1")
    client.alerts = alerts
    return state, client, queue, reports


class EntryHub(FakeHub):
    def __init__(self, *, positions=None, position_error=None, trace=None):
        super().__init__()
        entry = {**ROBINHOOD_CONNECTOR, "id": "demo"}
        entry["capabilities"] = {
            name: dict(value) for name, value in ROBINHOOD_CONNECTOR["capabilities"].items()
        }
        entry["capabilities"]["place_order"] = {
            **entry["capabilities"]["place_order"], "tool": "place_order"}
        self._spec = load_specs({"connectors": [entry]})["demo"]
        self.positions = ([{"symbol": "AAPL", "quantity": "3"}]
                          if positions is None else positions)
        self.position_error = position_error
        self.trace = trace if trace is not None else []

    def spec(self, connector_id):
        assert connector_id == "demo"
        return self._spec

    async def call(self, connector_id, tool, args, **kw):
        if kw.get("approved"):
            self.calls.append((connector_id, tool, args, kw))
            self.trace.append("broker")
            return {"is_error": False, "data": {"order_id": "42"}}
        assert tool == "get_equity_positions"
        self.trace.append("positions")
        if self.position_error is not None:
            raise self.position_error
        return {"is_error": False, "data": {
            "data": {"positions": self.positions}}}


WARRANT = {"exit_warrant": {"expires_at": 4_102_444_800.0, "signature": "warrant"}}
FILL = {"order_id": "42", "symbol": "AAPL", "side": "buy",
        "quantity": 3.0, "status": "filled", "fill_price": 211.31}


def test_remote_enqueue_points_at_order_proposals(tmp_path):
    """The platform has no door for a local write to wait behind: an order
    reaches the broker only as a proposal the owner approves there."""
    from nakagai_edge.hub import GuardrailDenied

    state, client, queue, _ = _setup(tmp_path)
    with pytest.raises(GuardrailDenied, match=r"\(propose_order\)"):
        queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    assert intents(state) == {}


async def test_granted_artifact_executes_and_reports(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"))
    _seed(state)
    hub = FakeHub()
    n = await poll_once(hub, state, client, EdgeAudit(state))
    assert n == 1
    assert hub.calls and hub.calls[0][3].get("approved") is True
    assert reports and reports[0]["ok"] is True
    assert intents(state) == {}


async def test_tampered_args_hash_never_executes(tmp_path):
    bad = _artifact("a1", args={"account_number": "463605220", "qty": 100})
    state, client, queue, reports = _setup(tmp_path, artifact=bad)
    _seed(state)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == []
    assert reports and reports[0]["ok"] is False


async def test_proposal_grant_requires_the_same_proposal(tmp_path):
    artifact = _artifact("a1", args=ENTRY_ARGS, proposal_id="proposal-other",
                         account="463605220")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    _seed_proposal(state)
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert "proposal_id mismatch" in reports[0]["error"]
    assert intents(state) == {}


async def test_proposal_grant_requires_the_same_frozen_account(tmp_path):
    artifact = _artifact("a1", args=ENTRY_ARGS, proposal_id=PROPOSAL,
                         account="other-account")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    _seed_proposal(state)
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert "account mismatch" in reports[0]["error"]


async def test_denied_proposal_never_contacts_the_broker(tmp_path):
    state, client, queue, reports = _setup(tmp_path, grant_status="denied")
    _seed_proposal(state)
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports == []
    assert intents(state) == {}


async def test_disarmed_brake_refuses_a_proposal_grant_before_broker_contact(tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm

    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact())
    _seed_proposal(state)
    set_local_disarm(state, all_positions=True)
    hub = FakeHub()

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert "brake" in reports[0]["error"]
    assert intents(state) == {}


async def test_preexisting_position_does_not_turn_resting_limit_into_fill(tmp_path):
    trace = []
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub(trace=trace)
    original_report = client.report_execution

    def report(*args, **kwargs):
        trace.append("execution")
        return original_report(*args, **kwargs)

    client.report_execution = report

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert trace == ["broker", "execution"]
    assert reports == [{
        "ok": True, "result": {"is_error": False, "data": {"order_id": "42"}},
        "error": "", "outcome_unknown": False, "order_id": "42",
    }]
    assert load_supervision(state) == {}
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["broker_order_id"] == "42"
    assert client.alerts == []


async def test_503_report_recovers_after_restart_without_duplicate_broker_submit(
        tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(), execution_failures=1,
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)

    hub = EntryHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    await poll_once(hub, state, client, EdgeAudit(state))

    submitted = intents(state)["a1"]
    assert reports and reports[0]["ok"] is True
    assert submitted["phase"] == "submitted"
    assert submitted["broker_order_id"] == "42"
    assert submitted["pending_execution_report"] == reports[0]
    assert client.alerts == []

    assert await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    ) == 0
    assert load_supervision(state) == {}

    restarted = EdgeState(tmp_path)
    await poll_once(hub, restarted, client, EdgeAudit(restarted))

    assert len(hub.calls) == 1
    assert len(reports) == 2
    assert reports[1] == reports[0]
    recovered = intents(restarted)["a1"]
    expected = dict(submitted)
    expected.pop("pending_execution_report")
    assert recovered == expected
    assert await reconcile_submitted_fills(
        hub, restarted, client, EdgeAudit(restarted), hub.spec("demo"),
        "463605220", [FILL],
    ) == 1
    assert load_supervision(restarted)["a1"]["state"] == "armed"
    assert intents(restarted) == {}


async def test_lost_execution_ack_retries_the_exact_report_only(tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    original_report = client.report_execution
    lose_response = True

    def report_then_lose_response(*args, **kwargs):
        nonlocal lose_response
        response = original_report(*args, **kwargs)
        if lose_response:
            lose_response = False
            raise httpx.ReadError("execution acknowledgement was lost")
        return response

    client.report_execution = report_then_lose_response
    hub = EntryHub()

    await poll_once(hub, state, client, EdgeAudit(state))
    pending = intents(state)["a1"]["pending_execution_report"]
    restarted = EdgeState(tmp_path)
    await poll_once(hub, restarted, client, EdgeAudit(restarted))

    assert len(hub.calls) == 1
    assert reports == [pending, pending]
    assert "pending_execution_report" not in intents(state)["a1"]


async def test_nonexact_execution_ack_keeps_the_report_pending(tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS},
    )
    _seed_proposal(state)
    original_report = client.report_execution
    acknowledge_exactly = False

    def report(*args, **kwargs):
        if not acknowledge_exactly:
            return {"ok": True, "status": "submitted"}
        return original_report(*args, **kwargs)

    client.report_execution = report
    hub = EntryHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    submitted = intents(state)["a1"]
    assert "pending_execution_report" in submitted
    acknowledge_exactly = True
    await poll_once(hub, state, client, EdgeAudit(state))

    recovered = intents(state)["a1"]
    expected = dict(submitted)
    expected.pop("pending_execution_report")
    assert recovered == expected
    assert len(hub.calls) == 1
    assert reports == [submitted["pending_execution_report"]]


async def test_missing_order_id_preserves_urgent_submitted_truth_and_alerts(
        tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS},
    )
    _seed_proposal(state)

    class MissingOrderIdHub(EntryHub):
        async def call(self, connector_id, tool, args, **kw):
            if kw.get("approved"):
                self.calls.append((connector_id, tool, args, kw))
                return {"is_error": False, "data": {"accepted": True}}
            return await super().call(connector_id, tool, args, **kw)

    hub = MissingOrderIdHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reason = (
        "frozen order broker result has no declared order id; "
        "fill attribution is impossible"
    )
    assert reports == [{
        "ok": True,
        "result": {"is_error": False, "data": {"accepted": True}},
        "error": reason,
        "outcome_unknown": True,
        "order_id": "",
    }]
    submitted = intents(state)["a1"]
    assert submitted["phase"] == "submitted"
    assert submitted["broker_order_id"] == ""
    assert "pending_execution_report" not in submitted
    assert [alert["note"] for alert in client.alerts] == [reason]
    assert await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{**FILL, "order_id": ""}],
    ) == 0
    assert intents(state)["a1"] == submitted


async def test_later_matching_fill_establishes_supervision(tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    await poll_once(hub, state, client, EdgeAudit(state))

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    )

    assert matched == 1
    stored = load_supervision(state)["a1"]
    assert stored["state"] == "armed"
    assert stored["entry_price"] == 211.31
    assert intents(state) == {}
    assert client.alerts == []


async def test_matching_fill_renews_missing_warrant_before_supervision(tmp_path):
    state, client, queue, _reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS},
    )
    _seed_proposal(state)
    hub = EntryHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    await poll_once(hub, state, client, EdgeAudit(state))
    asks = []

    def renew(positions):
        asks.append(positions)
        return {"warrants": {"a1": {
            "grant_id": "warrant-1", "max_qty": 3.0,
            "expires_at": time.time() + 3600,
        }}}

    client.renew_warrants = renew

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    )

    assert matched == 1
    assert asks == [[{
        "position_id": "a1", "connector_id": "demo",
        "account": "463605220", "symbol": "AAPL",
        "confirmed_qty": 3.0, "signal_id": "signal-1",
    }]]
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}


async def test_a_submitted_proposal_is_never_resubmitted_on_a_later_pass(tmp_path):
    state, client, queue, _reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1
    before = intents(state)["a1"]
    assert before["phase"] == "submitted"
    assert before["broker_order_id"] == "42"

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 0
    assert len(hub.calls) == 1
    assert intents(state)["a1"] == before

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    )

    assert matched == 1
    assert len(hub.calls) == 1
    assert hub.calls[0][3]["approved"] is True
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}


async def test_ordinary_approved_entry_uses_the_same_exact_fill_supervision_path(
        tmp_path):
    artifact = _artifact("a1", args=ENTRY_ARGS, account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed(state, args=ENTRY_ARGS, account="463605220")
    hub = EntryHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert load_supervision(state) == {}
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["broker_order_id"] == "42"

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    )

    assert matched == 1
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}


async def test_mismatched_fill_order_id_cannot_complete_a_submitted_entry(tmp_path):
    state, client, queue, _reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{**FILL, "order_id": "other-order"}],
    )

    assert matched == 0
    assert load_supervision(state) == {}
    assert intents(state)["a1"]["phase"] == "submitted"


async def test_declared_terminal_order_preserves_submitted_broker_truth(tmp_path):
    state, client, queue, _reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reconciled = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "cancelled"}],
    )

    assert reconciled == 1
    assert intents(state) == {}
    assert load_supervision(state) == {}
    assert client.alerts == []


async def test_undeclared_order_status_keeps_submitted_entry_durable(tmp_path):
    state, client, queue, _reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS},
    )
    _seed_proposal(state)
    hub = EntryHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reconciled = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "broker-specific-done"}],
    )

    assert reconciled == 0
    assert intents(state)["a1"]["phase"] == "submitted"
    assert load_supervision(state) == {}


async def test_supervision_failure_after_actual_fill_alerts_owner(tmp_path):
    state, client, queue, reports = _setup(
        tmp_path, artifact=_proposal_artifact(),
        record_overrides={"args": ENTRY_ARGS, **WARRANT},
    )
    _seed_proposal(state)
    hub = EntryHub(position_error=RuntimeError("positions unreadable"))

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1
    assert reports[0]["ok"] is True

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [FILL],
    )

    assert matched == 1
    assert len(hub.calls) == 1
    assert len(client.alerts) == 1
    assert client.alerts[0]["status"] == "alert"
    assert client.alerts[0]["note"].startswith(
        "supervision failed after reconciled broker fill")
    from nakagai_edge.edge.brake import armed
    assert armed(state) is True
    assert intents(state) == {}


async def test_an_uncertain_broker_outcome_is_reported_unknown(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_proposal_artifact())
    _seed_proposal(state)
    hub = EntryHub()

    async def uncertain(*args, **kwargs):
        raise RuntimeError("broker timed out")

    hub.call = uncertain

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert reports[0]["ok"] is False
    assert reports[0]["outcome_unknown"] is True
    assert intents(state) == {}


async def test_expired_artifact_never_executes(tmp_path):
    stale = _artifact("a1", expires_in=-10)
    state, client, queue, reports = _setup(tmp_path, artifact=stale)
    _seed(state)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == [] and reports[0]["ok"] is False


async def test_granted_intent_deferred_on_stale_policy(tmp_path, monkeypatch):
    # expires_in is generous so the artifact itself outlives the TTL patch below;
    # this test targets the policy-freshness gate, not artifact expiry.
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1", expires_in=5000))
    _seed(state)
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
    _seed(state)
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
    _seed(state)
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
    _seed(state)
    hub = FakeHub()
    audit = EdgeAudit(state)

    def _boom(*a, **kw):
        raise RuntimeError("not even an OSError")

    audit.record = _boom
    await poll_once(hub, state, client, audit)     # must not raise
    assert intents(state) == {}                     # no second execution possible
    assert reports and reports[0]["ok"] is True


async def test_nonjson_execution_response_cannot_rearm_intent(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"),
                                           broken_execution=True)
    _seed(state)
    hub = FakeHub()
    audit = EdgeAudit(state)
    await poll_once(hub, state, client, audit)     # must not raise
    assert len(hub.calls) == 1
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["pending_execution_report"] == reports[0]
    await poll_once(hub, state, client, audit)
    assert len(hub.calls) == 1                      # no duplicate broker order
    assert reports[1] == reports[0]
