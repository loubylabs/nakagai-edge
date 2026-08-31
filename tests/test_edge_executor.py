"""The edge's write path: intent → platform grant → artifact verification →
execute → report. Tampered artifacts and stale grants must never execute."""

import json
import time

import httpx
import pytest

pytest.importorskip("cryptography")

from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import PlatformClient
from nakagai_edge.edge.candidate import (
    candidate_entries_armed,
    pending_candidate_outcomes,
)
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
CANDIDATE_ARGS = {
    "account_number": "463605220", "symbol": "AAPL", "side": "buy",
    "type": "limit", "quantity": "3", "limit_price": "211.25",
    "stop_price": "207.0", "time_in_force": "day",
}


def _bundle():
    return {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
            "connectors": {"connectors": []},
            "mandate": {}, "strategy_configs": {},
            "signing_public_key": PUB}


def _artifact(approval_id, *, args=ARGS, agent_id="ag1", expires_in=900,
              candidate_id="", account=""):
    payload = build_payload(
        approval_id=approval_id, agent_id=agent_id, connector_id="demo",
        tool="place_order", args=args,
        account_arg_names=["account_number"], ttl_s=expires_in,
        candidate_id=candidate_id)
    if account:
        payload["account"] = account
    return sign_artifact(PRIV, payload)


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
    reports, outcomes, alerts = [], [], []
    failures_left = execution_failures

    def handler(req):
        if req.url.path == "/api/agent/approvals" and req.method == "POST":
            return httpx.Response(200, json={"ok": True, "approval_id": "a1",
                                             "status": "pending",
                                             "expires_at": time.time() + 900})
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
        if req.url.path.endswith("/outcome"):
            payload = json.loads(req.content)
            outcomes.append(payload)
            return httpx.Response(200, json={
                "candidate_id": req.url.path.split("/")[-2],
                "mechanical_status": payload["mechanical_status"],
                "mechanical_reason": payload["mechanical_reason"],
                "approval_id": payload["approval_id"],
            })
        if req.url.path == "/api/agent/checkin":
            alerts.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True})
        if req.url.path == "/api/agent/audit":
            return httpx.Response(200, json={"ok": True, "accepted": 1})
        return httpx.Response(404, json={"detail": "?"})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(handler))
    queue = RemoteApprovalQueue(client, state, "ag1")
    client.candidate_outcomes = outcomes
    client.candidate_alerts = alerts
    return state, client, queue, reports


class CandidateHub(FakeHub):
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


async def test_enqueue_records_local_intent(tmp_path):
    state, client, queue, _ = _setup(tmp_path)
    rec = queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    assert rec.id == "a1" and rec.status == "pending"
    assert intents(state)["a1"]["tool"] == "place_order"


async def test_granted_artifact_executes_and_reports(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"))
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    n = await poll_once(hub, state, client, EdgeAudit(state))
    assert n == 1
    assert hub.calls and hub.calls[0][3].get("approved") is True
    assert reports and reports[0]["ok"] is True
    assert intents(state) == {}


async def test_tampered_args_hash_never_executes(tmp_path):
    bad = _artifact("a1", args={"account_number": "463605220", "qty": 100})
    state, client, queue, reports = _setup(tmp_path, artifact=bad)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == []
    assert reports and reports[0]["ok"] is False


async def test_candidate_grant_requires_the_same_candidate(tmp_path):
    artifact = _artifact("a1", candidate_id="candidate-other")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900,
                  signal_id="signal-1", candidate_id="candidate-1",
                  intent_account="463605220")
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert client.candidate_outcomes[0]["mechanical_status"] == "blocked"
    assert "candidate_id mismatch" in client.candidate_outcomes[0]["mechanical_reason"]


async def test_candidate_grant_requires_the_same_frozen_account(tmp_path):
    artifact = _artifact("a1", candidate_id="candidate-1",
                         account="other-account")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900,
                  signal_id="signal-1", candidate_id="candidate-1",
                  intent_account="463605220")
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert "account mismatch" in reports[0]["error"]
    assert client.candidate_outcomes[0]["mechanical_status"] == "blocked"
    assert "account mismatch" in client.candidate_outcomes[0]["mechanical_reason"]


async def test_candidate_grant_requires_the_same_frozen_signal(tmp_path):
    artifact = _artifact(
        "a1", candidate_id="candidate-1", account="463605220")
    state, client, queue, reports = _setup(
        tmp_path,
        artifact=artifact,
        record_overrides={"signal_id": "other-signal"},
    )
    queue.enqueue(
        "ag1", "demo", "place_order", ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert client.candidate_outcomes[0]["mechanical_status"] == "blocked"
    assert "signal_id mismatch" in client.candidate_outcomes[0]["mechanical_reason"]


@pytest.mark.parametrize(
    ("local_signal", "remote_signal"),
    [
        (None, None),
        ("signal-1", None),
        ("", ""),
        ("signal-1", ""),
        (" signal-1 ", "signal-1"),
        (" signal-1 ", " signal-1 "),
    ],
    ids=["missing-local", "missing-remote", "empty-local", "empty-remote",
         "whitespace-exact-mismatch", "both-sides-untrimmed"],
)
async def test_candidate_signal_binding_fails_closed_at_persisted_verification(
        tmp_path, local_signal, remote_signal):
    artifact = _artifact(
        "a1", candidate_id="candidate-1", account="463605220")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue(
        "ag1", "demo", "place_order", ARGS, ttl_s=900,
        signal_id=(local_signal if isinstance(local_signal, str)
                   and local_signal.strip() else "signal-1"),
        candidate_id="candidate-1", intent_account="463605220",
    )
    stored = intents(state)
    if local_signal is None:
        stored["a1"].pop("signal_id")
    else:
        stored["a1"]["signal_id"] = local_signal
    state._write_private(state.intents_path, stored)
    original_get = client.get_approval

    def approval_without_assumed_signal(approval_id):
        record = original_get(approval_id)
        if remote_signal is None:
            record.pop("signal_id")
        else:
            record["signal_id"] = remote_signal
        return record

    client.get_approval = approval_without_assumed_signal

    def platform_unavailable(*args, **kwargs):
        raise RuntimeError("platform unavailable")

    client.report_candidate_outcome = platform_unavailable
    hub = FakeHub()

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    pending = pending_candidate_outcomes(state)["candidate-1"]
    assert pending["mechanical_status"] == "blocked"
    assert "signal_id" in pending["mechanical_reason"]
    assert pending["approval_id"] == "a1"


async def test_lost_candidate_approval_never_contacts_the_broker(tmp_path):
    state, client, queue, reports = _setup(tmp_path, grant_status="denied")
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900,
                  signal_id="signal-1", candidate_id="candidate-1",
                  intent_account="463605220")
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports == []
    assert intents(state) == {}


async def test_disarmed_brake_refuses_a_candidate_grant_before_broker_contact(tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm

    artifact = _artifact("a1", candidate_id="candidate-1")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900,
                  signal_id="signal-1", candidate_id="candidate-1",
                  intent_account="463605220")
    set_local_disarm(state, all_positions=True)
    hub = FakeHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    assert "brake" in reports[0]["error"]
    assert client.candidate_outcomes[0]["mechanical_status"] == "blocked"
    assert "brake" in client.candidate_outcomes[0]["mechanical_reason"]


async def test_signed_brake_refusal_retries_candidate_outcome_until_acknowledged(
        tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm

    artifact = _artifact("a1", candidate_id="candidate-1")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue(
        "ag1", "demo", "place_order", ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    set_local_disarm(state, all_positions=True)
    hub = FakeHub()
    original = client.report_candidate_outcome
    attempts = 0

    def fail_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("platform unavailable")
        return original(*args, **kwargs)

    client.report_candidate_outcome = fail_once

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1
    assert hub.calls == []
    assert reports and reports[0]["ok"] is False
    pending = pending_candidate_outcomes(state)
    assert pending["candidate-1"]["mechanical_status"] == "blocked"
    assert "brake" in pending["candidate-1"]["mechanical_reason"]
    assert intents(state) == {}

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 0
    assert attempts == 2
    assert pending_candidate_outcomes(state) == {}
    assert hub.calls == []


async def test_preexisting_position_does_not_turn_resting_candidate_limit_into_fill(
        tmp_path):
    trace = []
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    warrant = {"expires_at": time.time() + 3600, "signature": "warrant"}
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS,
            "signal_id": "signal-1",
            "exit_warrant": warrant,
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub(trace=trace)
    original_report = client.report_execution

    def report(*args, **kwargs):
        trace.append("execution")
        return original_report(*args, **kwargs)

    original_outcome = client.report_candidate_outcome

    def outcome(*args, **kwargs):
        trace.append("outcome")
        return original_outcome(*args, **kwargs)

    client.report_execution = report
    client.report_candidate_outcome = outcome

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert trace == ["broker", "execution"]
    assert reports == [{
        "ok": True, "result": {"is_error": False, "data": {"order_id": "42"}},
        "error": "", "outcome_unknown": False, "order_id": "42",
    }]
    assert load_supervision(state) == {}
    assert client.candidate_outcomes == []
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["broker_order_id"] == "42"
    assert candidate_entries_armed(state) is True


async def test_503_report_recovers_after_restart_without_duplicate_broker_submit(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact, execution_failures=1,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )

    hub = CandidateHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    await poll_once(hub, state, client, EdgeAudit(state))

    submitted = intents(state)["a1"]
    assert reports and reports[0]["ok"] is True
    assert submitted["phase"] == "submitted"
    assert submitted["broker_order_id"] == "42"
    assert submitted["pending_execution_report"] == reports[0]
    assert candidate_entries_armed(state) is True
    assert client.candidate_outcomes == []
    assert client.candidate_alerts == []

    fill = {
        "order_id": "42", "symbol": "AAPL", "side": "buy",
        "quantity": 3.0, "status": "filled", "fill_price": 211.31,
    }
    assert await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [fill],
    ) == 0
    assert load_supervision(state) == {}

    state._write_private(state.candidate_outcomes_path, {
        "older-candidate": {
            "mechanical_status": "blocked",
            "mechanical_reason": "older durable outcome",
            "approval_id": "older-approval",
            "urgent": False,
            "outcome_unknown": False,
        },
    })
    recovery_trace = []
    original_execution = client.report_execution
    original_outcome = client.report_candidate_outcome

    def traced_execution(*args, **kwargs):
        recovery_trace.append("execution-report")
        return original_execution(*args, **kwargs)

    def traced_outcome(*args, **kwargs):
        recovery_trace.append("candidate-outcome")
        return original_outcome(*args, **kwargs)

    client.report_execution = traced_execution
    client.report_candidate_outcome = traced_outcome

    restarted = EdgeState(tmp_path)
    await poll_once(hub, restarted, client, EdgeAudit(restarted))

    assert recovery_trace[:2] == ["execution-report", "candidate-outcome"]
    assert len(hub.calls) == 1
    assert len(reports) == 2
    assert reports[1] == reports[0]
    recovered = intents(restarted)["a1"]
    expected = dict(submitted)
    expected.pop("pending_execution_report")
    assert recovered == expected
    assert await reconcile_submitted_fills(
        hub, restarted, client, EdgeAudit(restarted), hub.spec("demo"),
        "463605220", [fill],
    ) == 1
    assert load_supervision(restarted)["a1"]["state"] == "armed"
    assert intents(restarted) == {}


async def test_lost_execution_ack_retries_the_exact_report_only(tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
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
    hub = CandidateHub()

    await poll_once(hub, state, client, EdgeAudit(state))
    pending = intents(state)["a1"]["pending_execution_report"]
    restarted = EdgeState(tmp_path)
    await poll_once(hub, restarted, client, EdgeAudit(restarted))

    assert len(hub.calls) == 1
    assert reports == [pending, pending]
    assert "pending_execution_report" not in intents(state)["a1"]
    assert candidate_entries_armed(state) is True
    assert client.candidate_outcomes == []


async def test_nonexact_execution_ack_keeps_the_report_pending(tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={"args": CANDIDATE_ARGS, "signal_id": "signal-1"},
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    original_report = client.report_execution
    acknowledge_exactly = False

    def report(*args, **kwargs):
        if not acknowledge_exactly:
            return {"ok": True, "status": "submitted"}
        return original_report(*args, **kwargs)

    client.report_execution = report
    hub = CandidateHub()
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


async def test_missing_order_id_preserves_urgent_submitted_truth_and_disarms(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={"args": CANDIDATE_ARGS, "signal_id": "signal-1"},
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )

    class MissingOrderIdHub(CandidateHub):
        async def call(self, connector_id, tool, args, **kw):
            if kw.get("approved"):
                self.calls.append((connector_id, tool, args, kw))
                return {"is_error": False, "data": {"accepted": True}}
            return await super().call(connector_id, tool, args, **kw)

    hub = MissingOrderIdHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reason = (
        "candidate broker result has no declared order id; "
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
    assert candidate_entries_armed(state) is False
    assert client.candidate_outcomes == []
    assert client.candidate_alerts
    assert await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{
            "order_id": "", "symbol": "AAPL", "side": "buy",
            "quantity": 3.0, "status": "filled", "fill_price": 211.31,
        }],
    ) == 0
    assert intents(state)["a1"] == submitted


async def test_later_matching_fill_establishes_supervision_and_completes_candidate(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    warrant = {"expires_at": time.time() + 3600, "signature": "warrant"}
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": warrant,
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    await poll_once(hub, state, client, EdgeAudit(state))

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 1
    stored = load_supervision(state)["a1"]
    assert stored["state"] == "armed"
    assert stored["entry_price"] == 211.31
    assert intents(state) == {}
    assert client.candidate_outcomes == []


async def test_matching_fill_renews_missing_warrant_before_candidate_completion(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={"args": CANDIDATE_ARGS, "signal_id": "signal-1"},
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
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
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 1
    assert asks == [[{
        "position_id": "a1", "connector_id": "demo",
        "account": "463605220", "symbol": "AAPL",
        "confirmed_qty": 3.0, "signal_id": "signal-1",
    }]]
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}
    assert client.candidate_outcomes == []


@pytest.mark.parametrize("retry_status", ["granted", "executed"])
async def test_candidate_accept_retry_preserves_submitted_fill_fence(
        tmp_path, retry_status):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS,
            "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600,
                "signature": "warrant",
            },
        },
    )
    enqueue = {
        "signal_id": "signal-1",
        "candidate_id": "candidate-1",
        "intent_account": "463605220",
    }
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        **enqueue,
    )
    hub = CandidateHub(positions=[{"symbol": "AAPL", "quantity": "3"}])
    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1
    before = intents(state)["a1"]
    assert before["phase"] == "submitted"
    assert before["broker_order_id"] == "42"
    assert len(hub.calls) == 1
    original_enqueue = client.enqueue_approval

    def retry_existing(*args, **kwargs):
        response = original_enqueue(*args, **kwargs)
        return {**response, "status": retry_status}

    client.enqueue_approval = retry_existing

    retried = queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        **enqueue,
    )

    assert retried.status == retry_status
    assert intents(state)["a1"] == before
    assert await poll_once(hub, state, client, EdgeAudit(state)) == 0
    assert len(hub.calls) == 1
    assert intents(state)["a1"]["broker_order_id"] == "42"

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 1
    assert len(hub.calls) == 1
    assert hub.calls[0][3]["approved"] is True
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}


async def test_ordinary_approved_entry_uses_the_same_exact_fill_supervision_path(
        tmp_path):
    artifact = _artifact("a1", args=CANDIDATE_ARGS, account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        intent_account="463605220",
    )
    hub = CandidateHub()

    await poll_once(hub, state, client, EdgeAudit(state))

    assert load_supervision(state) == {}
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["broker_order_id"] == "42"

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 1
    assert load_supervision(state)["a1"]["state"] == "armed"
    assert intents(state) == {}
    assert client.candidate_outcomes == []


async def test_mismatched_fill_order_id_cannot_complete_candidate(tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "other-order", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 0
    assert load_supervision(state) == {}
    assert intents(state)["a1"]["phase"] == "submitted"
    assert client.candidate_outcomes == []


async def test_declared_terminal_order_preserves_submitted_broker_truth(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS, "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reconciled = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "cancelled"}],
    )

    assert reconciled == 1
    assert intents(state) == {}
    assert load_supervision(state) == {}
    assert candidate_entries_armed(state) is True
    assert client.candidate_outcomes == []


async def test_undeclared_order_status_keeps_submitted_candidate_durable(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, _reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={"args": CANDIDATE_ARGS},
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub()
    await poll_once(hub, state, client, EdgeAudit(state))

    reconciled = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "broker-specific-done"}],
    )

    assert reconciled == 0
    assert intents(state)["a1"]["phase"] == "submitted"
    assert load_supervision(state) == {}
    assert client.candidate_outcomes == []


async def test_supervision_failure_after_actual_fill_disarms_and_alerts_owner(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(
        tmp_path, artifact=artifact,
        record_overrides={
            "args": CANDIDATE_ARGS,
            "signal_id": "signal-1",
            "exit_warrant": {
                "expires_at": time.time() + 3600, "signature": "warrant"},
        },
    )
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub(position_error=RuntimeError("positions unreadable"))

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1
    assert reports[0]["ok"] is True

    matched = await reconcile_submitted_fills(
        hub, state, client, EdgeAudit(state), hub.spec("demo"), "463605220",
        [{"order_id": "42", "symbol": "AAPL", "side": "buy",
          "quantity": 3.0, "status": "filled", "fill_price": 211.31}],
    )

    assert matched == 1
    assert len(hub.calls) == 1
    assert client.candidate_outcomes == [{
        "mechanical_status": "submitted",
        "mechanical_reason": client.candidate_alerts[0]["note"],
        "approval_id": "a1", "urgent": True, "outcome_unknown": True,
    }]
    assert client.candidate_alerts == [{
        "status": "alert",
        "note": client.candidate_outcomes[0]["mechanical_reason"],
        "account_equity": None,
        "day_pnl": None,
    }]
    assert candidate_entries_armed(state) is False
    from nakagai_edge.edge.brake import armed
    assert armed(state) is True
    assert intents(state) == {}


async def test_candidate_outcome_unknown_keeps_the_existing_fence_and_alerts(
        tmp_path):
    artifact = _artifact(
        "a1", args=CANDIDATE_ARGS, candidate_id="candidate-1",
        account="463605220")
    state, client, queue, reports = _setup(tmp_path, artifact=artifact)
    queue.enqueue(
        "ag1", "demo", "place_order", CANDIDATE_ARGS, ttl_s=900,
        signal_id="signal-1", candidate_id="candidate-1",
        intent_account="463605220",
    )
    hub = CandidateHub()

    async def uncertain(*args, **kwargs):
        raise RuntimeError("broker timed out")

    hub.call = uncertain

    assert await poll_once(hub, state, client, EdgeAudit(state)) == 1

    assert reports[0]["outcome_unknown"] is True
    assert client.candidate_outcomes[0]["urgent"] is True
    assert client.candidate_outcomes[0]["outcome_unknown"] is True
    assert candidate_entries_armed(state) is False


async def test_expired_artifact_never_executes(tmp_path):
    stale = _artifact("a1", expires_in=-10)
    state, client, queue, reports = _setup(tmp_path, artifact=stale)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    await poll_once(hub, state, client, EdgeAudit(state))
    assert hub.calls == [] and reports[0]["ok"] is False


async def test_granted_intent_deferred_on_stale_policy(tmp_path, monkeypatch):
    # expires_in is generous so the artifact itself outlives the TTL patch below;
    # this test targets the policy-freshness gate, not artifact expiry.
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1", expires_in=5000))
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
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
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
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
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
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
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
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
    import yaml

    from nakagai_edge.hub import ConnectorHub
    from tests.fixtures.echo_mcp import mcp as echo_server
    from tests.fixtures.inproc import connect_to

    state, client, queue, _ = _setup(tmp_path)

    (state.root / "config").mkdir(parents=True, exist_ok=True)
    (state.root / "config" / "connectors.yaml").write_text(yaml.safe_dump({"connectors": [{
        "id": "echo", "name": "Echo", "kind": "mcp-http", "role": "broker",
        "url": "https://echo.test/mcp", "enabled": True,
        "guardrails": {"read_only_tools": ["get_*", "echo", "search"],
                       "allow_writes": True,
                       "approvals": {"require_for": ["place_*"]}},
    }]}))

    hub = ConnectorHub(state.root, connect=connect_to(echo_server), approvals=queue)
    out = await hub.call("echo", "place_equity_order",
                         {"symbol": "SPY", "account_number": "1"},
                         account_key="ag1",
                         signal_id="abc123")
    assert out["approval_required"] is True and out["approval_id"] == "a1"
    await hub.aclose()


def test_remote_enqueue_carries_signal_id_onto_the_returned_record(tmp_path):
    """RemoteApprovalQueue forwards `signal_id` to the platform (the platform
    recomputes signal/notional from it) but never sends its own signal/notional,
    and the local record it hands back is honest about what the agent claimed."""
    state, client, queue, _ = _setup(tmp_path)
    rec = queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900,
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
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900, signal_id="abc123",
                  signal={"strategy": "ict"}, notional=1184.0)
    assert posted["signal_id"] == "abc123"
    assert "signal" not in posted and "notional" not in posted


async def test_full_edge_loop_closes_on_the_owners_tap(tmp_path, monkeypatch):
    """One real candidate reaches one supervised broker position."""
    pytest.importorskip("nakagai_platform")

    import pandas as pd
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from mcp.server.mcpserver import MCPServer

    from nakagai_platform.api.app import create_app
    from nakagai_platform.candidate_store import CandidateStore
    from nakagai_edge.hub import ConnectorHub
    from nakagai_edge.capability import resolve
    from nakagai_edge.edge.candidate import CandidateWakeScope
    from nakagai_edge.edge.runtime import _prepared_order
    from tests.fixtures.inproc import connect_to
    from nakagai_edge.signing import generate_keypair, public_key_for
    from nakagai_platform.api.signal_store import SignalStore

    NOW = pd.Timestamp("2026-07-13T15:00:00+00:00")   # Monday 08:00 LA, inside RTH
    monkeypatch.setattr(pd.Timestamp, "now", staticmethod(lambda tz=None: NOW))
    monkeypatch.setattr(time, "time", lambda: NOW.timestamp())

    # A fresh id every run, not the literal "abc123": the approvals queue is a
    # real Postgres table now (workspace_mandate requires DATABASE_URL to grant
    # anything, so this test runs on Postgres too), and autoapprove's
    # already-executed fence (nakagai_platform/gateway/envelope.py) is keyed on
    # signal_id alone, checked against every approval ever citing it, not just
    # this test's own. A hardcoded id would auto-decline on the second run
    # against the same database, exactly like a real signal_id (a sha256 of
    # symbol|strategy|direction|bar_ts) is never reused, ever.
    import uuid

    signal_id = f"test-{uuid.uuid4().hex}"

    priv, _ = generate_keypair()
    monkeypatch.setenv("NAKAGAI_API_TOKEN", "api-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_TOKEN", "approver-secret")
    monkeypatch.setenv("NAKAGAI_APPROVER_EMAILS", "chris@nakag.ai")
    monkeypatch.setenv("NAKAGAI_APPROVAL_SIGNING_KEY", priv)

    canonical_order = {
        "symbol": "NVDA", "side": "buy", "order_type": "limit",
        "quantity": 10, "limit_price": 118.40, "stop_price": 116.10,
        "time_in_force": "gtc", "account": "463605220",
    }
    connector = {
        **ROBINHOOD_CONNECTOR,
        "id": "broker",
        "url": "https://example.test/mcp",
    }

    # ---- platform: copilot mandate, a seeded signal, the signing key ----
    plat = tmp_path / "platform"
    (plat / "config").mkdir(parents=True)
    (plat / "config" / "scan.yaml").write_text(
        'expressions:\n  swing: true\nrth:\n  start: "06:45"\n'
        '  end: "13:00"\n  tz: America/Los_Angeles\n')
    # The mandate is a per-workspace Postgres row now, not config/mandate.yaml.
    # autoapprove.py resolves the account off the approval record's
    # `requested_by`, which agent_routes.py sets to the agent's owner email
    # (the X-User the approver headers below carry, chris@nakag.ai). Seed the
    # SAME account here, through MandateStore, or the enqueue below resolves
    # to a workspace-less context that reads the observer default and never
    # arms anything.
    from nakagai_platform.api.db import Database
    from nakagai_platform.api.tenancy import resolve_workspace_for_email
    from nakagai_platform.mandate_store import MandateStore

    mandate_db = Database.from_env()
    mandate_db.workspace_id("chris-nakag", "chris@nakag.ai")
    # Candidate execution needs Pro. Grant it before resolving the context,
    # because WorkspaceContext carries the plan it read at resolution time.
    mandate_db.grant_plan("chris@nakag.ai", plan="pro",
                          reason="edge candidate fixture",
                          granted_by="edge-tests")
    mandate_ctx = resolve_workspace_for_email(mandate_db, "chris@nakag.ai")
    mandate_store = MandateStore(plat, mandate_db, mandate_ctx)
    doc = mandate_store.load()
    doc["preset"] = "copilot"
    mandate_store.save(doc)

    from nakagai_platform.api.connectors import ConnectorStore

    # Connector and signal setup share the same durable database the running
    # platform reads. A file registry is no longer an input to PlatformHub.
    ConnectorStore(mandate_db).add(connector)
    SignalStore(mandate_db).append([{
        "id": signal_id, "bar_ts": "2026-07-13T14:55:00+00:00",
        "detected_ts": "2026-07-13T14:55:00+00:00", "symbol": "NVDA",
        "strategy": "ict", "direction": "LONG", "timeframe": "15m",
        "entry": 118.4, "stop": 116.1,
        "target": 124.0,
        "stale_data": False, "expressions": {"swing": {"instrument": "shares"}}}])

    platform = TestClient(create_app(plat, with_mcp=False))
    approver = {"Authorization": "Bearer api-secret", "X-User": "chris@nakag.ai",
                "X-Approver-Token": "approver-secret"}
    code = platform.post("/api/agents", json={"name": "edge"},
                         headers=approver).json()["code"]
    paired = platform.post("/api/agents/pair", json={"code": code}).json()
    agent_id, token = paired["agent_id"], paired["token"]

    store = CandidateStore(mandate_db)
    from datetime import datetime, timedelta, timezone

    candidate = store.create(
        workspace_id=str(mandate_ctx.wid),
        agent_id=agent_id,
        signal_id=signal_id,
        policy_version=1,
        signal={
            "id": signal_id, "play": "ict", "symbol": "NVDA",
            "direction": "LONG", "timeframe": "15m",
            "bar_ts": "2026-07-13T14:55:00+00:00",
            "entry": 118.4, "stop": 116.1, "reward_to_risk": 2.4,
            "confluence": 1, "independence": 1,
            "stacked_timeframes": ["15m"], "provenance": "live",
        },
        score=10.0,
        score_breakdown={
            "confluence": 1.0, "stacked": 1.0, "rr": 1.0,
            "proven_pf": 1.0, "proven": True,
        },
        play_title="ICT",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    reserved = store.reserve_accept(
        candidate["id"], agent_id=agent_id,
        rationale="The bounded setup is valid.",
    )
    connector_spec = ConnectorStore(mandate_db).specs()["broker"]
    tool, broker_args = resolve(
        "place_order", connector_spec.capabilities["place_order"],
        canonical_order,
    )
    store.prepare(
        candidate["id"], agent_id=agent_id,
        preparation_token=reserved["preparation_token"],
        connector_id="broker", account="463605220", tool=tool,
        canonical_order=canonical_order, args=broker_args,
        args_hash=args_hash(broker_args), compiler_version=1,
    )

    # ---- edge: its state + a bundle carrying the platform's public key ----
    state = EdgeState(tmp_path / "edge")
    state.save_agent("https://api.test", agent_id, token)
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": [connector]},
                         "signing_public_key": public_key_for(priv)}, "v1")

    lose_execution_ack = True

    def forward(req):
        nonlocal lose_execution_ack
        headers = {"Authorization": f"Bearer {token}"}
        ct = req.headers.get("content-type")
        if ct:
            headers["content-type"] = ct
        resp = platform.request(req.method, req.url.path, content=req.content,
                                headers=headers)
        if req.url.path.endswith("/execution") and lose_execution_ack:
            lose_execution_ack = False
            raise httpx.ReadError("execution acknowledgement was lost")
        return httpx.Response(resp.status_code, content=resp.content,
                              headers={"content-type": "application/json"})

    edge_client = PlatformClient("https://api.test", token,
                                 transport=httpx.MockTransport(forward))
    queue = RemoteApprovalQueue(edge_client, state, agent_id)

    # ---- edge broker: a real downstream over the SDK memory transport ----
    # Stood up BEFORE the enqueue so the pre-tap assertions below can look at a
    # broker that was genuinely reachable and still took no order, rather than
    # at one that did not exist yet.
    placed: list = []
    positions: list = []
    broker = MCPServer("broker")

    @broker.tool()
    def place_equity_order(
            symbol: str, side: str, type: str, quantity: str,
            limit_price: str, stop_price: str, time_in_force: str,
            account_number: str) -> dict:
        placed.append((symbol, side, quantity))
        positions[:] = [{"symbol": symbol, "quantity": quantity}]
        return {"order_id": "broker-order-1", "average_price": limit_price}

    @broker.tool()
    def get_equity_positions(account_number: str) -> dict:
        return {"data": {"positions": positions}}

    edge_hub = ConnectorHub(state.root, connect=connect_to(broker), approvals=queue)
    edge_hub.account_key = agent_id

    CandidateWakeScope(state).begin({
        "seq": 1, "kind": "execution_candidate", "response_required": True,
        "candidate_id": candidate["id"], "expires_at": time.time() + 300,
    })
    accepted = edge_client.accept_candidate(
        candidate["id"], "The bounded setup is valid.")
    prepared = _prepared_order(
        candidate["id"], accepted, load_specs({"connectors": [connector]})["broker"],
        "463605220",
    )
    submitted = await edge_hub.call(
        prepared["connector_id"], prepared["tool"], prepared["args"],
        account_key=agent_id, signal_id=prepared["signal_id"],
        candidate_id=candidate["id"], intent_account=prepared["account_id"],
        require_approval=True,
    )
    rec = queue.get(agent_id, submitted["approval_id"])
    assert rec is not None
    assert rec.status == "pending"      # declined to a human tap, not auto-granted
    assert rec.candidate_id == candidate["id"]
    assert rec.signal_id == signal_id

    from nakagai_platform.api.db import require_installed_database
    from nakagai_platform.gateway import get_hub

    hub = get_hub(plat, require_installed_database())
    undecided = hub.approvals.get(str(mandate_ctx.wid), rec.id)
    assert undecided.status == "pending"
    assert undecided.artifact is None   # nothing was signed, so nothing is executable
    assert undecided.decided_by == ""

    # A decline is not a denial: the intent stays on the edge, waiting. Polling
    # it is the real proof that no order reaches the broker without a human,
    # because it drives the same code path that would have executed a grant.
    assert await poll_once(edge_hub, state, edge_client, EdgeAudit(state)) == 0
    assert placed == []                 # the broker took nothing
    assert rec.id in intents(state)     # still armed for the tap below

    # ---- half two: the owner taps approve, and the edge closes the loop ----
    # The real owner-action route, with the approver headers that
    # assert_owner_action requires: the agent's own bearer token can never reach
    # it. For an edge-origin approval this SIGNS an artifact rather than
    # executing anything, because the broker credentials live out here.
    decided = platform.post(f"/api/approvals/{rec.id}",
                            json={"decision": "approve"}, headers=approver)
    assert decided.status_code == 200
    assert decided.json()["approval"]["status"] == "granted"

    n = await poll_once(edge_hub, state, edge_client, EdgeAudit(state))
    assert n == 1
    assert placed == [("NVDA", "buy", "10")]
    assert intents(state)[rec.id]["broker_order_id"] == "broker-order-1"
    assert "pending_execution_report" in intents(state)[rec.id]
    assert store.decision(candidate["id"])["mechanical_status"] == "submitted"

    # A repeated accepted decision receives the same frozen order and approval.
    # The local retry must preserve the submitted fence rather than reopening
    # broker dispatch after the platform has already recorded execution.
    submitted_before_retry = intents(state)[rec.id]
    accepted_retry = edge_client.accept_candidate(
        candidate["id"], "The bounded setup is valid.")
    assert accepted_retry == {
        "candidate_id": candidate["id"],
        "decision": "accepted",
        "mechanical_status": "submitted",
        "mechanical_reason": "",
        "approval_id": rec.id,
    }
    assert store.decision(candidate["id"])["mechanical_status"] == "submitted"
    assert intents(state)[rec.id] == submitted_before_retry
    assert placed == [("NVDA", "buy", "10")]
    assert await poll_once(
        edge_hub, state, edge_client, EdgeAudit(state)) == 1
    assert "pending_execution_report" not in intents(state)[rec.id]
    assert store.decision(candidate["id"])["mechanical_status"] == "submitted"
    assert placed == [("NVDA", "buy", "10")]

    matched = await reconcile_submitted_fills(
        edge_hub, state, edge_client, EdgeAudit(state),
        edge_hub.spec("broker"), "463605220",
        [{"order_id": "broker-order-1", "symbol": "NVDA", "side": "buy",
          "quantity": 10.0, "status": "filled", "fill_price": 118.4}],
    )
    assert matched == 1
    decision_after_fill = store.decision(candidate["id"])
    assert decision_after_fill["mechanical_status"] == "submitted", {
        "decision": decision_after_fill,
        "supervision": load_supervision(state),
        "outcomes": pending_candidate_outcomes(state),
    }
    assert intents(state) == {}
    supervised = load_supervision(state)[rec.id]
    assert supervised["signal_id"] == signal_id
    assert supervised["state"] == "armed"

    # the platform's own record: decided by the owner, then executed by the edge.
    # assert_owner_action returns the X-User it validated, lowercased, and that
    # is what hub.decide() stamps on the record as decided_by.
    plat_rec = hub.approvals.get(str(mandate_ctx.wid), rec.id)
    assert plat_rec.decided_by == "chris@nakag.ai"
    assert plat_rec.status == "executed"
    assert plat_rec.candidate_id == candidate["id"]
    assert plat_rec.signal_id == signal_id
    assert store.decision(candidate["id"])["mechanical_status"] == "submitted"
    assert pending_candidate_outcomes(state) == {}
    await edge_hub.aclose()


async def test_nonjson_execution_response_cannot_rearm_intent(tmp_path):
    state, client, queue, reports = _setup(tmp_path, artifact=_artifact("a1"),
                                           broken_execution=True)
    queue.enqueue("ag1", "demo", "place_order", ARGS, ttl_s=900)
    hub = FakeHub()
    audit = EdgeAudit(state)
    await poll_once(hub, state, client, audit)     # must not raise
    assert len(hub.calls) == 1
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["pending_execution_report"] == reports[0]
    await poll_once(hub, state, client, audit)
    assert len(hub.calls) == 1                      # no duplicate broker order
    assert reports[1] == reports[0]
