"""Proposal protocol 1: an order an outside agent proposed, granted by the
owner on the platform, becomes a local intent only after the edge re-derives
every byte of it itself. Any doubt refuses before the broker is contacted."""

import copy
import json
import time

import httpx
import pytest

pytest.importorskip("cryptography")

from nakagai_edge.capability import resolve
from nakagai_edge.config import load_specs
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.client import (
    PROPOSAL_PROTOCOL_HEADER,
    PROPOSAL_PROTOCOL_VERSION,
    PlatformClient,
)
from nakagai_edge.edge.executor import poll_once
from nakagai_edge.edge.proposals import adopt_granted_proposals
from nakagai_edge.edge.remote import intents
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle
from nakagai_edge.signing import args_hash, build_payload, generate_keypair, sign_artifact
from tests.fixtures.alien_registry import ROBINHOOD_CONNECTOR

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


PRIV, PUB = generate_keypair()
ACCOUNT = "463605220"
PROPOSAL = "f" * 32
CANONICAL = {
    "symbol": "AAPL", "side": "buy", "order_type": "limit", "quantity": 3,
    "limit_price": 211.25, "stop_price": 207.0, "time_in_force": "gtc",
    "account": ACCOUNT,
}


def _connector():
    entry = copy.deepcopy(ROBINHOOD_CONNECTOR)
    entry["id"] = "demo"
    return entry


SPEC = load_specs({"connectors": [_connector()]})["demo"]
TOOL, ARGS = resolve("place_order", SPEC.capability("place_order"), CANONICAL)


def _bundle():
    return {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
            "connectors": {"connectors": [_connector()]},
            "mandate": {}, "strategy_configs": {},
            "signing_public_key": PUB}


def _item(approval_id="a1", *, args=ARGS, canonical=CANONICAL, agent_id="ag1",
          expires_in=900, proposal_id=PROPOSAL, signed_proposal_id=None,
          candidate_id="", priv=PRIV, **overrides):
    payload = build_payload(
        approval_id=approval_id, agent_id=agent_id, connector_id="demo",
        tool=TOOL, args=args, account_arg_names=["account_number"],
        ttl_s=expires_in, candidate_id=candidate_id,
        proposal_id=proposal_id if signed_proposal_id is None else signed_proposal_id)
    item = {
        "approval_id": approval_id, "proposal_id": proposal_id,
        "connector_id": "demo", "tool": TOOL, "args": args,
        "args_hash": args_hash(args), "canonical_order": canonical,
        "account": ACCOUNT, "expires_at": payload["expires_at"],
        "artifact": sign_artifact(priv, payload),
    }
    item.update(overrides)
    return item


class Platform:
    """The platform's agent routes, recording what the edge sent."""

    def __init__(self, items):
        self.items = items
        self.reports: list[tuple[str, dict]] = []
        self.pickups: list[httpx.Request] = []
        self.report_status = 200

    def handler(self, req):
        path = req.url.path
        if path == "/api/agent/proposals/granted" and req.method == "GET":
            self.pickups.append(req)
            return httpx.Response(200, json={"proposals": self.items})
        if path.endswith("/execution") and req.method == "POST":
            self.reports.append((path.split("/")[-2], json.loads(req.content)))
            return httpx.Response(self.report_status,
                                  json={"ok": True, "status": "executed"})
        if path.startswith("/api/agent/approvals/") and req.method == "GET":
            approval_id = path.split("/")[-1]
            item = next(i for i in self.items if i["approval_id"] == approval_id)
            return httpx.Response(200, json={
                "id": approval_id, "status": "granted", "connector_id": "demo",
                "tool": item["tool"], "args": item["args"], "agent_id": "ag1",
                "artifact": item["artifact"], "expires_at": item["expires_at"],
                "signal_id": ""})
        if path == "/api/agent/checkin":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"detail": "?"})


class Hub:
    def __init__(self, result=None):
        self.calls = []
        self.account_key = "ag1"
        self.result = result or {"is_error": False, "data": {"order_id": "42"}}

    def spec(self, connector_id):
        assert connector_id == "demo"
        return SPEC

    async def call(self, connector_id, tool, args, **kw):
        self.calls.append((connector_id, tool, args, kw))
        return self.result


def _setup(tmp_path, items):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, _bundle(), "v1")
    platform = Platform(items)
    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(platform.handler))
    return state, client, platform, EdgeAudit(state)


def _refused(state, platform, hub=None):
    assert intents(state) == {}
    assert len(platform.reports) == 1
    approval_id, report = platform.reports[0]
    assert approval_id == "a1"
    assert report["ok"] is False
    assert report["outcome_unknown"] is False
    assert report["error"].startswith("proposal grant refused: ")
    if hub is not None:
        assert hub.calls == []
    return report["error"]


def test_payload_always_carries_proposal_id():
    p = build_payload(approval_id="a", agent_id="g", connector_id="c", tool="t",
                      args={"x": 1}, account_arg_names=[], ttl_s=60, now=0.0)
    assert p["proposal_id"] == ""
    assert p["candidate_id"] == ""
    p = build_payload(approval_id="a", agent_id="g", connector_id="c", tool="t",
                      args={"x": 1}, account_arg_names=[], ttl_s=60, now=0.0,
                      proposal_id="f" * 32)
    assert p["proposal_id"] == "f" * 32


def test_adopt_writes_intent_for_a_valid_grant(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])

    assert adopt_granted_proposals(Hub(), state, client, audit) == 1

    intent = intents(state)["a1"]
    assert intent["proposal_id"] == PROPOSAL
    assert intent["candidate_id"] == ""
    assert intent["signal_id"] == ""
    assert intent["connector_id"] == "demo"
    assert intent["tool"] == TOOL
    assert intent["args"] == ARGS
    assert intent["args_hash"] == args_hash(ARGS)
    assert intent["account"] == ACCOUNT
    assert platform.reports == []


async def test_adopted_grant_executes_once_through_the_unchanged_executor(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])
    hub = Hub()

    await poll_once(hub, state, client, audit)

    assert len(hub.calls) == 1
    connector_id, tool, args, kw = hub.calls[0]
    assert (connector_id, tool, args) == ("demo", TOOL, ARGS)
    assert kw["approved"] is True
    assert platform.reports == [("a1", {
        "ok": True, "result": hub.result, "error": "",
        "outcome_unknown": False, "order_id": "42"})]
    # Durable under its broker id until the fill journal matches it.
    assert intents(state)["a1"]["phase"] == "submitted"
    assert intents(state)["a1"]["broker_order_id"] == "42"


def test_adopt_refuses_bad_signature(tmp_path):
    other_priv, _ = generate_keypair()
    state, client, platform, audit = _setup(tmp_path, [_item(priv=other_priv)])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "signature verification failed" in _refused(state, platform)


def test_adopt_refuses_a_tampered_artifact(tmp_path):
    item = _item()
    item["artifact"]["args_hash"] = "0" * 64
    state, client, platform, audit = _setup(tmp_path, [item])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "signature verification failed" in _refused(state, platform)


async def test_adopt_refuses_capability_drift(tmp_path):
    # The platform's args do not match what THIS edge's map makes of the
    # canonical order: ten times the size, consistently hashed and signed.
    drifted = {**ARGS, "quantity": "30"}
    state, client, platform, audit = _setup(tmp_path, [_item(args=drifted)])
    hub = Hub()

    await poll_once(hub, state, client, audit)

    error = _refused(state, platform, hub)
    assert "args do not match the local capability map" in error


async def test_adopt_refuses_a_canonical_order_the_local_map_rejects(tmp_path):
    canonical = {**CANONICAL, "quantity": 2.5}
    state, client, platform, audit = _setup(
        tmp_path, [_item(canonical=canonical)])
    hub = Hub()
    await poll_once(hub, state, client, audit)
    assert "failed the local capability map" in _refused(state, platform, hub)


@pytest.mark.parametrize("change, why", [
    ({"side": "sell"}, "only a buy limit"),
    ({"order_type": "market", "limit_price": None, "stop_price": None},
     "only a buy limit"),
    ({"stop_price": 212.0}, "stop below the limit"),
    ({"stop_price": None}, "stop below the limit"),
])
def test_adopt_refuses_anything_but_a_stopped_buy_limit(tmp_path, change, why):
    canonical = {**CANONICAL, **change}
    state, client, platform, audit = _setup(tmp_path, [_item(canonical=canonical)])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert why in _refused(state, platform)


def test_adopt_refuses_wrong_agent(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item(agent_id="ag2")])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "agent_id mismatch" in _refused(state, platform)


def test_adopt_refuses_expired_artifact(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item(expires_in=-10)])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "artifact expired" in _refused(state, platform)


def test_adopt_refuses_a_signed_proposal_id_mismatch(tmp_path):
    state, client, platform, audit = _setup(
        tmp_path, [_item(signed_proposal_id="e" * 32)])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "proposal_id mismatch" in _refused(state, platform)


def test_adopt_refuses_a_grant_naming_a_candidate(tmp_path):
    state, client, platform, audit = _setup(
        tmp_path, [_item(candidate_id="candidate-1")])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "cannot name a candidate" in _refused(state, platform)


def test_adopt_refuses_an_extra_or_missing_field(tmp_path):
    item = _item()
    item["notional"] = 633.75
    state, client, platform, audit = _setup(tmp_path, [item])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "missing or additional fields" in _refused(state, platform)


def test_adopt_refuses_another_account(tmp_path):
    canonical = {**CANONICAL, "account": "999"}
    _, other_args = resolve("place_order", SPEC.capability("place_order"), canonical)
    item = _item(args=other_args, canonical=canonical)
    state, client, platform, audit = _setup(tmp_path, [item])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "account mismatch" in _refused(state, platform)


async def test_adopt_refuses_when_brake_disarmed(tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm

    state, client, platform, audit = _setup(tmp_path, [_item()])
    set_local_disarm(state, all_positions=True)
    hub = Hub()

    await poll_once(hub, state, client, audit)

    assert "brake is disarmed" in _refused(state, platform, hub)


async def test_brake_disarmed_after_adoption_refuses_before_the_broker(tmp_path):
    from nakagai_edge.edge.brake import set_local_disarm

    state, client, platform, audit = _setup(tmp_path, [_item()])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 1
    set_local_disarm(state, all_positions=True)
    hub = Hub()

    await poll_once(hub, state, client, audit)

    assert hub.calls == []
    assert intents(state) == {}
    assert platform.reports[0][1]["ok"] is False
    assert "brake is disarmed" in platform.reports[0][1]["error"]


def test_adopt_refuses_stale_policy(tmp_path, monkeypatch):
    state, client, platform, audit = _setup(tmp_path, [_item(expires_in=5000)])
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 1000)  # past the policy TTL

    assert adopt_granted_proposals(Hub(), state, client, audit) == 0

    assert intents(state) == {}
    assert platform.reports == []
    assert platform.pickups == []      # retried next pass, nothing reported


def test_adopt_refuses_without_a_sole_order_capable_broker(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])
    second = {**_connector(), "id": "demo-2"}
    bundle = {**_bundle(), "connectors": {"connectors": [_connector(), second]}}
    apply_bundle(state, bundle, "v2")
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert "no sole order-capable broker" in _refused(state, platform)


def test_adopt_is_idempotent(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 1
    before = intents(state)
    assert adopt_granted_proposals(Hub(), state, client, audit) == 0
    assert intents(state) == before
    assert platform.reports == []


async def test_a_lost_report_never_adopts_the_same_grant_twice(tmp_path):
    # The broker raises after the guardrails, the outcome is unknown, and the
    # failure report never reaches the platform: the approval is still listed
    # as granted next pass. It must not be adopted, and so submitted, again.
    state, client, platform, audit = _setup(tmp_path, [_item()])
    platform.report_status = 503

    class Raising(Hub):
        async def call(self, connector_id, tool, args, **kw):
            self.calls.append((connector_id, tool, args, kw))
            raise httpx.ReadTimeout("broker went quiet")

    hub = Raising()
    await poll_once(hub, state, client, audit)
    assert len(hub.calls) == 1
    assert intents(state) == {}

    await poll_once(hub, state, client, audit)
    await poll_once(hub, state, client, audit)

    assert len(hub.calls) == 1
    assert intents(state) == {}


def test_an_unreadable_ledger_adopts_nothing(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])
    state.proposals_adopted_path.parent.mkdir(parents=True, exist_ok=True)
    state.proposals_adopted_path.write_text("{torn")

    assert adopt_granted_proposals(Hub(), state, client, audit) == 0

    assert intents(state) == {}
    assert platform.reports == []


async def test_verify_rejects_proposal_id_mismatch(tmp_path):
    # The local intent and the platform's current artifact disagree about the
    # proposal: a re-signed grant for another proposal cannot execute this one.
    state, client, platform, audit = _setup(tmp_path, [_item()])
    assert adopt_granted_proposals(Hub(), state, client, audit) == 1
    platform.items = [_item(proposal_id="e" * 32)]
    hub = Hub()

    await poll_once(hub, state, client, audit)

    assert hub.calls == []
    assert intents(state) == {}
    approval_id, report = platform.reports[0]
    assert approval_id == "a1" and report["ok"] is False
    assert report["error"] == "artifact verification failed: proposal_id mismatch"


async def test_proposal_acceptance_without_order_id_is_outcome_unknown(tmp_path):
    state, client, platform, audit = _setup(tmp_path, [_item()])
    hub = Hub(result={"is_error": False, "data": {"accepted": True}})

    await poll_once(hub, state, client, audit)

    assert len(hub.calls) == 1
    assert platform.reports == [("a1", {
        "ok": True, "result": {"is_error": False, "data": {"accepted": True}},
        "error": ("frozen order broker result has no declared order id; "
                  "fill attribution is impossible"),
        "outcome_unknown": True, "order_id": ""})]
    submitted = intents(state)["a1"]
    assert submitted["phase"] == "submitted"
    assert submitted["broker_order_id"] == ""
    assert "pending_execution_report" not in submitted


def test_granted_proposals_sends_protocol_header(tmp_path):
    item = _item()
    state, client, platform, audit = _setup(tmp_path, [item])
    assert client.granted_proposals() == [item]
    request = platform.pickups[0]
    assert request.headers[PROPOSAL_PROTOCOL_HEADER] == PROPOSAL_PROTOCOL_VERSION
    assert PROPOSAL_PROTOCOL_HEADER == "X-Nakagai-Proposal-Protocol"
    assert PROPOSAL_PROTOCOL_VERSION == "1"


def test_granted_proposals_refuses_a_malformed_response(tmp_path):
    from nakagai_edge.edge.client import EdgeClientError

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=httpx.MockTransport(
                                lambda req: httpx.Response(200, json={"items": []})))
    with pytest.raises(EdgeClientError, match="malformed"):
        client.granted_proposals()
