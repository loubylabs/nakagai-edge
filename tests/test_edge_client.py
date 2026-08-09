"""PlatformClient against httpx.MockTransport: no network, real wire shapes."""

import json

import httpx
import pytest

from nakagai_edge.edge.client import EdgeClientError, PlatformClient, pair
from nakagai_edge.edge.state import EdgeState


def _transport(handler):
    return httpx.MockTransport(handler)


def test_pair_stores_nothing_but_returns_token():
    def handler(req):
        assert req.url.path == "/api/agents/pair"
        assert json.loads(req.content) == {"code": "c123"}
        return httpx.Response(200, json={"ok": True, "agent_id": "ag1",
                                         "token": "nk_agent_t"})
    out = pair("https://api.test", "c123", transport=_transport(handler))
    assert out == {"ok": True, "agent_id": "ag1", "token": "nk_agent_t"}


def test_pair_error_raises_with_detail():
    def handler(req):
        return httpx.Response(403, json={"detail": "pairing failed"})
    with pytest.raises(EdgeClientError, match="pairing failed"):
        pair("https://api.test", "bad", transport=_transport(handler))


def test_bundle_etag_and_304():
    def handler(req):
        assert req.headers["authorization"] == "Bearer nk_agent_t"
        if req.headers.get("if-none-match") == "v1":
            return httpx.Response(304)
        return httpx.Response(200, json={"bundle_version": "v1", "connectors": {}},
                              headers={"ETag": "v1"})
    c = PlatformClient("https://api.test", "nk_agent_t", transport=_transport(handler))
    etag, bundle = c.get_bundle()
    assert etag == "v1" and bundle["bundle_version"] == "v1"
    etag2, none = c.get_bundle(etag="v1")
    assert etag2 == "v1" and none is None


def test_approval_round_trip_paths():
    seen = []
    def handler(req):
        seen.append((req.method, req.url.path))
        return httpx.Response(200, json={"ok": True, "approval_id": "a1",
                                         "status": "pending", "expires_at": 0})
    c = PlatformClient("https://api.test", "nk_agent_t", transport=_transport(handler))
    c.enqueue_approval("rh", "place_order", {"qty": 1})
    c.get_approval("a1")
    c.report_execution("a1", ok=True, result={"id": "42"})
    c.ship_audit([{"kind": "call"}])
    assert seen == [("POST", "/api/agent/approvals"),
                    ("GET", "/api/agent/approvals/a1"),
                    ("POST", "/api/agent/approvals/a1/execution"),
                    ("POST", "/api/agent/audit")]


def test_chat_protocol_request_shapes():
    def handler(request):
        assert request.headers["x-nakagai-chat-protocol"] == "2"
        if request.url.path == "/api/agent/messages/41/claim":
            assert request.method == "POST"
            assert request.content == b""
        elif request.url.path == "/api/agent/message":
            assert request.method == "POST"
            assert json.loads(request.content) == {
                "text": "answer",
                "room_id": "desk",
                "idempotency_key": "reply-1",
                "reply_to_seq": 41,
            }
        elif request.url.path == "/api/agent/requests":
            assert request.method == "POST"
            assert json.loads(request.content) == {
                "agent_ids": ["a2", "a3"],
                "text": "please review",
                "idempotency_key": "peer-1",
                "source_seq": 40,
            }
        elif request.url.path == "/api/agent/peers":
            assert request.method == "GET"
            assert request.content == b""
        else:
            raise AssertionError(request.url.path)
        return httpx.Response(200, json={"ok": True})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=_transport(handler))
    client.claim_message(41)
    client.send_message("answer", "desk", "reply-1", reply_to_seq=41)
    client.request_peer(["a2", "a3"], "please review", "peer-1", source_seq=40)
    client.list_peers()


def test_chat_protocol_header_only_covers_chat_routes():
    def handler(request):
        chat_protocol = request.headers.get("x-nakagai-chat-protocol")
        if request.url.path == "/api/agent/bundle":
            assert chat_protocol is None
            return httpx.Response(200, json={"bundle_version": "v1"})
        assert request.url.path in {"/api/agent/checkin", "/api/agent/events"}
        assert chat_protocol == "2"
        return httpx.Response(200, json={"ok": True, "events": []})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=_transport(handler))
    client.get_bundle()
    client.agent_checkin("idle")
    client.await_events(timeout_s=0)


def test_chat_conflict_body_is_returned_intact():
    conflict = {
        "ok": False,
        "error": "already_claimed",
        "claimant": {"agent_id": "a1", "name": "Claude"},
        "claim_expires_at": "2026-08-08T22:05:00+00:00",
        "retry_at": "2026-08-08T22:05:00+00:00",
    }

    def handler(request):
        assert request.url.path == "/api/agent/messages/41/claim"
        return httpx.Response(409, json=conflict)

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=_transport(handler))
    assert client.claim_message(41) == conflict


def test_chat_protocol_upgrade_error_preserves_server_detail():
    def handler(request):
        return httpx.Response(426, json={
            "detail": "edge_upgrade_required: minimum edge version 0.3.0"})

    client = PlatformClient("https://api.test", "nk_agent_t",
                            transport=_transport(handler))
    with pytest.raises(EdgeClientError, match="edge_upgrade_required.*0.3.0"):
        client.list_peers()


def test_state_agent_json_round_trip(tmp_path):
    s = EdgeState(tmp_path)
    assert s.agent() is None
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    assert s.agent() == {"platform_url": "https://api.test",
                         "agent_id": "ag1", "token": "nk_agent_t"}
    import stat, os
    mode = stat.S_IMODE(os.stat(s.root / "agent.json").st_mode)
    assert mode == 0o600


def test_edge_root_dir_is_0700(tmp_path):
    root = tmp_path / "edge-root"
    s = EdgeState(root)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    import stat, os
    mode = stat.S_IMODE(os.stat(s.root).st_mode)
    assert mode == 0o700


def test_edge_root_dir_tightened_if_already_loose(tmp_path):
    root = tmp_path / "edge-root"
    root.mkdir(mode=0o755)
    import os
    os.chmod(root, 0o755)  # mkdir's mode arg is masked by umask; force it
    s = EdgeState(root)
    s.save_agent("https://api.test", "ag1", "nk_agent_t")
    import stat
    mode = stat.S_IMODE(os.stat(s.root).st_mode)
    assert mode == 0o700


# ---- tool schemas upstream, on change only -------------------------------
#
# The platform never dials a broker, so this report is the only way a
# connector's JSON schemas ever reach it. Robinhood publishes thirty-odd tools
# with full schemas and the syncer reports every sixty seconds, so the schemas
# ride only when they have actually changed since the last report the platform
# accepted.

GET_ACCOUNT = {
    "name": "get_account",
    "description": "Balances for one account",
    "inputSchema": {"type": "object",
                    "properties": {"account": {"type": "string"}},
                    "required": ["account"]},
}


def _connector(tools, cid="robinhood-trading", status="connected"):
    """One row shaped like Connection.to_dict(with_tools=True) produces."""
    return {"id": cid, "name": "Robinhood", "status": status,
            "tool_count": len(tools), "tools": tools}


class _Platform:
    """Records every connector report; `fail` makes the next one a 500."""

    def __init__(self):
        self.seen: list[list[dict]] = []
        self.fail = False

    def client(self) -> PlatformClient:
        return PlatformClient("https://api.test", "nk_agent_t",
                              transport=_transport(self._handle))

    def _handle(self, req):
        assert req.url.path == "/api/agent/connectors"
        self.seen.append(json.loads(req.content)["connectors"])
        if self.fail:
            return httpx.Response(500, json={"detail": "boom"})
        return httpx.Response(200, json={"ok": True, "connectors": 1})

    def entry(self, cycle: int, cid: str = "robinhood-trading") -> dict:
        return next(c for c in self.seen[cycle] if c["id"] == cid)


def test_the_first_report_carries_the_tool_schemas():
    p = _Platform()
    p.client().report_connectors([_connector([GET_ACCOUNT])])
    assert p.entry(0)["tools"] == [GET_ACCOUNT]
    assert "tools_unchanged" not in p.entry(0)


def test_an_unchanged_tool_list_is_not_sent_twice():
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    c.report_connectors([_connector([GET_ACCOUNT])])
    assert "tools" not in p.entry(1)
    assert p.entry(1)["tools_unchanged"] is True
    # Everything else still rides every cycle: the guard is about the schemas,
    # never about the status the owner watches in the web UI.
    assert p.entry(1)["status"] == "connected"
    assert p.entry(1)["tool_count"] == 1


def test_a_renamed_tool_resends_the_schemas():
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    c.report_connectors([_connector([GET_ACCOUNT])])
    c.report_connectors([_connector([{**GET_ACCOUNT, "name": "get_accounts"}])])
    assert p.entry(2)["tools"][0]["name"] == "get_accounts"
    assert "tools_unchanged" not in p.entry(2)


def test_a_changed_input_schema_resends_under_the_same_tool_name():
    # The case a name-only hash misses: same tool, new required argument. The
    # platform derives its capability map from the schema, so it has to see
    # this one.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    widened = {**GET_ACCOUNT,
               "inputSchema": {"type": "object",
                               "properties": {"account": {"type": "string"},
                                              "since": {"type": "string"}},
                               "required": ["account", "since"]}}
    c.report_connectors([_connector([widened])])
    assert p.entry(1)["tools"] == [widened]
    assert "tools_unchanged" not in p.entry(1)


def test_key_order_inside_a_schema_is_not_a_change():
    # A downstream that re-serializes its schemas in a different key order has
    # changed nothing. Without the sorted projection this would resend thirty
    # schemas every sixty seconds forever, which is the exact waste the guard
    # exists to stop.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    reordered = {**GET_ACCOUNT,
                 "inputSchema": {"required": ["account"],
                                 "properties": {"account": {"type": "string"}},
                                 "type": "object"}}
    c.report_connectors([_connector([reordered])])
    assert "tools" not in p.entry(1)
    assert p.entry(1)["tools_unchanged"] is True


def test_the_order_the_downstream_listed_its_tools_in_is_not_a_change():
    p = _Platform()
    c = p.client()
    other = {"name": "cancel_order", "description": "", "inputSchema": {}}
    c.report_connectors([_connector([GET_ACCOUNT, other])])
    c.report_connectors([_connector([other, GET_ACCOUNT])])
    assert p.entry(1)["tools_unchanged"] is True


def test_a_reworded_description_resends_the_tools():
    # The digest covers every field the report carries, description included.
    # The description is what the human approving a derived capability map
    # reads, and it rides in the same payload as the schemas anyway, so
    # excluding it would buy no bytes on a quiet cycle and would leave the
    # platform showing wording the broker has retired.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    reworded = {**GET_ACCOUNT, "description": "Balances. Cash included."}
    c.report_connectors([_connector([reworded])])
    assert p.entry(1)["tools"] == [reworded]
    assert "tools_unchanged" not in p.entry(1)


def test_a_failed_report_resends_the_schemas_next_cycle():
    """The digest is the edge's belief about what the PLATFORM holds. A report
    that never landed changed nothing there, so remembering its digest would
    leave the platform without schemas it never received until the tool list
    happened to change again."""
    p = _Platform()
    c = p.client()
    p.fail = True
    with pytest.raises(EdgeClientError):
        c.report_connectors([_connector([GET_ACCOUNT])])
    p.fail = False
    c.report_connectors([_connector([GET_ACCOUNT])])
    assert p.entry(1)["tools"] == [GET_ACCOUNT]
    assert "tools_unchanged" not in p.entry(1)


def test_each_connector_is_remembered_on_its_own():
    p = _Platform()
    c = p.client()
    other = {"name": "get_clock", "description": "", "inputSchema": {}}
    c.report_connectors([_connector([GET_ACCOUNT]),
                         _connector([other], cid="alpaca")])
    c.report_connectors([_connector([GET_ACCOUNT]),
                         _connector([other, GET_ACCOUNT], cid="alpaca")])
    assert p.entry(1)["tools_unchanged"] is True
    assert len(p.entry(1, "alpaca")["tools"]) == 2


def test_a_connector_with_no_tools_says_so_before_it_says_unchanged():
    # `tools: []` means "this connector has no tools"; `tools_unchanged` means
    # "you already hold what it has". The platform cannot act on the first
    # correctly if the second looks identical on the wire.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([])])
    assert p.entry(0)["tools"] == []
    c.report_connectors([_connector([])])
    assert "tools" not in p.entry(1)
    assert p.entry(1)["tools_unchanged"] is True


def test_a_row_that_carries_no_tools_key_is_passed_through_untouched():
    p = _Platform()
    c = p.client()
    c.report_connectors([{"id": "robinhood-trading", "status": "error"}])
    c.report_connectors([{"id": "robinhood-trading", "status": "error"}])
    assert p.entry(1) == {"id": "robinhood-trading", "status": "error"}


def test_the_callers_status_rows_are_never_mutated():
    # The syncer hands us hub.status()'s own dicts. Stripping `tools` in place
    # would edit what the caller still holds, and the next cycle would then
    # hash an already-stripped row.
    p = _Platform()
    c = p.client()
    rows = [_connector([GET_ACCOUNT])]
    c.report_connectors(rows)
    c.report_connectors(rows)
    assert rows[0]["tools"] == [GET_ACCOUNT]
    assert "tools_unchanged" not in rows[0]


def test_a_connector_that_is_not_connected_says_nothing_about_its_tools():
    # Its Connection is replaced on the way down, so the tool list here is
    # empty because nobody looked, not because the broker publishes nothing.
    # Reporting that emptiness would invite the platform to read a flap as a
    # broker that lost every tool it had.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([], status="error")])
    assert "tools" not in p.entry(0)
    assert "tools_unchanged" not in p.entry(0)
    assert p.entry(0)["status"] == "error"


def test_a_connection_flap_costs_neither_a_wipe_nor_a_resend():
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    c.report_connectors([_connector([], status="error")])
    c.report_connectors([_connector([GET_ACCOUNT])])
    # Cycle 1 said nothing, so the platform still holds what cycle 0 gave it
    # and cycle 2 has nothing new to say. Both halves matter: a wipe in the
    # middle would lose the schemas, and a forgotten digest would resend them.
    assert "tools" not in p.entry(1)
    assert "tools" not in p.entry(2)
    assert p.entry(2)["tools_unchanged"] is True


def test_a_connector_dropped_from_the_report_is_forgotten():
    # Disabled in the registry, then re-added. To the platform that is a new
    # connector row, so answering `tools_unchanged` would leave it holding no
    # schemas at all with nothing short of an edge restart to correct it.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    c.report_connectors([_connector([{"name": "get_clock", "description": "",
                                      "inputSchema": {}}], cid="alpaca")])
    c.report_connectors([_connector([GET_ACCOUNT])])
    assert p.entry(2)["tools"] == [GET_ACCOUNT]
    assert "tools_unchanged" not in p.entry(2)


def test_a_report_that_failed_prunes_nothing_either():
    # Same rule as the digest itself: a call that never landed changed nothing
    # on the platform, so it may not change what we believe about it.
    p = _Platform()
    c = p.client()
    c.report_connectors([_connector([GET_ACCOUNT])])
    p.fail = True
    with pytest.raises(EdgeClientError):
        c.report_connectors([_connector([{"name": "get_clock", "description": "",
                                          "inputSchema": {}}], cid="alpaca")])
    p.fail = False
    c.report_connectors([_connector([GET_ACCOUNT])])
    assert p.entry(2)["tools_unchanged"] is True


def test_every_request_carries_the_running_version():
    """One header on the shared client, so the platform learns the version
    from whatever call happens first after a restart rather than from a
    check-in the daemon may never make on its own."""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("x-nakagai-edge-version"))
        return httpx.Response(200, json={"ok": True})

    from nakagai_edge.identity import package_version
    c = PlatformClient("https://p.example", "nk_agent_x",
                       transport=httpx.MockTransport(handler))
    c.agent_checkin("idle")
    c.ship_audit([])
    assert seen == [package_version(), package_version()]
    assert package_version() != ""
