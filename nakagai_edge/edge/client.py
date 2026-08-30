"""Sync httpx client for the platform's /api/agent/* contract. Sync on purpose:
enqueue is called from inside the hub's guardrail path (a sync method), and
every other call sits in its own background thread/loop."""

import hashlib
import json

import httpx

from nakagai_edge.identity import package_version


CHAT_PROTOCOL_VERSION = "2"
CHAT_PROTOCOL_HEADER = "X-Nakagai-Chat-Protocol"
CANDIDATE_PROTOCOL_VERSION = "1"
CANDIDATE_PROTOCOL_HEADER = "X-Nakagai-Candidate-Protocol"


class EdgeClientError(Exception):
    """The platform refused or could not be reached.

    `status` is the HTTP status when there was one, and None for a transport
    failure. A retry loop needs it: 401 is terminal (the token was revoked and
    reconnecting forever would leave the owner believing the pipe is live),
    while 429 and 5xx are worth backing off and trying again.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def _tool_digest(tools: list[dict]) -> str:
    """A stable fingerprint of one connector's tool list.

    Covers exactly the three fields that get reported (name, description,
    inputSchema), so nothing the platform reads can go stale behind an
    unchanged digest. Including the description costs nothing on a quiet cycle,
    since it rides in the same payload as the schema when anything at all
    changes, and it only forces a resend when a broker actually reworded a
    tool, which is a release, not a minute.

    Sorted by name, and each schema serialized with sorted keys: the order a
    downstream happened to list its tools in, and the key order inside a
    schema, are not changes to what it can be asked to do. Without both
    sortings a server that re-serializes its schemas differently between two
    calls would look changed every sixty seconds forever, which is the exact
    resend this digest exists to prevent.
    """
    projection = sorted(
        (str(tool.get("name") or ""),
         str(tool.get("description") or ""),
         json.dumps(tool.get("inputSchema") or {}, sort_keys=True, default=str))
        for tool in tools)
    return hashlib.sha256(json.dumps(projection).encode()).hexdigest()


def _detail(resp: httpx.Response) -> str:
    try:
        return resp.json().get("detail", resp.text)
    except Exception:
        return resp.text


def pair(platform_url: str, code: str, *, transport=None) -> dict:
    with httpx.Client(base_url=platform_url.rstrip("/"), timeout=15.0,
                      transport=transport) as c:
        resp = c.post("/api/agents/pair", json={"code": code})
    if resp.status_code != 200:
        raise EdgeClientError(f"pairing failed ({resp.status_code}): {_detail(resp)}")
    return resp.json()


class PlatformClient:
    def __init__(self, platform_url: str, token: str, timeout: float = 15.0,
                 *, transport=None) -> None:
        self._client = httpx.Client(
            base_url=platform_url.rstrip("/"), timeout=timeout, transport=transport,
            # The version rides on every request rather than on one dedicated
            # call, because the daemon has no heartbeat of its own: agent_checkin
            # is a tool the AGENT invokes, so an edge whose agent is idle would
            # never report. Whatever the syncer does next carries it instead, and
            # the platform learns the version at the earliest moment it could.
            #
            # Advisory only, and self-asserted. Nothing but a chat message may
            # ever depend on it, for the same reason report_connectors has
            # exactly one reader: an edge that can lie about its own health must
            # never lie its way into authority.
            headers={"Authorization": f"Bearer {token}",
                     "X-Nakagai-Edge-Version": package_version()})
        # connector id -> digest of the tool list the platform has actually
        # taken, so report_connectors can leave unchanged schemas at home. In
        # memory on purpose: a restart re-sends once, which costs one payload
        # and cannot disagree with the platform the way a cache file could.
        self._reported_tools: dict[str, str] = {}

    def close(self) -> None:
        self._client.close()

    def _check(self, resp: httpx.Response) -> dict:
        if resp.status_code == 401:
            raise EdgeClientError(
                "platform rejected the agent token. Was it revoked?", 401)
        if resp.status_code >= 400:
            raise EdgeClientError(f"{resp.request.method} {resp.request.url.path} "
                                  f"-> {resp.status_code}: {_detail(resp)}",
                                  resp.status_code)
        return resp.json()

    def _chat_headers(self) -> dict[str, str]:
        return {CHAT_PROTOCOL_HEADER: CHAT_PROTOCOL_VERSION}

    def _chat_result(self, resp: httpx.Response) -> dict:
        if resp.status_code in {200, 409}:
            return resp.json()
        return self._check(resp)

    def get_bundle(self, etag: str = "") -> tuple[str, dict | None]:
        headers = {"If-None-Match": etag} if etag else {}
        resp = self._client.get("/api/agent/bundle", headers=headers)
        if resp.status_code == 304:
            return etag, None
        body = self._check(resp)
        return resp.headers.get("etag", body.get("bundle_version", "")), body

    def enqueue_approval(self, connector_id: str, tool: str, args: dict,
                         signal_id: str = "", candidate_id: str = "") -> dict:
        # signal_id is what the platform resolves to a frozen signal + notional
        # and checks against the autopilot envelope: an order citing a signal
        # Nakagai emitted, inside the owner's caps, comes back `granted` (signed)
        # for the edge to execute. The platform decides; the edge never does.
        return self._check(self._client.post("/api/agent/approvals", json={
            "connector_id": connector_id, "tool": tool, "args": args,
            "signal_id": signal_id, "candidate_id": candidate_id}))

    def get_approval(self, approval_id: str) -> dict:
        return self._check(self._client.get(f"/api/agent/approvals/{approval_id}"))

    def report_execution(self, approval_id: str, *, ok: bool, result=None,
                         error: str = "", outcome_unknown: bool = False,
                         order_id: str = "") -> dict:
        # `order_id` is the broker's own id for the order this approval placed,
        # read through the connector's `place_order` map. It is the left side of
        # the join that tells an owner-placed fill from an agent one: the fill
        # journal carries the right side, and a fill no approval claims is the
        # owner's. Empty when the connector declares no path to it, which is
        # honest rather than a guess; those approvals simply match nothing.
        return self._check(self._client.post(
            f"/api/agent/approvals/{approval_id}/execution",
            json={"ok": ok, "result": result, "error": error,
                  "outcome_unknown": outcome_unknown, "order_id": order_id}))

    def agent_checkin(self, status: str, note: str = "",
                      account_equity: float | None = None,
                      day_pnl: float | None = None) -> dict:
        # The edge's own heartbeat: relayed to the platform's
        # POST /api/agent/checkin (see nakagai/api/agent_routes.py), which
        # runs the exact same body a direct MCP agent's agent_checkin tool
        # does (nakagai.mandate.record_checkin). This is the edge's only
        # route to the autopilot daily-loss breaker's equity figure - the
        # platform holds no broker credentials to read it itself.
        return self._chat_result(self._client.post(
            "/api/agent/checkin", headers=self._chat_headers(), json={
                "status": status, "note": note, "account_equity": account_equity,
                "day_pnl": day_pnl}))

    def await_events(self, after: int = 0, timeout_s: float = 50) -> dict:
        # The one PlatformClient call whose response is SUPPOSED to take up
        # to the hold cap: give httpx a read timeout past it, or every quiet
        # hold would surface as a transport error.
        return self._chat_result(self._client.get(
            "/api/agent/events", params={"after": after, "timeout_s": timeout_s},
            headers={**self._chat_headers(),
                     CANDIDATE_PROTOCOL_HEADER: CANDIDATE_PROTOCOL_VERSION},
            timeout=httpx.Timeout(15.0, read=float(timeout_s) + 10.0)))

    def accept_candidate(self, candidate_id: str, rationale: str) -> dict:
        return self._check(self._client.post(
            f"/api/agent/candidates/{candidate_id}/accept",
            json={"rationale": rationale}))

    def abstain_candidate(self, candidate_id: str, rationale: str) -> dict:
        return self._check(self._client.post(
            f"/api/agent/candidates/{candidate_id}/abstain",
            json={"rationale": rationale}))

    def report_candidate_outcome(
            self, candidate_id: str, *, mechanical_status: str,
            mechanical_reason: str, approval_id: str = "", urgent: bool = False,
            outcome_unknown: bool = False) -> dict:
        return self._check(self._client.post(
            f"/api/agent/candidates/{candidate_id}/outcome",
            json={
                "mechanical_status": mechanical_status,
                "mechanical_reason": mechanical_reason,
                "approval_id": approval_id,
                "urgent": urgent,
                "outcome_unknown": outcome_unknown,
            }))

    def list_peers(self) -> dict:
        return self._chat_result(self._client.get(
            "/api/agent/peers", headers=self._chat_headers()))

    def claim_message(self, message_seq: int) -> dict:
        return self._chat_result(self._client.post(
            f"/api/agent/messages/{message_seq}/claim",
            headers=self._chat_headers()))

    def send_message(self, text: str, room_id: str, idempotency_key: str,
                     reply_to_seq: int = 0) -> dict:
        return self._chat_result(self._client.post(
            "/api/agent/message", headers=self._chat_headers(), json={
                "text": text, "room_id": room_id,
                "idempotency_key": idempotency_key,
                "reply_to_seq": reply_to_seq}))

    def request_peer(self, agent_ids: list[str], text: str,
                     idempotency_key: str, source_seq: int = 0) -> dict:
        return self._chat_result(self._client.post(
            "/api/agent/requests", headers=self._chat_headers(), json={
                "agent_ids": agent_ids, "text": text,
                "idempotency_key": idempotency_key,
                "source_seq": source_seq}))

    def report_connectors(self, connectors: list[dict]) -> dict:
        # What this edge can currently reach, so the owner sees it in the web UI
        # without the platform ever holding the broker credential. Status is not
        # a secret; the token is, and the token stays here.
        #
        # The tool schemas ride only when they have changed since the last
        # report the platform took. Robinhood publishes thirty-odd tools with
        # full JSON schemas, this runs every sixty seconds, and the list changes
        # about never: sending it regardless would be a steady tax on someone's
        # home connection for no new information.
        #
        # Three distinguishable statements per connector, because they are three
        # different facts: `tools: [...]` is "here is what it publishes",
        # `tools_unchanged: true` is "you already hold that", and neither key at
        # all is "this edge is not in touch with it, so it is not saying".
        # `tools: []` therefore keeps its own meaning: a connected broker that
        # publishes nothing.
        payload, digests, reported = [], {}, set()
        for row in connectors:
            entry = dict(row)   # the caller still holds `row`; never edit it
            connector_id = str(entry.get("id") or "")
            reported.add(connector_id)
            if entry.get("status") != "connected":
                # A connector we cannot currently reach has nothing to say
                # about its tools. Its Connection is replaced on the way down,
                # so an empty `tools` here means "we did not look", not "this
                # broker has none", and shipping it would invite the platform
                # to read a flap as a broker that lost every tool. Saying
                # nothing at all (no `tools`, no `tools_unchanged`) leaves both
                # the platform's copy and our digest exactly as they were, so
                # the reconnect costs no resend either.
                entry.pop("tools", None)
            if entry.get("tools") is None:
                payload.append(entry)
                continue
            digest = _tool_digest(entry["tools"])
            digests[connector_id] = digest
            if self._reported_tools.get(connector_id) == digest:
                del entry["tools"]
                entry["tools_unchanged"] = True
            payload.append(entry)
        out = self._check(self._client.post("/api/agent/connectors",
                                            json={"connectors": payload}))
        # Only once the report has landed. A call that raised above delivered
        # nothing, and remembering its digest would leave the platform without
        # schemas it never received until the tool list happened to change
        # again, which for a stable broker is never.
        #
        # Connectors this report did not mention are forgotten: one dropped
        # from the bundle and later re-added is a new connector to the
        # platform, and a remembered digest would answer `tools_unchanged`
        # about schemas it no longer holds, with nothing short of an edge
        # restart to correct it.
        kept = {cid: digest for cid, digest in self._reported_tools.items()
                if cid in reported}
        kept.update(digests)
        self._reported_tools = kept
        return out

    def report_portfolio(self, connectors: list[dict]) -> dict:
        # The edge's own figures for the owner's Portfolio page. Display state
        # only on the platform; the broker credential that fetched them stays
        # here, which is the whole point of the split.
        return self._check(self._client.post("/api/agent/portfolio",
                                             json={"connectors": connectors}))

    def ship_audit(self, events: list[dict]) -> dict:
        return self._check(self._client.post("/api/agent/audit",
                                             json={"events": events}))

    def report_fills(self, fills: list[dict]) -> dict:
        # Deliberately NOT ship_audit. The audit trail is the record of what
        # the edge did, and it is what answers for the agent's conduct; a trade
        # the owner placed in the broker's own app is an observation about the
        # broker, not an edge action. Sharing the pipe would stop the audit
        # trail answering the one question it exists for.
        return self._check(self._client.post("/api/agent/fills",
                                             json={"fills": fills}))

    def renew_warrants(self, positions: list[dict]) -> dict:
        # Warrants expire in 24h so a stolen one dies quickly; the edge
        # re-asks on its ordinary sync cadence and the platform re-signs what
        # it still recognizes. This is also the channel that will later carry
        # recomputed trailing-stop levels, which is why it posts positions
        # rather than just ids.
        return self._check(self._client.post("/api/agent/warrants",
                                             json={"positions": positions}))
