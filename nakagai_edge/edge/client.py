"""Sync httpx client for the platform's /api/agent/* contract. Sync on purpose:
enqueue is called from inside the hub's guardrail path (a sync method), and
every other call sits in its own background thread/loop."""

import httpx


class EdgeClientError(Exception):
    """The platform refused or could not be reached."""


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
            headers={"Authorization": f"Bearer {token}"})

    def close(self) -> None:
        self._client.close()

    def _check(self, resp: httpx.Response) -> dict:
        if resp.status_code == 401:
            raise EdgeClientError("platform rejected the agent token. Was it revoked?")
        if resp.status_code >= 400:
            raise EdgeClientError(f"{resp.request.method} {resp.request.url.path} "
                                  f"-> {resp.status_code}: {_detail(resp)}")
        return resp.json()

    def get_bundle(self, etag: str = "") -> tuple[str, dict | None]:
        headers = {"If-None-Match": etag} if etag else {}
        resp = self._client.get("/api/agent/bundle", headers=headers)
        if resp.status_code == 304:
            return etag, None
        body = self._check(resp)
        return resp.headers.get("etag", body.get("bundle_version", "")), body

    def enqueue_approval(self, connector_id: str, tool: str, args: dict,
                         signal_id: str = "") -> dict:
        # signal_id is what the platform resolves to a frozen signal + notional
        # and checks against the autopilot envelope: an order citing a signal
        # Nakagai emitted, inside the owner's caps, comes back `granted` (signed)
        # for the edge to execute. The platform decides; the edge never does.
        return self._check(self._client.post("/api/agent/approvals", json={
            "connector_id": connector_id, "tool": tool, "args": args,
            "signal_id": signal_id}))

    def get_approval(self, approval_id: str) -> dict:
        return self._check(self._client.get(f"/api/agent/approvals/{approval_id}"))

    def report_execution(self, approval_id: str, *, ok: bool, result=None,
                         error: str = "", outcome_unknown: bool = False) -> dict:
        return self._check(self._client.post(
            f"/api/agent/approvals/{approval_id}/execution",
            json={"ok": ok, "result": result, "error": error,
                  "outcome_unknown": outcome_unknown}))

    def agent_checkin(self, status: str, note: str = "",
                      account_equity: float | None = None,
                      day_pnl: float | None = None) -> dict:
        # The edge's own heartbeat: relayed to the platform's
        # POST /api/agent/checkin (see nakagai/api/agent_routes.py), which
        # runs the exact same body a direct MCP agent's agent_checkin tool
        # does (nakagai.mandate.record_checkin). This is the edge's only
        # route to the autopilot daily-loss breaker's equity figure - the
        # platform holds no broker credentials to read it itself.
        return self._check(self._client.post("/api/agent/checkin", json={
            "status": status, "note": note, "account_equity": account_equity,
            "day_pnl": day_pnl}))

    def await_events(self, after: int = 0, timeout_s: float = 50) -> dict:
        # The one PlatformClient call whose response is SUPPOSED to take up
        # to the hold cap: give httpx a read timeout past it, or every quiet
        # hold would surface as a transport error.
        return self._check(self._client.get(
            "/api/agent/events", params={"after": after, "timeout_s": timeout_s},
            timeout=httpx.Timeout(15.0, read=float(timeout_s) + 10.0)))

    def send_message(self, text: str) -> dict:
        return self._check(self._client.post("/api/agent/message",
                                             json={"text": text}))

    def report_connectors(self, connectors: list[dict]) -> dict:
        # What this edge can currently reach, so the owner sees it in the web UI
        # without the platform ever holding the broker credential. Status is not
        # a secret; the token is, and the token stays here.
        return self._check(self._client.post("/api/agent/connectors",
                                             json={"connectors": connectors}))

    def report_portfolio(self, connectors: list[dict]) -> dict:
        # The edge's own figures for the owner's Portfolio page. Display state
        # only on the platform; the broker credential that fetched them stays
        # here, which is the whole point of the split.
        return self._check(self._client.post("/api/agent/portfolio",
                                             json={"connectors": connectors}))

    def ship_audit(self, events: list[dict]) -> dict:
        return self._check(self._client.post("/api/agent/audit",
                                             json={"events": events}))
