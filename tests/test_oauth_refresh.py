"""A stored OAuth access token is refreshed when it expires.

The MCP SDK's `OAuthClientProvider._initialize` loads tokens from storage but
never sets `token_expiry_time`, and `is_token_valid()` treats a missing
expiry as valid forever. So a provider built from disk skipped the refresh,
sent the expired token, took the 401 into the full browser flow, and failed
for want of a browser: every connect after expiry, until a manual login.

These run the REAL SDK provider over an httpx2 MockTransport: nothing here
stubs the flow under test.
"""

import json
import time

import httpx2
import pytest

from nakagai_edge.auth import FileTokenStorage, build_oauth_provider, token_path
from nakagai_edge.config import ConnectorSpec
from nakagai_edge.hub import needs_login

URL = "https://broker.test/mcp/"
TOKEN_ENDPOINT = "https://auth.broker.test/oauth/token"
SPEC = ConnectorSpec(id="rh", kind="mcp-http", role="broker", url=URL,
                     enabled=True, auth={"mode": "oauth"})


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _store(root, *, expires_at="absent", metadata=True):
    doc = {"tokens": {"access_token": "old", "token_type": "Bearer",
                      "refresh_token": "r1", "expires_in": 3600},
           "client_info": {"client_id": "cid",
                           "redirect_uris": ["http://127.0.0.1:1/callback"]}}
    if expires_at != "absent":
        doc["expires_at"] = expires_at
    if metadata:
        doc["oauth_metadata"] = {
            "issuer": "https://auth.broker.test/",
            "authorization_endpoint": "https://auth.broker.test/oauth/authorize",
            "token_endpoint": TOKEN_ENDPOINT,
            "response_types_supported": ["code"]}
    path = token_path(root, SPEC.id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))
    return path


class Broker:
    """A resource server that wants `Bearer new` and a token endpoint that
    answers `token_status`."""

    def __init__(self, token_status=200, accept="new"):
        self.token_status = token_status
        self.accept = accept
        self.seen: list[str] = []

    def __call__(self, request):
        url = str(request.url)
        self.seen.append(f"{request.method} {url}")
        if url == TOKEN_ENDPOINT or url.endswith("/token"):
            if self.token_status != 200:
                return httpx2.Response(self.token_status,
                                       json={"error": "invalid_grant"})
            return httpx2.Response(200, json={
                "access_token": "new", "token_type": "Bearer",
                "refresh_token": "r2", "expires_in": 3600})
        if request.headers.get("Authorization") == f"Bearer {self.accept}":
            return httpx2.Response(200, json={"ok": True})
        return httpx2.Response(401)


async def _get(root, broker):
    provider = build_oauth_provider(SPEC, root)
    async with httpx2.AsyncClient(auth=provider,
                                  transport=httpx2.MockTransport(broker)) as c:
        return await c.get(URL)


@pytest.mark.anyio
async def test_an_expired_stored_token_is_refreshed_and_the_file_rewritten(tmp_path):
    path = _store(tmp_path, expires_at=time.time() - 60)
    broker = Broker()
    response = await _get(tmp_path, broker)
    assert response.status_code == 200
    assert f"POST {TOKEN_ENDPOINT}" in broker.seen      # the stored endpoint
    doc = json.loads(path.read_text())
    assert doc["tokens"]["access_token"] == "new"
    assert doc["tokens"]["refresh_token"] == "r2"
    assert doc["expires_at"] == pytest.approx(time.time() + 3600, abs=30)
    assert doc["oauth_metadata"]["token_endpoint"] == TOKEN_ENDPOINT


@pytest.mark.anyio
async def test_a_legacy_file_with_no_recorded_expiry_is_refreshed_first(tmp_path):
    """Files written by 0.9.0 carry no expiry. Treating that as "valid
    forever" is the bug; treating it as expired costs one refresh."""
    _store(tmp_path, metadata=False)                 # 0.9.0 wrote neither
    broker = Broker()
    response = await _get(tmp_path, broker)
    assert response.status_code == 200
    assert broker.seen[0] == "POST https://broker.test/token"


@pytest.mark.anyio
async def test_a_valid_stored_token_is_used_without_a_refresh(tmp_path):
    _store(tmp_path, expires_at=time.time() + 3600)
    broker = Broker(accept="old")
    assert (await _get(tmp_path, broker)).status_code == 200
    assert broker.seen == [f"GET {URL}"]


@pytest.mark.anyio
async def test_a_refused_refresh_is_an_oauth_failure(tmp_path):
    """invalid_grant: the login really has ended. It surfaces as the SDK's
    OAuth error, which the hub counts toward needs_login."""
    _store(tmp_path, expires_at=time.time() - 60)
    with pytest.raises(Exception) as caught:
        await _get(tmp_path, Broker(token_status=400))
    assert needs_login(caught.value)


@pytest.mark.anyio
async def test_saving_records_the_absolute_expiry(tmp_path):
    from mcp.shared.auth import OAuthToken
    storage = FileTokenStorage(tmp_path / "t.json")
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer",
                                        expires_in=600))
    assert json.loads((tmp_path / "t.json").read_text())["expires_at"] == \
        pytest.approx(time.time() + 600, abs=5)
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer"))
    # Recorded as "no expiry", which is not the same as "never recorded".
    assert json.loads((tmp_path / "t.json").read_text())["expires_at"] is None
