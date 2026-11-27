"""A stored OAuth access token is refreshed when it expires.

The MCP SDK's `OAuthClientProvider._initialize` loads tokens from storage but
never sets `token_expiry_time`, and `is_token_valid()` treats a missing
expiry as valid forever. So a provider built from disk skipped the refresh,
sent the expired token, took the 401 into the full browser flow, and failed
for want of a browser: every connect after expiry, until a manual login.
The fix records the expiry and the token endpoint when tokens are saved, so
it applies from the first login on 0.10.0; a 0.9.0 file is used as before.

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
    assert doc["expires_at"] == pytest.approx(time.time() + 3600 - 60, abs=30)
    assert doc["oauth_metadata"]["token_endpoint"] == TOKEN_ENDPOINT


@pytest.mark.anyio
async def test_a_legacy_file_is_used_as_it_is(tmp_path):
    """A 0.9.0 file records neither an expiry nor the token endpoint. A
    forced refresh would have to guess the endpoint (`<host>/token`), and a
    wrong guess would end a login that works: every upgrading owner logged
    out and emailed. So the stored token is sent as before; it reaches
    needs_login only if the broker really rejects it, and the next login
    writes the new format."""
    _store(tmp_path, metadata=False)                 # 0.9.0 wrote neither
    broker = Broker(accept="old")
    response = await _get(tmp_path, broker)
    assert response.status_code == 200
    assert broker.seen == [f"GET {URL}"]


@pytest.mark.anyio
async def test_a_legacy_file_with_metadata_but_no_expiry_is_not_forced(tmp_path):
    _store(tmp_path)                                 # metadata, no expires_at
    broker = Broker(accept="old")
    assert (await _get(tmp_path, broker)).status_code == 200
    assert broker.seen == [f"GET {URL}"]


@pytest.mark.anyio
async def test_a_recorded_no_expiry_is_used_as_it_is(tmp_path):
    """`expires_at: null` is the server saying the token does not expire."""
    _store(tmp_path, expires_at=None)
    broker = Broker(accept="old")
    assert (await _get(tmp_path, broker)).status_code == 200
    assert broker.seen == [f"GET {URL}"]


@pytest.mark.anyio
async def test_a_failed_refresh_leaves_the_token_file_alone(tmp_path):
    """The SDK drops a refused token only in memory. The file on disk is
    what the next connect (and the next login check) reads, so it must come
    through a failed refresh byte for byte."""
    path = _store(tmp_path, expires_at=time.time() - 60)
    before = path.read_bytes()
    with pytest.raises(Exception):
        await _get(tmp_path, Broker(token_status=400))
    assert path.read_bytes() == before


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
async def test_saving_records_the_absolute_expiry_with_a_margin(tmp_path):
    """60 s early, so a token is refreshed before the broker refuses it, and
    never in the past."""
    from mcp.shared.auth import OAuthToken
    storage = FileTokenStorage(tmp_path / "t.json")
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer",
                                        expires_in=600))
    assert json.loads((tmp_path / "t.json").read_text())["expires_at"] == \
        pytest.approx(time.time() + 540, abs=5)
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer",
                                        expires_in=30))
    assert json.loads((tmp_path / "t.json").read_text())["expires_at"] == \
        pytest.approx(time.time(), abs=5)
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer"))
    # Recorded as "no expiry", which is not the same as "never recorded".
    assert json.loads((tmp_path / "t.json").read_text())["expires_at"] is None


@pytest.mark.anyio
async def test_a_garbled_expiry_reads_as_unrecorded(tmp_path):
    """Never a raw ValueError out of a connect: the legacy rule applies."""
    _store(tmp_path, expires_at="soon")
    broker = Broker(accept="old")
    assert (await _get(tmp_path, broker)).status_code == 200
    assert broker.seen == [f"GET {URL}"]


@pytest.mark.anyio
async def test_one_refresh_is_one_write(tmp_path, monkeypatch):
    """Tokens, expiry and metadata land together: two writes per refresh
    were two chances for a concurrent writer to interleave."""
    import os
    path = _store(tmp_path, expires_at=time.time() - 60)
    writes = []
    real = os.replace
    monkeypatch.setattr(os, "replace", lambda src, dst: (
        writes.append(str(dst)), real(src, dst))[1])
    await _get(tmp_path, Broker())
    assert writes == [str(path)]


@pytest.mark.anyio
async def test_the_token_file_is_0600_from_creation(tmp_path, monkeypatch):
    import os
    import stat
    from mcp.shared.auth import OAuthToken
    modes = []
    real = os.replace
    monkeypatch.setattr(os, "replace", lambda src, dst: (
        modes.append(stat.S_IMODE(os.stat(src).st_mode)), real(src, dst))[1])
    storage = FileTokenStorage(tmp_path / "t.json")
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer"))
    assert modes == [0o600]
    assert stat.S_IMODE((tmp_path / "t.json").stat().st_mode) == 0o600


@pytest.mark.anyio
async def test_an_unparseable_token_file_is_never_overwritten(tmp_path):
    """Merging into an empty document would leave a file with no tokens
    and no client registration: a login destroyed by a bad read."""
    from mcp.shared.auth import OAuthToken

    from nakagai_edge.auth import TokenFileError
    path = tmp_path / "t.json"
    path.write_text('{"tokens": {"access_')
    storage = FileTokenStorage(path)
    with pytest.raises(TokenFileError):
        await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer"))
    assert path.read_text() == '{"tokens": {"access_'


def test_interleaved_writers_never_lose_the_tokens(tmp_path):
    """The daemon refreshing while `nakagai-edge login` writes: each
    read-modify-write is whole, so neither drops the other's keys."""
    import asyncio
    import threading

    from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
    path = tmp_path / "t.json"
    a, b = FileTokenStorage(path), FileTokenStorage(path)
    info = OAuthClientInformationFull(client_id="cid",
                                      redirect_uris=["http://127.0.0.1:1/cb"])
    errors = []

    def hammer(fn):
        try:
            for _ in range(60):
                asyncio.run(fn())
        except Exception as e:      # noqa: BLE001
            errors.append(e)

    threads = [
        threading.Thread(target=hammer, args=(lambda: a.set_tokens(OAuthToken(
            access_token="x", token_type="Bearer", expires_in=600)),)),
        threading.Thread(target=hammer, args=(lambda: b.set_client_info(info),)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    doc = json.loads(path.read_text())
    assert doc["tokens"]["access_token"] == "x" and doc["client_info"]["client_id"] == "cid"
    assert not [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]


# ---- tokens belong to the server that issued them ---------------------------

def test_tokens_for_another_server_url_are_no_tokens(tmp_path):
    """The registry comes from the platform. A connector whose URL changed
    under the same id must not have its Bearer token (or refresh token)
    sent to the new host."""
    from nakagai_edge.auth import NoTokensError, build_http_client, has_oauth_tokens
    path = _store(tmp_path, expires_at=time.time() + 3600)
    doc = json.loads(path.read_text())
    path.write_text(json.dumps({**doc, "server_url": "https://old.test/mcp/"}))
    assert has_oauth_tokens(tmp_path, SPEC.id, SPEC.url) is False
    with pytest.raises(NoTokensError, match="old.test"):
        build_http_client(SPEC, tmp_path)


def test_a_legacy_file_with_no_server_url_still_connects(tmp_path):
    from nakagai_edge.auth import build_http_client, has_oauth_tokens
    _store(tmp_path, metadata=False)
    assert has_oauth_tokens(tmp_path, SPEC.id, SPEC.url) is True
    build_http_client(SPEC, tmp_path)


@pytest.mark.anyio
async def test_a_login_records_the_server_and_when(tmp_path):
    """`nakagai-edge login` (a provider with handlers) stamps the file;
    the daemon's own refresh writes never do."""
    from mcp.shared.auth import OAuthToken
    storage = FileTokenStorage(token_path(tmp_path, "rh"), login_url=URL)
    await storage.set_tokens(OAuthToken(access_token="a", token_type="Bearer"))
    doc = json.loads(token_path(tmp_path, "rh").read_text())
    assert doc["server_url"] == URL
    assert doc["login_at"] == pytest.approx(time.time(), abs=5)
    stamped = doc["login_at"]
    await FileTokenStorage(token_path(tmp_path, "rh")).set_tokens(
        OAuthToken(access_token="b", token_type="Bearer"))
    assert json.loads(token_path(tmp_path, "rh").read_text())["login_at"] == stamped


def test_the_login_provider_stamps_and_the_daemon_provider_does_not(tmp_path):
    async def handler(*a):
        return None
    login = build_oauth_provider(SPEC, tmp_path, handler, handler)
    daemon = build_oauth_provider(SPEC, tmp_path)
    assert login.context.storage.login_url == URL
    assert daemon.context.storage.login_url == ""


def test_a_renamed_sdk_hook_fails_loudly(monkeypatch):
    """The provider overrides private SDK methods. A future mcp that renames
    one must not silently bring back the never-refreshed token."""
    from mcp.client.auth import OAuthClientProvider
    monkeypatch.delattr(OAuthClientProvider, "_handle_refresh_response")
    with pytest.raises(RuntimeError, match="mcp"):
        build_oauth_provider(SPEC, "/tmp")
