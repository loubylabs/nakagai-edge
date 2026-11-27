"""`auth_state` in the connector report: a field, not a sentence.

The platform tells an owner their broker login has ended by reading this one
key. Before it existed the only sign was the text of `last_error`, which a
reader had to match with a regular expression. So the classification is pinned
here on the real exception types, and on the cases that must NOT read as a
login problem: a network error or a timeout is not something a browser login
fixes, and telling the owner to log in again would send them chasing the wrong
fault.
"""

import json

import httpx
import pytest
from mcp.client.auth.exceptions import (OAuthFlowError, OAuthRegistrationError,
                                        OAuthTokenError)

from nakagai_edge.auth import NoTokensError, build_http_client, token_path
from nakagai_edge.config import ConnectorSpec
from nakagai_edge.hub import Connection, ConnectorHub, needs_login


@pytest.fixture
def anyio_backend():
    return "asyncio"

OAUTH = {"id": "robinhood-trading", "kind": "mcp-http", "role": "broker",
         "url": "https://broker.test/mcp/", "enabled": True,
         "auth": {"mode": "oauth"}}
PLAIN = {"id": "quotes", "kind": "mcp-http", "role": "data",
         "url": "https://quotes.test/mcp/", "enabled": True}


def _tokens(root, connector_id="robinhood-trading"):
    path = token_path(root, connector_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"tokens": {"access_token": "a",
                                           "token_type": "Bearer"}}))


# ---- the classifier, on the real exception types ---------------------------

@pytest.mark.parametrize("exc", [
    NoTokensError("connector 'x' has no OAuth tokens"),
    OAuthFlowError("No redirect handler provided for authorization code grant"),
    OAuthTokenError("Token exchange failed (400): invalid_grant"),
    OAuthRegistrationError("registration refused"),
])
def test_an_oauth_failure_needs_a_login(exc):
    assert needs_login(exc) is True


def test_an_oauth_failure_inside_a_task_group_still_needs_a_login():
    """The transports run in anyio task groups, so the real failure arrives
    wrapped. describe_exception digs for the leaf; so must this."""
    wrapped = BaseExceptionGroup("unhandled errors in a TaskGroup", [
        ExceptionGroup("inner", [OAuthFlowError("refresh failed")])])
    assert needs_login(wrapped) is True


@pytest.mark.parametrize("exc", [
    httpx.ConnectError("no route to host"),
    TimeoutError("timed out"),
    OSError("network is unreachable"),
    ValueError("connector 'x' needs environment variable FOO (the bearer token)"),
    RuntimeError("anything else"),
])
def test_a_network_or_unrelated_failure_is_not_a_login(exc):
    assert needs_login(exc) is False


def test_the_no_tokens_error_is_still_a_value_error():
    """Callers that caught ValueError keep working: the new type is narrower,
    never different."""
    assert issubclass(NoTokensError, ValueError)


def test_build_http_client_raises_the_typed_no_tokens_error(tmp_path):
    with pytest.raises(NoTokensError):
        build_http_client(ConnectorSpec(**OAUTH), tmp_path)


# ---- the field on the report -----------------------------------------------

def test_an_oauth_connector_with_no_tokens_needs_login(tmp_path):
    row = Connection(spec=ConnectorSpec(**OAUTH)).to_dict(root=tmp_path)
    assert row["auth_state"] == "needs_login"


def test_an_oauth_connector_holding_tokens_is_ok(tmp_path):
    _tokens(tmp_path)
    row = Connection(spec=ConnectorSpec(**OAUTH),
                     status="connected").to_dict(root=tmp_path)
    assert row["auth_state"] == "ok"


def test_tokens_on_disk_whose_connect_failed_on_oauth_need_login(tmp_path):
    """The case the email exists for: the refresh token died. The file is
    still on disk (the SDK clears tokens only in memory), so the file alone
    reads as fine; the failed connect is what says otherwise."""
    _tokens(tmp_path)
    row = Connection(spec=ConnectorSpec(**OAUTH), status="error",
                     login_failed=True).to_dict(root=tmp_path)
    assert row["auth_state"] == "needs_login"


def test_a_non_oauth_connector_is_always_ok(tmp_path):
    row = Connection(spec=ConnectorSpec(**PLAIN), status="error",
                     last_error="ConnectError: boom").to_dict(root=tmp_path)
    assert row["auth_state"] == "ok"


# ---- end to end through the hub's own connect path -------------------------

def _failing_connect(exc):
    async def connect(spec):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _open():
            raise exc
            yield  # pragma: no cover

        return _open()
    return connect


async def _status_after(tmp_path, exc, spec=OAUTH):
    from nakagai_edge.hub import ConnectorError
    import yaml
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(
        yaml.safe_dump({"connectors": [spec]}))
    hub = ConnectorHub(tmp_path, connect=_failing_connect(exc))
    with pytest.raises(ConnectorError):
        await hub._ensure_connected(hub.spec(spec["id"]))
    return hub.status()["connectors"][0]


@pytest.mark.anyio
async def test_a_connect_that_fails_on_oauth_reports_needs_login(tmp_path):
    _tokens(tmp_path)
    row = await _status_after(tmp_path, BaseExceptionGroup(
        "unhandled errors in a TaskGroup", [OAuthTokenError("invalid_grant")]))
    assert row["status"] == "error"
    assert row["auth_state"] == "needs_login"


@pytest.mark.anyio
async def test_a_connect_that_fails_on_the_network_stays_ok(tmp_path):
    _tokens(tmp_path)
    row = await _status_after(tmp_path, httpx.ConnectError("no route to host"))
    assert row["status"] == "error"
    assert row["auth_state"] == "ok"


@pytest.mark.anyio
async def test_a_connect_that_times_out_stays_ok(tmp_path):
    import asyncio
    import yaml
    from nakagai_edge.hub import ConnectorError

    _tokens(tmp_path)

    async def hang(spec):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _open():
            await asyncio.sleep(60)
            yield  # pragma: no cover

        return _open()

    spec = {**OAUTH, "timeout_s": 0.05}
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "connectors.yaml").write_text(
        yaml.safe_dump({"connectors": [spec]}))
    hub = ConnectorHub(tmp_path, connect=hang)
    with pytest.raises(ConnectorError):
        await hub._ensure_connected(hub.spec(spec["id"]))
    row = hub.status()["connectors"][0]
    assert row["auth_state"] == "ok"
