"""Downstream credentials: env-var indirection for tokens, files for OAuth.

No secret ever lands in `config/connectors.yaml`: `bearer`/`headers` name
environment variables and this module resolves them at connect time. OAuth
tokens persist under `secrets/tokens/<connector-id>.json` (mode 0600, and
`secrets/` is gitignored).
"""

import json
import os
import time
from pathlib import Path

from nakagai_edge._env import read_env_ref
from nakagai_edge.config import ConnectorSpec

MISSING_ENV = "connector {id!r} needs environment variable {var} ({what})"


def resolve_headers(spec: ConnectorSpec) -> dict[str, str]:
    """Build the outbound header map, reading secrets from the environment."""
    auth = spec.auth
    headers: dict[str, str] = {}
    if auth.mode == "bearer":
        if not auth.token_env:
            raise ValueError(f"connector {spec.id!r}: auth.mode=bearer needs auth.token_env")
        token = read_env_ref(auth.token_env)
        if not token:
            raise ValueError(MISSING_ENV.format(id=spec.id, var=auth.token_env,
                                                what="the bearer token"))
        headers["Authorization"] = f"Bearer {token}"
    elif auth.mode == "headers":
        for header, env_name in auth.headers.items():
            value = read_env_ref(env_name)
            if not value:
                raise ValueError(MISSING_ENV.format(id=spec.id, var=env_name,
                                                    what=f"the {header} header"))
            headers[header] = value
    return headers


def token_path(root: Path, connector_id: str) -> Path:
    return root / "secrets" / "tokens" / f"{connector_id}.json"


class FileTokenStorage:
    """`mcp.client.auth.TokenStorage` backed by a 0600 file.

    Structural typing: the SDK's TokenStorage is a Protocol, so this needs no
    base class, only the four methods.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def _read(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def _write(self, doc: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    async def get_tokens(self):
        from mcp.shared.auth import OAuthToken
        raw = self._read().get("tokens")
        return OAuthToken.model_validate(raw) if raw else None

    async def set_tokens(self, tokens) -> None:
        # The absolute expiry, computed now from `expires_in`. The SDK keeps
        # it only in memory and never restores it on load (see
        # StoredOAuthProvider), so without this a token read back from disk
        # looks valid forever. None records "the server gave no expiry",
        # which is different from a file that never recorded one.
        expires_at = (time.time() + tokens.expires_in
                      if tokens.expires_in is not None else None)
        self._write({**self._read(), "tokens": tokens.model_dump(mode="json"),
                     "expires_at": expires_at})

    def get_expiry(self) -> tuple[bool, float | None]:
        """(recorded, expires_at). `recorded` is False for a file written
        before expiries were kept."""
        doc = self._read()
        return "expires_at" in doc, doc.get("expires_at")

    def get_oauth_metadata(self):
        from mcp.shared.auth import OAuthMetadata
        raw = self._read().get("oauth_metadata")
        try:
            return OAuthMetadata.model_validate(raw) if raw else None
        except Exception:      # noqa: BLE001
            return None

    def set_oauth_metadata(self, metadata) -> None:
        if metadata is not None:
            self._write({**self._read(),
                         "oauth_metadata": metadata.model_dump(mode="json")})

    async def get_client_info(self):
        from mcp.shared.auth import OAuthClientInformationFull
        raw = self._read().get("client_info")
        return OAuthClientInformationFull.model_validate(raw) if raw else None

    async def set_client_info(self, client_info) -> None:
        self._write({**self._read(), "client_info": client_info.model_dump(mode="json")})


def has_oauth_tokens(root: Path, connector_id: str) -> bool:
    """Whether `nakagai-edge login <id>` has been run for this connector."""
    path = token_path(root, connector_id)
    if not path.exists():
        return False
    try:
        return bool(json.loads(path.read_text()).get("tokens"))
    except (json.JSONDecodeError, OSError):
        return False


def build_oauth_provider(spec: ConnectorSpec, root: Path,
                         redirect_handler=None, callback_handler=None):
    """An `httpx2.Auth` that attaches (and silently refreshes) OAuth tokens.

    With no handlers this is the server-side posture: stored tokens are used and
    refreshed, but a flow that needs a browser fails loudly instead of hanging.
    """
    from mcp.client.auth import OAuthClientProvider
    from mcp.shared.auth import OAuthClientMetadata

    from nakagai_edge.identity import BRAND_URL, DISPLAY_NAME, SEAL_URL

    # The registration metadata is the other half of our identity: some hosts
    # brand the consent screen from here rather than from `clientInfo`. Note
    # Robinhood answered our registration with a `client_name` of its own
    # choosing, so this is a hope, not a guarantee.
    metadata = OAuthClientMetadata(
        client_name=DISPLAY_NAME,
        client_uri=BRAND_URL,
        logo_uri=SEAL_URL,
        redirect_uris=[f"http://127.0.0.1:{spec.auth.oauth.redirect_port}/callback"],
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        scope=spec.auth.oauth.scopes or None,
    )
    # A statically pre-registered client is configured by seeding `client_info`
    # into token storage, not by a field on this model: `OAuthClientMetadata`
    # has no `client_id` and forbids extras, so assigning one raises. No
    # connector sets `oauth.client_id` today; the first that does should seed
    # storage instead.

    class StoredOAuthProvider(OAuthClientProvider):
        """The SDK provider, plus what it forgets between processes.

        `OAuthClientProvider._initialize` loads tokens but not their expiry,
        and `is_token_valid()` reads a missing expiry as valid. A provider
        built from disk (the hub builds one per connect) therefore never
        refreshed: it sent an expired token, took the 401 into the full
        browser flow, and failed without a browser. So the expiry is restored
        here, and a file that never recorded one (0.9.0) reads as expired, so
        a refresh is tried first. The authorization server metadata is kept
        too: without it the refresh goes to `<host>/token`, which is not
        every server's token endpoint.
        """

        async def _initialize(self) -> None:
            await super()._initialize()
            storage = self.context.storage
            recorded, expires_at = storage.get_expiry()
            if self.context.current_tokens is not None:
                if not recorded:
                    self.context.token_expiry_time = 1.0      # long expired
                elif expires_at is not None:
                    self.context.token_expiry_time = float(expires_at)
            if self.context.oauth_metadata is None:
                self.context.oauth_metadata = storage.get_oauth_metadata()

        async def _handle_token_response(self, response) -> None:
            await super()._handle_token_response(response)
            self.context.storage.set_oauth_metadata(self.context.oauth_metadata)

        async def _handle_refresh_response(self, response) -> bool:
            ok = await super()._handle_refresh_response(response)
            if ok:
                self.context.storage.set_oauth_metadata(
                    self.context.oauth_metadata)
            return ok

    return StoredOAuthProvider(
        server_url=spec.url,
        client_metadata=metadata,
        storage=FileTokenStorage(token_path(root, spec.id)),
        redirect_handler=redirect_handler,
        callback_handler=callback_handler,
    )


class NoTokensError(ValueError):
    """An OAuth connector this edge has never been logged in to.

    A type of its own so the connector report can classify it as "needs a
    login" without reading the message. A ValueError still, so nothing that
    caught the old one changes.
    """


def no_tokens_message(spec: ConnectorSpec) -> str:
    """What to tell the operator when a connector has no OAuth tokens yet.

    Agents read this string and the web UI displays it, so it has to name a
    command that will actually work. The edge login is the only connector login there is: the platform
    has none, because OAuth tokens live only on the edge.
    """
    return (f"connector {spec.id!r} has no OAuth tokens; "
            f"run `nakagai-edge login {spec.id}` first")


def build_http_client(spec: ConnectorSpec, root: Path):
    """The HTTP client the streamable-HTTP transport rides on.

    httpx2, not httpx: the SDK's transport and its `OAuthClientProvider` are
    both written against httpx2, which mcp 2.x brings as its own distribution.
    This package's own platform calls (edge/client.py) stay on httpx, so both
    names are live here and neither is a leftover.
    """
    import httpx2
    from mcp.shared._httpx_utils import create_mcp_http_client

    auth = None
    if spec.auth.mode == "oauth":
        if not has_oauth_tokens(root, spec.id):
            raise NoTokensError(no_tokens_message(spec))
        auth = build_oauth_provider(spec, root)

    client = create_mcp_http_client(
        headers=resolve_headers(spec) or None,
        timeout=httpx2.Timeout(spec.timeout_s, read=spec.timeout_s),
        auth=auth,
    )
    # Servers mounted at /mcp/ answer /mcp with a 307. The client does not
    # follow redirects by default and the MCP client surfaces that as a bare
    # HTTPStatusError, so a URL missing its trailing slash looks like a dead
    # server. 307 preserves method and body, so following is safe for POST.
    client.follow_redirects = True
    return client
