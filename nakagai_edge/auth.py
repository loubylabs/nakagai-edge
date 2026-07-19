"""Downstream credentials: env-var indirection for tokens, files for OAuth.

No secret ever lands in `config/connectors.yaml`: `bearer`/`headers` name
environment variables and this module resolves them at connect time. OAuth
tokens persist under `secrets/tokens/<connector-id>.json` (mode 0600, and
`secrets/` is gitignored).
"""

import json
import os
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
        self._write({**self._read(), "tokens": tokens.model_dump(mode="json")})

    async def get_client_info(self):
        from mcp.shared.auth import OAuthClientInformationFull
        raw = self._read().get("client_info")
        return OAuthClientInformationFull.model_validate(raw) if raw else None

    async def set_client_info(self, client_info) -> None:
        self._write({**self._read(), "client_info": client_info.model_dump(mode="json")})


def has_oauth_tokens(root: Path, connector_id: str) -> bool:
    """Whether `nakagai connectors login <id>` has been run for this connector."""
    path = token_path(root, connector_id)
    if not path.exists():
        return False
    try:
        return bool(json.loads(path.read_text()).get("tokens"))
    except (json.JSONDecodeError, OSError):
        return False


def build_oauth_provider(spec: ConnectorSpec, root: Path,
                         redirect_handler=None, callback_handler=None):
    """An `httpx.Auth` that attaches (and silently refreshes) OAuth tokens.

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

    return OAuthClientProvider(
        server_url=spec.url,
        client_metadata=metadata,
        storage=FileTokenStorage(token_path(root, spec.id)),
        redirect_handler=redirect_handler,
        callback_handler=callback_handler,
    )


def no_tokens_message(spec: ConnectorSpec) -> str:
    """What to tell the operator when a connector has no OAuth tokens yet.

    Agents read this string and the web UI displays it, so it has to name a
    command that will actually work. `connectors login` refuses a broker
    outright: broker credentials live only on the edge (docs/internal/EDGE.md),
    so for a broker that advice is a dead end. The role comes off the spec, the
    same registry field `_connector_role` in cli.py reads.
    """
    if spec.role == "broker":
        command = f"uv run nakagai-edge login {spec.id}"
    else:
        command = f"uv run nakagai connectors login {spec.id}"
    return f"connector {spec.id!r} has no OAuth tokens; run `{command}` first"


def build_http_client(spec: ConnectorSpec, root: Path):
    """The httpx client the streamable-HTTP transport rides on."""
    import httpx
    from mcp.shared._httpx_utils import create_mcp_http_client

    auth = None
    if spec.auth.mode == "oauth":
        if not has_oauth_tokens(root, spec.id):
            raise ValueError(no_tokens_message(spec))
        auth = build_oauth_provider(spec, root)

    client = create_mcp_http_client(
        headers=resolve_headers(spec) or None,
        timeout=httpx.Timeout(spec.timeout_s, read=spec.timeout_s),
        auth=auth,
    )
    # Servers mounted at /mcp/ answer /mcp with a 307. httpx does not follow
    # redirects by default and the MCP client surfaces that as a bare
    # HTTPStatusError, so a URL missing its trailing slash looks like a dead
    # server. 307 preserves method and body, so following is safe for POST.
    client.follow_redirects = True
    return client
