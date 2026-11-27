"""Downstream credentials: env-var indirection for tokens, files for OAuth.

No secret ever lands in `config/connectors.yaml`: `bearer`/`headers` name
environment variables and this module resolves them at connect time. OAuth
tokens persist under `secrets/tokens/<connector-id>.json` (mode 0600, and
`secrets/` is gitignored).
"""

import contextlib
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


EXPIRY_MARGIN_S = 60


class TokenFileError(ValueError):
    """A token file that exists but cannot be read as JSON. It is never
    merged into and overwritten: that would leave a file with no tokens and
    no client registration."""


def _read_token_file(path: Path) -> dict:
    """The document, {} for a missing or empty file, TokenFileError for one
    that does not parse."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        return {}
    if not text.strip():
        return {}
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise TokenFileError(f"{path} is not valid JSON; not touching it") from e
    if not isinstance(doc, dict):
        raise TokenFileError(f"{path} is not a JSON object; not touching it")
    return doc


class FileTokenStorage:
    """`mcp.client.auth.TokenStorage` backed by a 0600 file.

    Structural typing: the SDK's TokenStorage is a Protocol, so this needs no
    base class, only the four methods.

    Two processes write this file: the daemon on every refresh, and
    `nakagai-edge login`. So every write is a whole read-modify-write under
    an exclusive lock on a sidecar file, through a unique temp file created
    0600, fsynced, then renamed into place; and a file that does not parse
    is refused rather than merged into an empty document.

    `login_url` is set only for the storage `nakagai-edge login` uses: its
    writes stamp `login_at` (when this login happened, which is how the hub
    tells a fresh login from the daemon's own refresh) and `server_url` (the
    server these tokens belong to).
    """

    def __init__(self, path: Path, *, login_url: str = "") -> None:
        self.path = Path(path)
        self.login_url = login_url
        # The provider's current authorization server metadata, so it rides
        # in the same write as the tokens it belongs to.
        self.metadata_source = lambda: None

    def _read(self) -> dict:
        try:
            return _read_token_file(self.path)
        except TokenFileError:
            if self.login_url:
                return {}          # a fresh login replaces a broken file
            raise

    def _update(self, changes: dict) -> None:
        import fcntl
        import tempfile

        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock_path = self.path.with_name(self.path.name + ".lock")
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            doc = {**self._read(), **changes}
            fd, tmp = tempfile.mkstemp(dir=self.path.parent,
                                       prefix=f".{self.path.name}.",
                                       suffix=".tmp")   # mkstemp creates 0600
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(json.dumps(doc, indent=2))
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
                raise
        finally:
            os.close(lock_fd)

    async def get_tokens(self):
        from mcp.shared.auth import OAuthToken
        raw = self._read().get("tokens")
        return OAuthToken.model_validate(raw) if raw else None

    async def set_tokens(self, tokens) -> None:
        # The absolute expiry, computed now from `expires_in`, a margin early
        # so the refresh happens before the broker refuses the token. The SDK
        # keeps it only in memory and never restores it on load (see
        # StoredOAuthProvider), so without this a token read back from disk
        # looks valid forever. None records "the server gave no expiry",
        # which is different from a file that never recorded one.
        now = time.time()
        expires_at = (max(now, now + tokens.expires_in - EXPIRY_MARGIN_S)
                      if tokens.expires_in is not None else None)
        changes = {"tokens": tokens.model_dump(mode="json"),
                   "expires_at": expires_at}
        metadata = self.metadata_source()
        if metadata is not None:
            changes["oauth_metadata"] = metadata.model_dump(mode="json")
        if self.login_url:
            changes.update(login_at=now, server_url=self.login_url)
        self._update(changes)

    def get_expiry(self) -> tuple[bool, float | None]:
        """(recorded, expires_at). `recorded` is False for a file written
        before expiries were kept, and for one whose value is not a number:
        both get the legacy rule (the token is used as it is)."""
        doc = self._read()
        if "expires_at" not in doc:
            return False, None
        value = doc["expires_at"]
        if value is None:
            return True, None
        if isinstance(value, bool) or not isinstance(value, int | float):
            return False, None
        return True, float(value)

    def get_oauth_metadata(self):
        from mcp.shared.auth import OAuthMetadata
        raw = self._read().get("oauth_metadata")
        try:
            return OAuthMetadata.model_validate(raw) if raw else None
        except Exception:      # noqa: BLE001
            return None

    async def get_client_info(self):
        from mcp.shared.auth import OAuthClientInformationFull
        raw = self._read().get("client_info")
        return OAuthClientInformationFull.model_validate(raw) if raw else None

    async def set_client_info(self, client_info) -> None:
        self._update({"client_info": client_info.model_dump(mode="json")})


def _token_doc(root: Path, connector_id: str) -> dict:
    try:
        return _read_token_file(token_path(root, connector_id))
    except (TokenFileError, OSError):
        return {}


def has_oauth_tokens(root: Path, connector_id: str, server_url: str = "") -> bool:
    """Whether `nakagai-edge login <id>` has been run for this connector, and
    (given `server_url`) for the server it points at now. A file with no
    recorded server (0.9.0) is taken as this server's."""
    doc = _token_doc(root, connector_id)
    if not doc.get("tokens"):
        return False
    recorded = doc.get("server_url")
    return not (server_url and recorded and recorded != server_url)


def login_at(root: Path, connector_id: str) -> float | None:
    """When `nakagai-edge login` last wrote this connector's tokens. The
    daemon's own refresh writes never set it."""
    value = _token_doc(root, connector_id).get("login_at")
    return float(value) if isinstance(value, int | float) else None


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
        here. The authorization server metadata is kept too: without it the
        refresh goes to `<host>/token`, which is not every server's token
        endpoint.

        A file that never recorded an expiry (written by 0.9.0) is used as
        it is, exactly as before: forcing a refresh would mean guessing the
        token endpoint, and a wrong guess ends a login that still works. It
        reaches needs_login only if the broker rejects the token, and the
        next `nakagai-edge login` writes the new format. A recorded
        `expires_at: null` is the server saying the token does not expire.
        """

        async def _initialize(self) -> None:
            await super()._initialize()
            storage = self.context.storage
            recorded, expires_at = storage.get_expiry()
            if (self.context.current_tokens is not None and recorded
                    and expires_at is not None):
                self.context.token_expiry_time = expires_at
            if self.context.oauth_metadata is None:
                self.context.oauth_metadata = storage.get_oauth_metadata()

    _check_sdk_hooks(OAuthClientProvider)
    # A provider with handlers is `nakagai-edge login`: its writes stamp the
    # login and the server. The daemon's provider (no handlers) never does.
    storage = FileTokenStorage(token_path(root, spec.id),
                               login_url=spec.url if redirect_handler else "")
    provider = StoredOAuthProvider(
        server_url=spec.url,
        client_metadata=metadata,
        storage=storage,
        redirect_handler=redirect_handler,
        callback_handler=callback_handler,
    )
    # The metadata the SDK discovers (or _initialize restores) rides in the
    # same write as the tokens: one write per refresh.
    storage.metadata_source = lambda: provider.context.oauth_metadata
    for attr in ("token_expiry_time", "oauth_metadata", "storage",
                 "current_tokens"):
        if not hasattr(provider.context, attr):
            _sdk_drift(f"OAuthContext.{attr}")
    return provider


# What StoredOAuthProvider relies on inside the SDK. All private; mcp is
# capped in pyproject.toml, and this check makes a rename fail loudly at
# construction instead of silently bringing back the never-refreshed token.
_SDK_HOOKS = ("_initialize", "_handle_token_response", "_handle_refresh_response")


def _sdk_drift(what: str):
    from importlib.metadata import version
    raise RuntimeError(
        f"the installed mcp {version('mcp')} no longer has {what}, which "
        "nakagai-edge's OAuth token refresh hooks; install a nakagai-edge "
        "release built for this mcp")


def _check_sdk_hooks(provider_cls) -> None:
    for name in _SDK_HOOKS:
        if not callable(getattr(provider_cls, name, None)):
            _sdk_drift(f"OAuthClientProvider.{name}")


class NoTokensError(ValueError):
    """An OAuth connector this edge has never been logged in to.

    A type of its own so the connector report can classify it as "needs a
    login" without reading the message. A ValueError still, so nothing that
    caught the old one changes.
    """


def no_tokens_message(spec: ConnectorSpec) -> str:
    """What to tell the operator when a connector has no OAuth tokens yet.

    Agents read this string and the web UI displays it, so it has to name a
    command that will actually work. The edge login is the only connector
    login there is: the platform has none, because OAuth tokens live only on
    the edge.
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
        recorded = _token_doc(root, spec.id).get("server_url")
        if recorded and recorded != spec.url:
            # The registry moved this connector to another server. Its
            # Bearer token (and refresh token) belong to the old one.
            raise NoTokensError(
                f"connector {spec.id!r} holds tokens for {recorded}, but now "
                f"points at {spec.url}; run `nakagai-edge login {spec.id}`")
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
