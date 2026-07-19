"""One-time interactive OAuth login for a downstream connector.

Run on the operator's machine, once per connector:

    uv run nakagai connectors login robinhood-trading

Opens the provider's consent page, catches the redirect on a throwaway
localhost listener, and writes tokens to `secrets/tokens/<id>.json`. The API
server afterwards refreshes those tokens on its own; it never needs a browser.
"""

import asyncio
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PAGE = b"""<!doctype html><meta charset=utf-8><title>Nakagai</title>
<body style="font:16px system-ui;padding:3rem;max-width:34rem">
<h1>Connected.</h1><p>Nakagai stored the token. You can close this tab and
return to the terminal.</p></body>"""


class _CallbackCatcher:
    """A single-shot localhost listener for the OAuth redirect."""

    def __init__(self, port: int) -> None:
        self.port = port
        self._result: tuple[str, str | None] | None = None
        self._done = threading.Event()
        self._server: HTTPServer | None = None

    def _handler(self):
        catcher = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (stdlib naming)
                params = parse_qs(urlparse(self.path).query)
                if "code" in params:
                    catcher._result = (params["code"][0], params.get("state", [None])[0])
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(PAGE)
                else:
                    err = params.get("error", ["missing ?code"])[0]
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(f"authorization failed: {err}".encode())
                catcher._done.set()

            def log_message(self, *a):  # keep the console clean
                pass

        return Handler

    def start(self) -> None:
        self._server = HTTPServer(("127.0.0.1", self.port), self._handler())
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()

    async def wait(self, timeout: float = 300.0) -> tuple[str, str | None]:
        loop = asyncio.get_running_loop()
        ok = await loop.run_in_executor(None, self._done.wait, timeout)
        if not ok or self._result is None:
            raise TimeoutError("timed out waiting for the OAuth redirect")
        return self._result


async def login(root: Path, connector_id: str) -> dict:
    """Run the browser flow and confirm it by listing the downstream's tools."""
    from mcp.client.session import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared._httpx_utils import create_mcp_http_client

    from nakagai_edge.auth import build_oauth_provider, token_path
    from nakagai_edge.hub import ConnectorHub
    from nakagai_edge.identity import client_info

    spec = ConnectorHub(root).spec(connector_id)
    spec.check_connectable()
    if spec.auth.mode != "oauth":
        raise ValueError(f"connector {connector_id!r} is auth.mode={spec.auth.mode!r}, "
                         f"not oauth; nothing to log in to")

    catcher = _CallbackCatcher(spec.auth.oauth.redirect_port)
    catcher.start()

    async def redirect_handler(url: str) -> None:
        print(f"\nOpening your browser to authorize {spec.name or connector_id}…")
        print(f"If it doesn't open, visit:\n  {url}\n")
        webbrowser.open(url)

    async def callback_handler() -> tuple[str, str | None]:
        return await catcher.wait()

    try:
        provider = build_oauth_provider(spec, root, redirect_handler, callback_handler)
        http_client = create_mcp_http_client(auth=provider)
        # One authenticated round-trip drives the whole flow and proves it worked.
        async with streamable_http_client(spec.url, http_client=http_client) as (r, w, _):
            async with ClientSession(r, w, client_info=client_info()) as session:
                await session.initialize()
                tools = await session.list_tools()
    finally:
        catcher.stop()

    return {"ok": True, "connector": connector_id,
            "tokens": str(token_path(root, connector_id)),
            "tool_count": len(tools.tools),
            "tools": sorted(t.name for t in tools.tools)}
