"""Is this URL actually the nakagai API?

The web app and the API are different servers. A --platform pointed at the web
app answers /api/agents/pair with a Next.js 404 page, and the pairing error
used to be that page, verbatim, in the terminal. One cheap GET turns that into
a sentence.
"""

import httpx

from nakagai_edge.edge.client import EdgeClientError

WRONG_SERVER = (
    "{url} does not look like the nakagai API (got {what}).\n"
    "the web app and the API are different servers; the API usually listens "
    "on :8321.\n"
    "try: --platform http://127.0.0.1:8321"
)


def check_platform(platform_url: str, *, transport=None) -> None:
    """Raise EdgeClientError unless `platform_url` serves the nakagai API."""
    url = platform_url.rstrip("/")
    try:
        with httpx.Client(base_url=url, timeout=10.0, transport=transport) as c:
            resp = c.get("/api/health")
    except httpx.HTTPError as e:
        raise EdgeClientError(f"could not reach {url}: {e}") from e

    if resp.status_code != 200:
        raise EdgeClientError(WRONG_SERVER.format(
            url=url, what=f"HTTP {resp.status_code} from /api/health"))
    try:
        resp.json()
    except ValueError:
        raise EdgeClientError(WRONG_SERVER.format(
            url=url, what="a non-JSON body from /api/health")) from None
