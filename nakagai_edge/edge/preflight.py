"""Is this URL actually the nakagai API?

The web app and the API are different servers. A --platform pointed at the web
app answers /api/agents/pair with a Next.js 404 page, and the pairing error
used to be that page, verbatim, in the terminal. One cheap GET turns that into
a sentence.

Two different mistakes reach the same non-200, so they get two different
sentences. A --platform naming the web app is the local-dev case above. A
--platform naming the hosted API over plain http is not the same mistake at
all: it is the right server, one scheme short, and the hosted deployment
answers it with a 301 to https. Handing that user the localhost hint moves
them further from working, since nothing is listening on their :8321.

Redirects are diagnosed here and never followed. These requests carry a
single-use pairing code, and every later one carries an `nk_agent_` bearer
token; replaying either to whatever a Location header names is how a
credential leaves the machine it was scoped to. httpx does not follow
redirects unless asked, so the posture costs nothing to hold, and it is
deliberate rather than an oversight: do not turn follow_redirects on in this
module or in edge/client.py.
"""

import httpx

from nakagai_edge.edge.client import EdgeClientError

WRONG_SERVER = (
    "{url} does not look like the nakagai API (got {what}).\n"
    "the web app and the API are different servers; the API usually listens "
    "on :8321.\n"
    "try: --platform http://127.0.0.1:8321"
)

HTTPS_UPGRADE = (
    "{url} is the nakagai API, but it does not serve plain http "
    "(got HTTP {status} to {target}/api/health).\n"
    "the same address over https is the whole fix.\n"
    "try: --platform {target}"
)

# 303 is absent on purpose: it means "go GET this other thing", not "you asked
# for the right thing at the wrong scheme".
_REDIRECTS = (301, 302, 307, 308)


def _https_upgrade(resp: httpx.Response) -> str | None:
    """The https origin to retry against, when `resp` is a redirect that only
    swaps http for https. None for every other response, including a redirect
    that points somewhere genuinely different: that one is a wrong server, and
    saying "just add an s" about it would be a lie.
    """
    if resp.status_code not in _REDIRECTS:
        return None
    location = resp.headers.get("location", "")
    if not location:
        return None
    try:
        target = httpx.URL(location)
    except httpx.InvalidURL:
        return None
    asked = resp.request.url
    if asked.scheme != "http" or target.scheme != "https":
        return None
    # Host and path only. The port rides along in netloc below, so a redirect
    # that also moves the port is still a scheme upgrade as far as the remedy
    # is concerned, and the remedy quotes the port the server itself named.
    if target.host != asked.host or target.path != asked.path:
        return None
    return f"{target.scheme}://{target.netloc.decode('ascii')}"


def check_platform(platform_url: str, *, transport=None) -> None:
    """Raise EdgeClientError unless `platform_url` serves the nakagai API."""
    url = platform_url.rstrip("/")
    try:
        with httpx.Client(base_url=url, timeout=10.0, transport=transport) as c:
            resp = c.get("/api/health")
    except httpx.HTTPError as e:
        raise EdgeClientError(f"could not reach {url}: {e}") from e

    if resp.status_code != 200:
        target = _https_upgrade(resp)
        if target is not None:
            raise EdgeClientError(HTTPS_UPGRADE.format(
                url=url, status=resp.status_code, target=target))
        raise EdgeClientError(WRONG_SERVER.format(
            url=url, what=f"HTTP {resp.status_code} from /api/health"))
    try:
        resp.json()
    except ValueError:
        raise EdgeClientError(WRONG_SERVER.format(
            url=url, what="a non-JSON body from /api/health")) from None
