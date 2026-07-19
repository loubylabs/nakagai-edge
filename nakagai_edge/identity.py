"""How Nakagai introduces itself to the MCP servers it connects to.

A host renders the `clientInfo` it receives in the `initialize` handshake. Leave
it unset and the SDK sends its own default, `mcp / 0.1.0`, which is why we show
up on a broker's connected-agent screen as an anonymous "Agent". Every
`ClientSession` in this package therefore passes `client_info=client_info()`.

`title`, `websiteUrl` and `icons` arrived with the MCP 2025-11-25 revision; a
host that predates it falls back to `name`, so that carries the brand too.

The icon URL has to be fetchable anonymously by whoever is drawing the screen.
That is why `/brand` is exempt from the web auth guard (`web/middleware.ts`): a
gated URL renders as a broken image, not as a logo.
"""

from importlib.metadata import PackageNotFoundError, version

BRAND_URL = "https://nakag.ai"
# The PNG, not the SVG sitting next to it: hosts routinely refuse to render a
# remote SVG (it can carry script), and a refused icon is an absent icon.
SEAL_URL = f"{BRAND_URL}/brand/seal-512.png"

# Shown to a human on someone else's consent screen, so it is the brand rather
# than our internal vocabulary ("Nakagai Gateway"). The site name, not the
# company name: a broker's connected-agent list reads as a list of sites.
DISPLAY_NAME = "Nakag.ai"


def package_version() -> str:
    """Our version, or a placeholder. Identity must never break a connect."""
    for dist in ("nakagai-edge", "nakagai"):
        try:
            return version(dist)
        except PackageNotFoundError:
            continue
    return "0.0.0"


def client_info():
    """The `clientInfo` every downstream MCP server sees from us."""
    from mcp.types import Icon, Implementation

    # `name` and `title` say the same thing on purpose. A host is free to read
    # either one, and we would rather not care which: both render as the site.
    return Implementation(
        name=DISPLAY_NAME,
        title=DISPLAY_NAME,
        version=package_version(),
        websiteUrl=BRAND_URL,
        icons=[Icon(src=SEAL_URL, mimeType="image/png", sizes=["512x512"])],
    )
