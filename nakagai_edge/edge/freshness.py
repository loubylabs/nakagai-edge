"""Is a newer nakagai-edge published?

Advisory only, and never fatal. The hard compatibility gate already exists in
sync.apply_bundle (BUNDLE_SCHEMA), which refuses a bundle this edge cannot fully
read. Adding a second refusal here would be two mechanisms answering the same
question, and the daemon holds broker credentials, so nothing about this file
ever updates anything by itself. It tells the owner; the owner decides.
"""

import httpx

_INDEX = "https://pypi.org/simple/nakagai-edge/"
_ACCEPT = "application/vnd.pypi.simple.v1+json"


def _key(version: str) -> tuple:
    """Sort key that tolerates anything unexpected rather than raising."""
    parts = []
    for chunk in version.split("."):
        digits = "".join(c for c in chunk if c.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def newer_release(current: str, *, timeout: float = 3.0, transport=None) -> str | None:
    """The newest published version above `current`, or None.

    None covers every uninteresting case on purpose: already current, no
    network, a rate limit, a malformed index. A version check must never be the
    reason an edge fails to start.
    """
    try:
        with httpx.Client(timeout=timeout, transport=transport) as c:
            body = c.get(_INDEX, headers={"Accept": _ACCEPT}).json()
        versions = body["versions"]
        newest = max(versions, key=_key)
        return newest if _key(newest) > _key(current) else None
    except Exception:
        return None
