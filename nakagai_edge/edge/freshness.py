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


def _is_stable(version: str) -> bool:
    """Only plain numeric versions count as an upgrade to advise.

    The index carries pre-releases, and `_key` would rank 1.0.0rc1 above 1.0.0
    because it reads the chunk as the number 1. The deeper problem is that the
    line we print tells the owner to run `uvx nakagai-edge@latest`, which
    resolves to the latest stable, so naming an rc would advise a version that
    command will not install.
    """
    return all(chunk.isdigit() for chunk in version.split("."))


def newer_release(current: str, *, timeout: float = 3.0, transport=None) -> str | None:
    """The newest published version above `current`, or None.

    None covers every uninteresting case on purpose: already current, no
    network, a rate limit, a malformed index. A version check must never be the
    reason an edge fails to start.
    """
    try:
        with httpx.Client(timeout=timeout, transport=transport) as c:
            body = c.get(_INDEX, headers={"Accept": _ACCEPT}).json()
        stable = [v for v in body["versions"] if _is_stable(v)]
        newest = max(stable, key=_key)
        return newest if _key(newest) > _key(current) else None
    except Exception:
        return None
