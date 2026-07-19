"""One filename-safe path component, for values that arrive from a remote caller.

Lives here rather than in nakagai.paths because nakagai_edge.config validates connector
ids with it, and this package must not import the platform. The rest of nakagai.paths
(nakagai_root, resolve_root_path, safe_relpath, safe_ticker, safe_date) is the
platform's and stays there.
"""

import re

_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def safe_slug(value: str, *, label: str = "name") -> str:
    """A single filename-safe component: alnum start, then alnum/dot/dash/underscore.

    Rejects path separators, `..`, absolute paths, and leading dots by construction.
    """
    v = (value or "").strip()
    if not _SLUG.match(v):
        raise ValueError(
            f"{label} must be 1-64 chars, start alphanumeric, and contain only "
            f"letters, digits, '.', '_' or '-': {value!r}"
        )
    return v
