"""What nakagai_edge is allowed to weigh.

This package exists so a stranger can `uvx nakagai-edge setup <code>` without
pulling pandas, numpy and pyarrow onto their laptop to place an order. That is a
property of the import graph, and import graphs rot silently: one convenient
`import pandas` at module scope in hub.py and the wheel still builds, the tests
still pass, and the install breaks for someone who cannot read the traceback.

Run in a subprocess: this process has the whole platform imported already.
"""

import subprocess
import sys
import textwrap

BANNED = ("pandas", "numpy", "pyarrow", "yfinance", "anthropic")

MODULES = [
    "nakagai_edge._env", "nakagai_edge.approvals", "nakagai_edge.auth",
    "nakagai_edge.cli", "nakagai_edge.config", "nakagai_edge.guardrails",
    "nakagai_edge.hub", "nakagai_edge.identity", "nakagai_edge.oauth_login",
    "nakagai_edge.signing", "nakagai_edge.slug",
    "nakagai_edge.edge.audit", "nakagai_edge.edge.client", "nakagai_edge.edge.executor",
    "nakagai_edge.edge.portfolio", "nakagai_edge.edge.preflight", "nakagai_edge.edge.remote",
    "nakagai_edge.edge.runtime", "nakagai_edge.edge.setup", "nakagai_edge.edge.state",
    "nakagai_edge.edge.sync",
]


def _closure() -> set[str]:
    script = textwrap.dedent(f"""
        import importlib, json, sys
        for m in {MODULES!r}:
            importlib.import_module(m)
        print(json.dumps(sorted(sys.modules)))
    """)
    out = subprocess.run([sys.executable, "-c", script],
                         capture_output=True, text=True, check=True).stdout
    import json
    return set(json.loads(out))


def test_the_edge_carries_no_platform_weight():
    heavy = sorted(m for m in _closure() if m.split(".")[0] in BANNED)
    assert heavy == [], (
        f"nakagai_edge imports {heavy} at module scope. That weight ships to every "
        f"alpha user. If a platform-shaped question crept into the edge, it belongs "
        f"on PlatformHub; see todos/edge-distribution-uvx-spec.md.")


def test_the_edge_never_imports_the_platform():
    leaked = sorted(m for m in _closure()
                    if m == "nakagai" or m.startswith("nakagai."))
    assert leaked == [], (
        f"nakagai_edge imports {leaked}. The wheel a stranger installs does not "
        f"contain the platform, so this is an ImportError in production.")
