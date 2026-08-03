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

# Discovered, never listed. A hand-written roster is a guard that narrows every
# time someone adds a module, and it narrows silently: the suite stays green
# while covering less. Walking the package means a new module is covered the
# moment it exists, which is the only way this stays honest.
_DISCOVER = textwrap.dedent("""
    import importlib, json, pkgutil, sys
    import nakagai_edge
    names = []
    for mod in pkgutil.walk_packages(nakagai_edge.__path__, "nakagai_edge."):
        names.append(mod.name)
    print(json.dumps(sorted(names)))
""")


def _modules() -> list[str]:
    import json
    out = subprocess.run([sys.executable, "-c", _DISCOVER],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def test_discovery_actually_finds_the_package():
    """A walk that returns nothing would make every assertion below vacuous, and
    a guard that passes by scanning zero files is worse than no guard."""
    found = _modules()
    assert len(found) > 15, f"discovery found only {found}"
    assert "nakagai_edge.edge.runtime" in found
    assert "nakagai_edge.edge.skills" in found


def _closure() -> set[str]:
    script = textwrap.dedent(f"""
        import importlib, json, sys
        for m in {_modules()!r}:
            importlib.import_module(m)
        print(json.dumps(sorted(sys.modules)))
    """)
    import json
    proc = subprocess.run([sys.executable, "-c", script],
                          capture_output=True, text=True)
    # Not check=True: a banned module that is not installed fails here as an
    # ImportError, and CalledProcessError would bury the one line that says
    # which module and why. Surface the traceback instead.
    assert proc.returncode == 0, (
        f"importing the edge modules failed, so the weight guard could not "
        f"run:\n{proc.stderr}")
    return set(json.loads(proc.stdout))


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
