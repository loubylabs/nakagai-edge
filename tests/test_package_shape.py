"""nakagai_edge stands alone: it is what a stranger installs from a wheel."""

import json
import subprocess
import sys


def test_edge_imports_nothing_from_the_platform():
    """A fresh interpreter, not this one: the platform's own tests run in the
    same pytest session and import `nakagai.*` long before this test does, so
    checking `sys.modules` in-process would pass or fail on suite ordering
    rather than on what nakagai_edge itself pulls in."""
    probe = (
        "import nakagai_edge.edge.runtime, nakagai_edge.hub, sys, json\n"
        "leaked = sorted(m for m in sys.modules "
        "if m == 'nakagai' or m.startswith('nakagai.'))\n"
        "print(json.dumps(leaked))\n"
    )
    result = subprocess.run([sys.executable, "-c", probe],
                            capture_output=True, text=True, check=True)
    leaked = json.loads(result.stdout)
    assert leaked == [], f"nakagai_edge reached back into the platform: {leaked}"


def test_the_base_hub_is_the_edges():
    from nakagai_edge.hub import ConnectorError, ConnectorHub, GuardrailDenied  # noqa: F401

    assert not hasattr(ConnectorHub, "decide")
