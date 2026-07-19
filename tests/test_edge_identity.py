"""package_version() must resolve the edge's own distribution first.

Standalone (uvx/pip install nakagai-edge) there is no "nakagai" distribution to
fall back to, so resolving "nakagai" first silently degraded to "0.0.0" and
misreported the edge's identity to every downstream MCP broker. See
nakagai_edge.identity.client_info, which feeds this into every clientInfo we
send.
"""

from nakagai_edge.identity import package_version


def test_package_version_resolves_without_raising():
    # Both "nakagai-edge" and "nakagai" are installed editable in this dev
    # environment, so either order would "work" here; the real regression this
    # guards is a raise or a silent "0.0.0", not which one wins.
    version = package_version()
    assert isinstance(version, str)
    assert version != "0.0.0"


def test_package_version_prefers_its_own_distribution(monkeypatch):
    """Standalone, "nakagai" is absent. Fall through to it must not be the
    first thing tried, or a standalone install always reports 0.0.0."""
    import nakagai_edge.identity as identity

    seen = []

    def fake_version(dist):
        seen.append(dist)
        if dist == "nakagai-edge":
            return "0.1.0"
        raise identity.PackageNotFoundError(dist)

    monkeypatch.setattr(identity, "version", fake_version)
    assert identity.package_version() == "0.1.0"
    assert seen[0] == "nakagai-edge"
