import httpx

from nakagai_edge.edge import freshness


def _index(versions):
    return httpx.MockTransport(lambda r: httpx.Response(
        200, json={"versions": versions},
        headers={"content-type": "application/vnd.pypi.simple.v1+json"}))


def test_reports_a_newer_release():
    assert freshness.newer_release(
        "0.1.0", transport=_index(["0.1.0", "0.3.1"])) == "0.3.1"


def test_silent_when_current():
    assert freshness.newer_release(
        "0.3.1", transport=_index(["0.1.0", "0.3.1"])) is None


def test_network_failure_is_never_fatal():
    def boom(request):
        raise httpx.ConnectError("no network")
    assert freshness.newer_release(
        "0.1.0", transport=httpx.MockTransport(boom)) is None


def test_malformed_index_is_never_fatal():
    bad = httpx.MockTransport(lambda r: httpx.Response(200, json={"nope": 1}))
    assert freshness.newer_release("0.1.0", transport=bad) is None


def test_a_prerelease_is_not_advised():
    """The line we print says to run `uvx nakagai-edge@latest`, which resolves
    to the latest stable. Naming an rc would advise a version that command does
    not install, so pre-releases are not upgrades as far as this is concerned.
    """
    assert freshness.newer_release(
        "0.1.0", transport=_index(["0.1.0", "0.2.0rc1"])) is None
    assert freshness.newer_release(
        "0.1.0", transport=_index(["0.1.0", "0.2.0", "0.3.0b2"])) == "0.2.0"


def test_an_index_of_only_prereleases_is_never_fatal():
    assert freshness.newer_release(
        "0.1.0", transport=_index(["0.2.0rc1"])) is None
