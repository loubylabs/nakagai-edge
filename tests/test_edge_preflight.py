"""A --platform pointed at the web app must say so, not dump an HTML page."""

import httpx
import pytest

from nakagai_edge.edge.client import EdgeClientError
from nakagai_edge.edge.preflight import check_platform

NEXT_404 = "<!DOCTYPE html><html><title>404: This page could not be found.</title></html>"


def _transport(handler):
    return httpx.MockTransport(handler)


def test_html_404_names_the_likely_cause():
    def handler(request):
        return httpx.Response(404, text=NEXT_404)

    with pytest.raises(EdgeClientError) as e:
        check_platform("http://localhost:3100", transport=_transport(handler))
    msg = str(e.value)
    assert "does not look like the nakagai API" in msg
    assert "8321" in msg
    assert "<!DOCTYPE" not in msg          # the page body never reaches the user


def test_healthy_api_passes():
    def handler(request):
        assert request.url.path == "/api/health"
        return httpx.Response(200, json={"ok": True})

    check_platform("http://127.0.0.1:8321", transport=_transport(handler))


def test_unreachable_platform_says_so():
    def handler(request):
        raise httpx.ConnectError("nope", request=request)

    with pytest.raises(EdgeClientError, match="could not reach"):
        check_platform("http://127.0.0.1:9999", transport=_transport(handler))


def test_http_to_https_redirect_names_the_scheme():
    """The hosted API 301s http to https. The old message sent that user to
    127.0.0.1:8321, where nothing was listening, so the advice made it worse."""
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(
            301, headers={"location": "https://api.nakag.ai/api/health"})

    with pytest.raises(EdgeClientError) as e:
        check_platform("http://api.nakag.ai", transport=_transport(handler))
    msg = str(e.value)
    assert "--platform https://api.nakag.ai" in msg
    assert "8321" not in msg               # the local-dev hint is wrong here
    assert "127.0.0.1" not in msg
    # The redirect is diagnosed, never followed: this request is one hop away
    # from carrying a pairing code.
    assert seen == ["http://api.nakag.ai/api/health"]


def test_https_upgrade_keeps_a_nonstandard_port():
    def handler(request):
        return httpx.Response(
            308, headers={"location": "https://box.local:8443/api/health"})

    with pytest.raises(EdgeClientError) as e:
        check_platform("http://box.local:8443", transport=_transport(handler))
    assert "--platform https://box.local:8443" in str(e.value)


def test_redirect_somewhere_else_keeps_the_wrong_server_message():
    """A redirect to a different host or path is not a scheme upgrade, and the
    web-app-vs-API hint is the right guess for it."""
    def handler(request):
        return httpx.Response(
            302, headers={"location": "https://www.nakag.ai/login"})

    with pytest.raises(EdgeClientError) as e:
        check_platform("http://www.nakag.ai", transport=_transport(handler))
    msg = str(e.value)
    assert "does not look like the nakagai API" in msg
    assert "8321" in msg


def test_relative_redirect_keeps_the_wrong_server_message():
    def handler(request):
        return httpx.Response(307, headers={"location": "/api/health/"})

    with pytest.raises(EdgeClientError) as e:
        check_platform("http://localhost:3100", transport=_transport(handler))
    assert "does not look like the nakagai API" in str(e.value)
