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
