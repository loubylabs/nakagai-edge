"""Reading the packaged skills, and serving them over MCP."""

import json
from pathlib import Path

import httpx
import pytest

from nakagai_edge.edge import skills

pytest.importorskip("mcp")

from nakagai_edge.edge.audit import EdgeAudit  # noqa: E402
from nakagai_edge.edge.brake import Brake  # noqa: E402
from nakagai_edge.edge.client import PlatformClient  # noqa: E402
from nakagai_edge.edge.runtime import build_hub, create_edge_mcp  # noqa: E402
from nakagai_edge.edge.state import EdgeState  # noqa: E402
from nakagai_edge.edge.sync import BUNDLE_SCHEMA, apply_bundle  # noqa: E402


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Reporter:
    """Stub for PortfolioReporter: these tests exercise the skill surface, not
    the portfolio path, so a no-op stand-in keeps their intent unchanged."""

    async def snapshot_and_push(self):
        return {"connectors": []}


@pytest.fixture
def edge_mcp(tmp_path):
    """The real MCPServer instance create_edge_mcp builds.

    No shared fixture for this exists: test_edge_runtime.py builds one inline
    and test_edge_channel.py has a local `_edge_mcp` helper that needs the
    platform package. This mirrors the runtime module's shape without taking
    that dependency.
    """
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1", "schema_version": BUNDLE_SCHEMA,
                         "connectors": {"connectors": []},
                         "signing_public_key": "k"}, "v1")
    client = PlatformClient(
        "https://api.test", "t",
        transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    return create_edge_mcp(state, hub, client, audit, _Reporter(),
                           Brake(state, hub, client, audit))


def test_list_skills_returns_the_canonical_eight():
    assert skills.list_skills() == [
        "check-the-evidence",
        "connect-edge",
        "daily-brief",
        "halt",
        "nakagai-chat",
        "pair-agent",
        "verify",
        "verify-edge",
    ]


def test_read_skill_returns_the_body():
    body = skills.read_skill("halt")
    assert body.startswith("---")
    assert "kill switch" in body.lower()


def test_read_skill_raises_for_unknown():
    with pytest.raises(KeyError):
        skills.read_skill("no-such-skill")


def test_description_comes_from_frontmatter():
    assert "stop" in skills.skill_description("halt").lower()


def test_pair_agent_covers_direct_and_edge_onboarding():
    body = skills.read_skill("pair-agent")
    assert "codex mcp add" in body
    assert "uv tool install nakagai-edge" in body
    assert "Do not copy a direct-mode token" in body
    assert "list_connector_tools" in body


def test_access_does_not_use_file_path_arithmetic():
    """importlib.resources works from a zipimport; __file__ math does not. We
    assert on the mechanism because the failure only shows up in a packaging
    mode nobody runs locally."""
    src = Path(skills.__file__).read_text()
    assert "importlib.resources" in src or "importlib import resources" in src
    assert "__file__" not in src


@pytest.mark.anyio
async def test_every_skill_is_offered_as_a_prompt(edge_mcp):
    """list_prompts() is the public coroutine on the installed MCP SDK's
    MCPServer. Asserting through it rather than through _prompt_manager keeps
    the test on the surface a client actually sees."""
    offered = {p.name for p in await edge_mcp.list_prompts()}
    assert set(skills.list_skills()) <= offered


@pytest.mark.anyio
async def test_the_index_resource_names_every_skill(edge_mcp):
    body = next(iter(await edge_mcp.read_resource("nakagai://skills"))).content
    index = json.loads(body)
    assert set(index) == set(skills.list_skills())
    assert index["halt"] == skills.skill_description("halt")


@pytest.mark.anyio
async def test_a_skill_resource_serves_its_full_text(edge_mcp):
    body = next(iter(await edge_mcp.read_resource("nakagai://skills/halt"))).content
    assert body == skills.read_skill("halt")
