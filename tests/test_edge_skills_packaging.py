"""The defect being fixed is that skills were absent from the ARTIFACT while
present in the repo, so every assertion here reads a built wheel. A test that
globbed nakagai_edge/skills/ in the source tree would have passed throughout the
entire period the bug existed."""
import re
import subprocess
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXPECTED = {"connect-edge", "pair-agent", "verify-edge",
            "daily-brief", "halt", "check-the-evidence",
            "nakagai-chat", "verify", "candidate-trader"}


@pytest.fixture(scope="module")
def wheel(tmp_path_factory) -> zipfile.ZipFile:
    # The `uv` binary, not `python -m uv`: uv is not a dependency of this
    # project, so there is no uv module in the venv to run. CI puts the binary
    # on PATH via astral-sh/setup-uv, and so does any machine that builds here.
    out = tmp_path_factory.mktemp("wheel")
    proc = subprocess.run(["uv", "build", "--wheel", "-o", str(out)],
                          cwd=REPO, capture_output=True, text=True)
    assert proc.returncode == 0, f"uv build failed:\n{proc.stderr}"
    built = list(out.glob("*.whl"))
    assert len(built) == 1, f"expected one wheel, got {built}"
    return zipfile.ZipFile(built[0])


def test_every_skill_ships_in_the_wheel(wheel):
    shipped = {
        name.split("/")[2]
        for name in wheel.namelist()
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md")
    }
    assert shipped == EXPECTED, (
        f"skill bundle drift: missing={EXPECTED - shipped}, "
        f"undocumented={shipped - EXPECTED}")


def test_readme_names_the_exact_shipped_skill_bundle():
    readme = (REPO / "README.md").read_text()
    section = readme.split("## Skills", 1)[1].split("## Live chat", 1)[0]
    documented = set(re.findall(r"^- \*\*`([^`]+)`\*\*:", section, re.MULTILINE))
    assert documented == EXPECTED, (
        f"README skill inventory drift: missing={EXPECTED - documented}, "
        f"extra={documented - EXPECTED}")


def test_shipped_skills_are_not_empty(wheel):
    for name in wheel.namelist():
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md"):
            assert len(wheel.read(name)) > 200, f"{name} is suspiciously small"


def test_every_skill_has_name_and_description(wheel):
    for name in wheel.namelist():
        if name.startswith("nakagai_edge/skills/") and name.endswith("/SKILL.md"):
            body = wheel.read(name).decode()
            assert body.startswith("---\n"), f"{name} has no frontmatter"
            head = body.split("---")[1]
            assert "name:" in head and "description:" in head, f"{name} frontmatter is incomplete"


def test_paired_agent_skill_teaches_room_aware_chat_protocol(wheel):
    body = wheel.read("nakagai_edge/skills/pair-agent/SKILL.md").decode()

    assert "send_message(text)" not in body
    assert "no idempotency key" not in body
    assert "one JSON object per eligible event, on stdout" in body
    for required in (
        "list_peers()",
        "claim_message(message_seq)",
        "`seq`, `kind`, `at`, and `cursor`",
        "room_id",
        "reply_to_seq",
        "sender_agent_id",
        "dispatch_mode",
        "response_required",
        "claim_required",
        "claim_expires_at",
        "retry_at",
        "recipient_status",
        "recipient_count",
        "source_seq",
        "hop_count",
        "idempotency_key",
        "request_peer(agent_ids, text, idempotency_key, source_seq=0)",
    ):
        assert required in body


def test_live_chat_skill_teaches_the_same_room_aware_protocol(wheel):
    body = wheel.read("nakagai_edge/skills/nakagai-chat/SKILL.md").decode()

    assert "prints owner messages as JSON" not in body
    assert "response_required" in body
    assert "claim_required" in body
    assert "claim_message(message_seq)" in body
    assert "send_message(text, room_id, idempotency_key, reply_to_seq=0)" in body


def test_candidate_trader_skill_bounds_one_terminal_decision(wheel):
    """Removing the terminal boundary could let a wake process keep trading."""
    body = wheel.read("nakagai_edge/skills/candidate-trader/SKILL.md").decode()

    for required in (
        "## The bounded decision",
        "## One-event loop",
        "## Boundaries",
        "accept_candidate(candidate_id, rationale)",
        "abstain_candidate(candidate_id, rationale)",
        "inspect one candidate",
        "choose accept or abstain",
        "provide a concise rationale",
        "stop after one durable decision",
        "Accepting does not authorize changing any prepared field.",
        "Local policy or the brake can still refuse execution.",
        "The platform owns quantity and price.",
        "The model cannot select or edit quantity or price.",
    ):
        assert required in body


def test_published_candidate_guidance_rejects_retired_order_authority():
    """A stale instruction could give the resident agent money-moving scope."""
    docs = [
        (REPO / "README.md").read_text(),
        (REPO / "nakagai_edge/skills/candidate-trader/SKILL.md").read_text(),
    ]
    forbidden = (
        "market_args",
        "raw model-authored order",
        "raw model order",
        "agent chooses quantity",
        "agent chooses price",
        "agent sets quantity",
        "agent sets price",
    )
    lowered = "\n".join(docs).lower()
    assert not any(term in lowered for term in forbidden)


def test_readme_keeps_candidate_inspection_read_only():
    """A candidate wake must retain evidence reads without gaining other writes."""
    readme = " ".join((REPO / "README.md").read_text().split())

    assert "Read-only inspection remains allowed." in readme
    assert (
        "The edge enforces `accept_candidate` and `abstain_candidate` as the "
        "only write actions for the same candidate during that wake."
    ) in readme
    assert "refused in code until the wake ends or expires" in readme


def test_readme_states_the_strict_entry_and_market_exit_contract():
    readme = " ".join((REPO / "README.md").read_text().split())

    assert "frozen candidate executor creates limit entries only" in readme
    assert "positive whole-share count" in readme
    assert "both the limit and protective stop must be positive" in readme
    assert "Market orders carrying either priced field are refused" in readme
    assert "reduce-only warrant exits" in readme
