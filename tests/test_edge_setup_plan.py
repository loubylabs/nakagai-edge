"""The setup planner: what runs, what is skipped, and why. Pure, no I/O."""

import pytest

from nakagai_edge.edge.setup import plan, BROKER

BASE = dict(paired=False, code="ABC", synced=False, has_broker_tokens=False,
            broker_enabled=True, run_server=True)


def _named(steps):
    return {s.name: s for s in steps}


def test_fresh_edge_runs_everything():
    steps = plan(**BASE)
    assert [s.name for s in steps] == ["pair", "sync", "login", "run"]
    assert all(s.run for s in steps)
    steps_named = _named(steps)
    assert steps_named["pair"].reason == "pairing this edge"
    assert steps_named["login"].reason == f"{BROKER} needs a one-time browser login"
    assert steps_named["run"].reason == "serving MCP"


def test_already_paired_without_code_skips_pairing():
    steps = _named(plan(**{**BASE, "paired": True, "code": ""}))
    assert steps["pair"].run is False
    assert "already paired" in steps["pair"].reason
    assert steps["sync"].run is True


def test_explicit_code_repairs_even_when_paired():
    steps = _named(plan(**{**BASE, "paired": True, "code": "NEW"}))
    assert steps["pair"].run is True
    assert "re-pairing" in steps["pair"].reason


def test_existing_tokens_skip_login():
    steps = _named(plan(**{**BASE, "has_broker_tokens": True}))
    assert steps["login"].run is False
    assert "already has robinhood-trading tokens" in steps["login"].reason


def test_disabled_broker_skips_login():
    steps = _named(plan(**{**BASE, "broker_enabled": False}))
    assert steps["login"].run is False
    assert "not enabled" in steps["login"].reason


def test_no_run_flag_drops_the_server_step():
    steps = _named(plan(**{**BASE, "run_server": False}))
    assert steps["run"].run is False
    assert steps["run"].reason == "skipped (--no-run)"


def test_synced_registry_refreshes():
    steps = _named(plan(**{**BASE, "synced": True}))
    assert steps["sync"].run is True
    assert steps["sync"].reason == "refreshing the registry"


def test_unpaired_without_a_code_is_an_error():
    with pytest.raises(ValueError, match="no pairing code"):
        plan(**{**BASE, "paired": False, "code": ""})
