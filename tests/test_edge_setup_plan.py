"""The setup planner: what runs, what is skipped, and why. Pure, no I/O.

There is no prompt anywhere in this table. Whether the broker login runs is the
planner's answer alone, and `--no-login` is the only way to say no to it.
"""

import pytest

from nakagai_edge.edge.setup import plan, BROKER

BASE = dict(paired=False, code="ABC", synced=False, has_broker_tokens=False,
            broker_enabled=True, login=True, run_server=True, foreground=False,
            service_supported=True)


def _named(steps):
    return {s.name: s for s in steps}


def test_fresh_edge_runs_everything_and_hands_the_edge_to_the_service():
    steps = plan(**BASE)
    assert [s.name for s in steps] == ["pair", "sync", "login", "service", "run"]
    named = _named(steps)
    assert all(named[n].run for n in ("pair", "sync", "login", "service"))
    assert named["run"].run is False       # the service serves; nothing blocks
    assert named["pair"].reason == "pairing this edge"
    assert named["login"].reason == f"{BROKER} needs a one-time browser login"
    assert "starts by itself at login" in named["service"].reason


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


def test_no_login_flag_skips_a_login_that_would_have_run():
    steps = _named(plan(**{**BASE, "login": False}))
    assert steps["login"].run is False
    assert steps["login"].reason == "skipped (--no-login)"


def test_no_login_flag_does_not_hide_why_no_login_was_needed():
    steps = _named(plan(**{**BASE, "login": False, "has_broker_tokens": True}))
    assert "already has" in steps["login"].reason


def test_no_run_flag_drops_both_the_service_and_the_server():
    steps = _named(plan(**{**BASE, "run_server": False}))
    assert steps["service"].run is False and steps["run"].run is False
    assert steps["service"].reason == "skipped (--no-run)"
    assert steps["run"].reason == "skipped (--no-run)"


def test_no_run_wins_over_foreground():
    steps = _named(plan(**{**BASE, "run_server": False, "foreground": True}))
    assert steps["service"].run is False and steps["run"].run is False


def test_foreground_serves_here_and_installs_no_service():
    steps = _named(plan(**{**BASE, "foreground": True}))
    assert steps["service"].run is False
    assert steps["service"].reason == "skipped (--foreground)"
    assert steps["run"].run is True
    assert "foreground" in steps["run"].reason


def test_a_platform_with_no_service_serves_in_the_foreground():
    steps = _named(plan(**{**BASE, "service_supported": False}))
    assert steps["service"].run is False
    assert "no login service" in steps["service"].reason
    assert steps["run"].run is True


def test_synced_registry_refreshes():
    steps = _named(plan(**{**BASE, "synced": True}))
    assert steps["sync"].run is True
    assert steps["sync"].reason == "refreshing the registry"


def test_unpaired_without_a_code_is_an_error():
    with pytest.raises(ValueError, match="no pairing code"):
        plan(**{**BASE, "paired": False, "code": ""})
