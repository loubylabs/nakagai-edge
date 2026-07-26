"""Arming, disarming, and the tick that ties observation to firing.

Disarming must work with no network at all: a brake you cannot release
because the platform is down is not a safety feature.
"""

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.edge.brake import (
    armed, clear_local_disarm, disarmed_positions, set_local_disarm,
)
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.sync import apply_bundle


def _state(tmp_path, mandate=None):
    state = EdgeState(tmp_path)
    state.save_agent("https://api.test", "ag1", "nk_agent_t")
    apply_bundle(state, {"bundle_version": "v1",
                         "connectors": {"connectors": []}, "watchlist": [],
                         "mandate": mandate or {}, "strategy_configs": {},
                         "signing_public_key": "k"}, "v1")
    return state


def test_the_brake_ships_armed(tmp_path):
    # A platform that has never heard of the dial still gets a live brake.
    assert armed(_state(tmp_path)) is True


def test_the_owner_can_disarm_it_from_the_platform(tmp_path):
    assert armed(_state(tmp_path, {"brake_armed": False})) is False


def test_local_disarm_works_with_no_network(tmp_path):
    state = _state(tmp_path)
    set_local_disarm(state, all_positions=True)
    assert armed(state) is False


def test_local_disarm_is_reversible(tmp_path):
    state = _state(tmp_path)
    set_local_disarm(state, all_positions=True)
    clear_local_disarm(state)
    assert armed(state) is True


def test_a_single_position_can_be_released_without_disarming_the_rest(tmp_path):
    state = _state(tmp_path)
    set_local_disarm(state, position_id="ap_1")
    assert armed(state) is True
    assert disarmed_positions(state) == {"ap_1"}


def test_a_corrupt_disarm_file_disarms_rather_than_crashing(tmp_path):
    # Fail SAFE, not closed: an unreadable brake file means we cannot know the
    # owner's intent, and quietly firing on an unknown intent is worse than
    # quietly not firing while saying so.
    state = _state(tmp_path)
    state.brake_off_path.parent.mkdir(parents=True, exist_ok=True)
    state.brake_off_path.write_text("{not json")
    assert armed(state) is False
