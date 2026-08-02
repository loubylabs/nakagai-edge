"""Renewal exists to defeat expiry. It may narrow a warrant's authority or
extend its clock; it may NEVER widen it."""

import pytest

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    TERMINAL, apply_renewals, load, record, renewal_request,
)


def _rec(**over):
    rec = {"position_id": "ap_1", "connector_id": "demo",
           "account": "463605220", "symbol": "AAPL", "direction": "long",
           "signal_id": "sig_1", "entry_args": {}, "entry_qty": 100.0,
           "entry_price": 47.55, "stop": 46.20, "confirmed_qty": 60.0,
           "last_confirmed_at": 0.0, "unguarded_qty": 0.0,
           "warrant": {"grant_id": "wr_1", "max_qty": 100.0,
                       "expires_at": 10.0},
           "state": "armed", "opened_at": 1.0}
    rec.update(over)
    return rec


def test_the_request_carries_what_the_platform_needs_to_re_sign(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    ask = renewal_request(state)
    assert ask == [{"position_id": "ap_1", "connector_id": "demo",
                    "account": "463605220", "symbol": "AAPL",
                    "confirmed_qty": 60.0, "signal_id": "sig_1"}]


def test_terminal_positions_are_not_renewed(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(state="fired"))
    assert renewal_request(state) == []


def test_a_fresh_warrant_replaces_the_old_one(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 60.0,
                                    "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_2"


def test_a_renewal_may_never_widen_the_ceiling(tmp_path):
    # A platform bug (or a compromised one) must not be able to hand the edge
    # authority to sell more than the entry ever bought.
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 10_000.0,
                                    "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"


def test_a_renewal_for_an_unknown_position_is_ignored(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, {"ap_99": {"grant_id": "x", "max_qty": 1.0,
                                     "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"


def test_an_unguarded_position_becomes_armed_once_a_warrant_arrives(tmp_path):
    # The version-skew recovery path: the platform shipped warrants after this
    # position was already open.
    state = EdgeState(tmp_path)
    record(state, _rec(warrant=None, state="unguarded"))
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 100.0,
                                    "expires_at": 99999.0}})
    row = load(state)["ap_1"]
    assert row["state"] == "armed"
    assert row["warrant"]["grant_id"] == "wr_2"


# ---- unclassifiable positions: unguarded for a reason no warrant fixes ----
#
# A record with direction "" is not the version-skew case above: its entry
# side (or account) could not be named, so the edge can never safely act on
# it. See executor.py's record_entry, which is the only place that produces
# direction: "".

def test_an_unclassifiable_position_is_absent_from_the_renewal_request(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(direction="", warrant=None, state="unguarded",
                       anomaly="unclassifiable order side"))
    assert renewal_request(state) == []


# ---- final round: the disqualification travels WITH the record ----------
#
# Three times now, a rule enforced at the entry path has been quietly undone
# downstream. `blocked` is written once at entry and never cleared, so the
# renewal path cannot re-arm a record the edge established it can never act
# on. These use the market_args case on purpose: its direction and
# account are both perfectly good, so only `blocked` can catch it.


def _blocked_rec(**over):
    return _rec(blocked="connector declares no market_args",
                anomaly="connector declares no market_args",
                warrant=None, state="unguarded", **over)


def test_a_blocked_position_is_absent_from_the_renewal_request(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _blocked_rec())
    assert renewal_request(state) == []


def test_apply_renewals_refuses_to_arm_a_blocked_position(tmp_path):
    # The reopening in full: sixty seconds after entry the platform answers
    # with a perfectly valid warrant, and before this the record went back to
    # `armed` and reported guarded: true for a brake that can never fire.
    state = EdgeState(tmp_path)
    record(state, _blocked_rec())
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 100.0,
                                    "expires_at": 99999.0}})
    row = load(state)["ap_1"]
    assert row["state"] == "unguarded"
    assert row["warrant"] is None
    assert row["blocked"]


def test_an_account_less_position_is_absent_from_the_renewal_request(tmp_path):
    # Task 4 disqualified the account-less record for the same reason as the
    # side-less one, and the renewal path has to honour BOTH halves of that
    # rule: an empty account is what fire() would ask the broker about, and no
    # warrant makes that question correct.
    state = EdgeState(tmp_path)
    record(state, _rec(account="", warrant=None, state="unguarded",
                       anomaly="no account on the order"))
    assert renewal_request(state) == []


def test_apply_renewals_refuses_to_arm_an_account_less_position(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(account="", warrant=None, state="unguarded",
                       anomaly="no account on the order"))
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 100.0,
                                    "expires_at": 99999.0}})
    row = load(state)["ap_1"]
    assert row["state"] == "unguarded"
    assert row["warrant"] is None


def test_apply_renewals_refuses_to_arm_an_unclassifiable_position(tmp_path):
    # Even handed a warrant that would otherwise be perfectly valid (within
    # the entry ceiling, well-formed), a record the edge cannot act on must
    # not be reported as guarded.
    state = EdgeState(tmp_path)
    record(state, _rec(direction="", warrant=None, state="unguarded",
                       anomaly="unclassifiable order side"))
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 100.0,
                                    "expires_at": 99999.0}})
    row = load(state)["ap_1"]
    assert row["state"] == "unguarded"
    assert row["warrant"] is None


# ---- fix round 1: a non-finite ceiling must never widen a warrant --------
#
# NaN and the infinities parse cleanly through float() and are truthy, so a
# bare `> entry_qty` bound (round 1's version of this check) lets both
# through and then silently disables every downstream ceiling check:
# brake.fire()'s clamp (`min(held, nan)` is `held`) and warrant.authorizes()'s
# `qty > ceiling` (always False against a NaN ceiling). Both of those carry
# their own regression coverage; these two pin the first line of defense.

def test_a_nan_ceiling_is_refused_and_the_existing_warrant_survives(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": float("nan"),
                                    "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"


def test_a_negative_infinite_ceiling_is_refused_and_the_existing_warrant_survives(tmp_path):
    # -inf is the mirror failure: it would replace a working warrant with one
    # that can never authorize anything, so the position reports guarded:
    # True while its brake is permanently dead. Refuse it, same as NaN.
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": float("-inf"),
                                    "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"


def test_a_batch_with_one_malformed_entry_still_applies_the_other(tmp_path):
    # A non-dict warrant value raises AttributeError out of `warrant.get(...)`
    # unless guarded. One caller's bad shape must cost only its own position,
    # not every other renewal riding in the same batch.
    state = EdgeState(tmp_path)
    record(state, _rec())
    record(state, _rec(position_id="ap_2", symbol="MSFT"))
    apply_renewals(state, {
        "ap_1": "not-a-warrant",
        "ap_2": {"grant_id": "wr_2", "max_qty": 50.0, "expires_at": 99999.0},
    })
    doc = load(state)
    assert doc["ap_1"]["warrant"]["grant_id"] == "wr_1"   # untouched
    assert doc["ap_2"]["warrant"]["grant_id"] == "wr_2"   # applied


def test_a_warrants_batch_that_is_a_list_is_survived_without_raising(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    apply_renewals(state, [{"grant_id": "wr_2"}])   # must not raise
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"


@pytest.mark.parametrize("terminal_state", TERMINAL)
def test_a_terminal_position_refuses_an_unsolicited_renewal(tmp_path, terminal_state):
    # renewal_request already keeps the edge from ASKING about a terminal
    # position (covered above); this is the second, independent line of
    # defense, against a buggy or compromised platform answering for a
    # position it was never asked about.
    state = EdgeState(tmp_path)
    record(state, _rec(state=terminal_state))
    apply_renewals(state, {"ap_1": {"grant_id": "wr_2", "max_qty": 60.0,
                                    "expires_at": 99999.0}})
    assert load(state)["ap_1"]["warrant"]["grant_id"] == "wr_1"
