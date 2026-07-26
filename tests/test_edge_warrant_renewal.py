"""Renewal exists to defeat expiry. It may narrow a warrant's authority or
extend its clock; it may NEVER widen it."""

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    apply_renewals, load, record, renewal_request,
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
