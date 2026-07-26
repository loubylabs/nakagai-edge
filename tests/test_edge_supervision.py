"""The ledger of supervised positions. Broker truth always wins, and every
drift case must resolve toward a SMALLER exit than before."""

import pytest

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    TERMINAL, claim, held_quantities, is_guarded, ledger_fault, load, mark,
    open_risk, recover_interrupted, reconcile, record, unreadable,
)

# A real warrant carries an epoch float (see warrant.build_warrant_payload),
# and `guarded` now reads it, so the fixture has to carry a real one too.
EXPIRY = 4_102_444_800.0            # 2100-01-01
BEFORE_EXPIRY = EXPIRY - 3600.0


def _rec(**over):
    rec = {"position_id": "ap_1", "connector_id": "demo",
           "account": "463605220", "symbol": "AAPL", "direction": "long",
           "signal_id": "sig_1", "entry_args": {"symbol": "AAPL"},
           "entry_qty": 100.0, "entry_price": 47.55, "stop": 46.20,
           "confirmed_qty": 100.0, "last_confirmed_at": 0.0,
           "unguarded_qty": 0.0,
           "warrant": {"grant_id": "wr_1", "expires_at": EXPIRY},
           "state": "armed", "opened_at": 1.0}
    rec.update(over)
    return rec


def _portfolio(qty, *, error="", symbol="AAPL"):
    return {"connectors": [{"id": "demo", "error": error, "accounts": [
        {"account_number": "463605220", "error": "", "portfolio": {},
         "positions": [{"symbol": symbol, "quantity": str(qty)}]}]}]}


def _portfolio_row(row):
    # A single position row under our own control, for rows that don't fit
    # the plain (symbol, quantity) shape _portfolio() produces.
    return {"connectors": [{"id": "demo", "error": "", "accounts": [
        {"account_number": "463605220", "error": "", "portfolio": {},
         "positions": [row]}]}]}


def test_a_recorded_position_round_trips(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert load(state)["ap_1"]["symbol"] == "AAPL"


def test_an_absent_ledger_is_empty_not_an_error(tmp_path):
    assert load(EdgeState(tmp_path)) == {}


def test_a_corrupt_ledger_is_empty_not_an_error(tmp_path):
    state = EdgeState(tmp_path)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text("{not json")
    assert load(state) == {}


def test_a_corrupt_ledger_is_kept_aside_and_reported(tmp_path):
    # {} stays the return value: five callers depend on load() never raising,
    # and a daemon that dies on a bad byte is worse. But {} on a corrupt file
    # and {} on a fresh one mean opposite things ("no positions" versus "the
    # brake forgot every position it was watching"), so the difference has to
    # survive somewhere the owner can see it.
    state = EdgeState(tmp_path)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text("{not json")
    assert load(state) == {}
    aside = state.supervised_path.with_name("supervised.corrupt.json")
    assert aside.read_text() == "{not json", "the bytes stay recoverable"
    assert "supervised.corrupt.json" in ledger_fault(state)


def test_a_ledger_that_parses_to_the_wrong_shape_is_also_reported(tmp_path):
    state = EdgeState(tmp_path)
    state.supervised_path.parent.mkdir(parents=True, exist_ok=True)
    state.supervised_path.write_text('["not", "a", "ledger"]')
    assert load(state) == {}
    assert ledger_fault(state)


def test_a_healthy_ledger_reports_no_fault(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert load(state)["ap_1"]["symbol"] == "AAPL"
    assert ledger_fault(state) == ""


def test_an_absent_ledger_reports_no_fault(tmp_path):
    assert ledger_fault(EdgeState(tmp_path)) == ""


def test_marking_changes_state_and_carries_extras(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    mark(state, "ap_1", "fired", fired_at=99.0)
    row = load(state)["ap_1"]
    assert row["state"] == "fired"
    assert row["fired_at"] == 99.0


def test_marking_fired_zeroes_the_confirmed_quantity(tmp_path):
    # The exit is placed, so this record carries no risk any more. reconcile()
    # skips TERMINAL, so nothing downstream will ever zero it, and a stale
    # confirmed_qty keeps a closed position inside "portfolio heat if every
    # stop hit at once" for as long as the record exists.
    state = EdgeState(tmp_path)
    record(state, _rec())
    mark(state, "ap_1", "fired", fired_qty=100.0)
    row = load(state)["ap_1"]
    assert row["confirmed_qty"] == 0.0
    assert row["fired_qty"] == 100.0


def test_claim_moves_a_matching_record_and_reports_success(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert claim(state, "ap_1", expected="armed", new="firing") is True
    assert load(state)["ap_1"]["state"] == "firing"


def test_claim_refuses_when_the_state_has_already_moved(tmp_path):
    # The whole point of claim(): a second caller racing to move the same
    # record loses rather than silently overwriting the first caller's claim.
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert claim(state, "ap_1", expected="armed", new="firing") is True
    assert claim(state, "ap_1", expected="armed", new="firing") is False
    assert load(state)["ap_1"]["state"] == "firing"


def test_claim_refuses_on_an_unknown_position(tmp_path):
    assert claim(EdgeState(tmp_path), "nope", expected="armed", new="firing") is False


def test_held_quantities_are_keyed_by_connector_account_and_symbol():
    assert held_quantities(_portfolio(100)) == {
        ("demo", "463605220", "AAPL"): 100.0}


def test_a_reduced_position_clamps_the_confirmed_quantity(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(40))
    row = load(state)["ap_1"]
    assert row["confirmed_qty"] == 40.0
    assert row["state"] == "armed"


def test_an_added_to_position_keeps_the_ceiling_and_reports_the_surplus(tmp_path):
    # The warrant covers 100. The extra 50 is not ours to exit.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(150))
    row = load(state)["ap_1"]
    assert row["confirmed_qty"] == 100.0
    assert row["unguarded_qty"] == 50.0
    assert row["state"] == "armed"


def test_a_closed_position_is_released(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(0, symbol="MSFT"))
    assert load(state)["ap_1"]["state"] == "released"


def test_a_failed_connector_snapshot_never_releases_a_position(tmp_path):
    # THE most dangerous reconciliation bug: a brief broker outage looks
    # exactly like "the position was closed", and releasing on it would
    # disarm the brake at the worst possible moment.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, {"connectors": [
        {"id": "demo", "error": "TimeoutError: dial failed", "accounts": []}]})
    row = load(state)["ap_1"]
    assert row["state"] == "armed"
    assert row["confirmed_qty"] == 100.0


def test_a_fired_position_is_not_reconciled_back_to_armed(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(state="fired"))
    reconcile(state, _portfolio(100))
    assert load(state)["ap_1"]["state"] == "fired"


def test_an_exit_interrupted_mid_flight_becomes_outcome_unknown(tmp_path):
    # The warrant is spent BEFORE the broker call, so a crash in between leaves
    # a record in `firing`. Nothing may guess whether the order landed.
    state = EdgeState(tmp_path)
    record(state, _rec(state="firing"))
    assert recover_interrupted(state) == ["ap_1"]
    assert load(state)["ap_1"]["state"] == "outcome_unknown"


def test_recovery_leaves_every_other_state_alone(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert recover_interrupted(state) == []
    assert load(state)["ap_1"]["state"] == "armed"


def test_open_risk_reports_r_multiple_and_distance_to_stop(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    row = open_risk(state, {"AAPL": 48.90})[0]
    # entry 47.55, stop 46.20, so 1R is 1.35. At 48.90 that is +1.0R.
    assert round(row["unrealized_r"], 3) == 1.0
    assert round(row["distance_to_stop"], 2) == 2.70
    assert row["guarded"] is True


def test_open_risk_marks_a_warrantless_position_unguarded(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(warrant=None, state="unguarded"))
    assert open_risk(state, {"AAPL": 48.90})[0]["guarded"] is False


def test_open_risk_survives_a_missing_price(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    row = open_risk(state, {})[0]
    assert row["unrealized_r"] is None
    assert row["price"] is None


# --- Fix round 1: `guarded` must consult the two disarm switches
# (brake.armed / brake.disarmed_positions), not just the ledger record. ---


def test_is_guarded_true_only_when_armed_warranted_arm_switch_and_not_disarmed():
    assert is_guarded(_rec(), brake_armed=True, disarmed=frozenset()) is True


def test_is_guarded_false_under_a_global_disarm():
    assert is_guarded(_rec(), brake_armed=False, disarmed=frozenset()) is False


def test_is_guarded_false_when_this_position_is_individually_disarmed():
    assert is_guarded(_rec(), brake_armed=True, disarmed={"ap_1"}) is False


def test_is_guarded_false_without_a_warrant_regardless_of_the_switches():
    rec = _rec(warrant=None, state="unguarded")
    assert is_guarded(rec, brake_armed=True, disarmed=frozenset()) is False


def test_open_risk_reports_unguarded_under_a_global_disarm(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    row = open_risk(state, {"AAPL": 48.90}, brake_armed=False)[0]
    assert row["guarded"] is False


def test_open_risk_reports_unguarded_for_a_disarmed_position(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    row = open_risk(state, {"AAPL": 48.90}, disarmed={"ap_1"})[0]
    assert row["guarded"] is False


# --- final round: `guarded` must also read the warrant's clock, and must not
# survive the deletion of its own state check. ---


def test_is_guarded_false_once_the_warrant_has_expired():
    # 24 hours with the platform dark and every renewal failing: nothing will
    # exit any of these positions, and this field is where the owner finds out.
    assert is_guarded(_rec(), brake_armed=True, disarmed=frozenset(),
                      now=EXPIRY + 1) is False


def test_is_guarded_true_while_the_warrant_is_still_live():
    assert is_guarded(_rec(), brake_armed=True, disarmed=frozenset(),
                      now=BEFORE_EXPIRY) is True


def test_is_guarded_false_when_the_expiry_cannot_be_read():
    # warrant.authorizes() treats an absent or unparseable expires_at as
    # expired, so such a warrant can never fire. Reporting it guarded would be
    # exactly the lie this check exists to end.
    rec = _rec(warrant={"grant_id": "wr_1", "expires_at": "next Tuesday"})
    assert is_guarded(rec, brake_armed=True, disarmed=frozenset(),
                      now=BEFORE_EXPIRY) is False


def test_is_guarded_without_a_clock_keeps_the_permissive_default():
    # A caller that never passes `now` must not suddenly report everything
    # unguarded; both real callers do pass one.
    rec = _rec(warrant={"grant_id": "wr_1"})
    assert is_guarded(rec, brake_armed=True, disarmed=frozenset()) is True


def test_open_risk_reports_unguarded_on_an_expired_warrant(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    assert open_risk(state, {}, now=EXPIRY + 1)[0]["guarded"] is False
    assert open_risk(state, {}, now=BEFORE_EXPIRY)[0]["guarded"] is True


@pytest.mark.parametrize("terminal_state", TERMINAL)
def test_is_guarded_false_for_a_terminal_record_that_still_holds_its_warrant(
        terminal_state):
    # `fired` is the sharpest case: mark() leaves the warrant on the record,
    # so warrant-plus-switches alone still looks guarded. Only the state check
    # says otherwise, and this pins it: deleting `state == "armed"` from
    # is_guarded must fail here.
    rec = _rec(state=terminal_state)
    assert is_guarded(rec, brake_armed=True, disarmed=frozenset(),
                      now=BEFORE_EXPIRY) is False


# --- Fix round 1: an unreadable quantity or a sign that contradicts the
# recorded direction must never read as "closed". Both are the module
# docstring's own warning (a brief outage looks like a close) arriving
# through a door reconcile() did not originally guard. ---


def test_an_unrecognized_quantity_key_does_not_release_the_position(tmp_path):
    # "amount_held" is not one of the candidate quantity keys. A skipped row
    # is not good enough here: absent-from-held plus answered-account reads
    # as released unless reconcile is told this row was unreadable, not zero.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio_row({"symbol": "AAPL", "amount_held": "100"}))
    row = load(state)["ap_1"]
    assert row["state"] == "armed"
    assert row["confirmed_qty"] == 100.0


def test_unreadable_reports_the_triple_and_held_quantities_omits_it():
    portfolio = _portfolio_row({"symbol": "AAPL", "amount_held": "100"})
    assert unreadable(portfolio) == {("demo", "463605220", "AAPL")}
    assert held_quantities(portfolio) == {}


def test_a_short_position_reported_as_negative_clamps_normally(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec(direction="short"))
    reconcile(state, _portfolio(-100))
    row = load(state)["ap_1"]
    assert row["state"] == "armed"
    assert row["confirmed_qty"] == 100.0


def test_a_direction_contradicting_the_broker_sign_is_left_untouched(tmp_path):
    # Recorded long, but the broker reports a short exposure under the same
    # symbol. Exiting on the warrant's (long) side would ADD exposure, which
    # a reduce-only brake may never do, so reconcile must not touch the
    # record's confirmed_qty or state; it only flags the anomaly.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(-100))
    row = load(state)["ap_1"]
    assert row["state"] == "armed"
    assert row["confirmed_qty"] == 100.0
    assert "anomaly" in row


def test_a_genuinely_flat_position_still_releases(tmp_path):
    # Guards against over-correcting the sign/unreadable fixes: a quantity
    # that parses to a true zero for the recorded symbol must still release.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(0))
    row = load(state)["ap_1"]
    assert row["state"] == "released"
    assert row["confirmed_qty"] == 0.0


def test_a_resolved_anomaly_is_cleared_not_left_stale(tmp_path):
    # A record that picked up "anomaly" from a contradiction must not carry it
    # forever: once a later reconcile's broker sign agrees with the recorded
    # direction again, the stale note would misreport a resolved condition as
    # still live to any consumer keying off "anomaly" in rec.
    state = EdgeState(tmp_path)
    record(state, _rec())
    reconcile(state, _portfolio(-100))
    assert "anomaly" in load(state)["ap_1"]
    reconcile(state, _portfolio(80))
    row = load(state)["ap_1"]
    assert row["confirmed_qty"] == 80.0
    assert "anomaly" not in row
