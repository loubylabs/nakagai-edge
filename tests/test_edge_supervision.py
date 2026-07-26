"""The ledger of supervised positions. Broker truth always wins, and every
drift case must resolve toward a SMALLER exit than before."""

from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    held_quantities, load, mark, open_risk, recover_interrupted, reconcile,
    record, unreadable,
)


def _rec(**over):
    rec = {"position_id": "ap_1", "connector_id": "demo",
           "account": "463605220", "symbol": "AAPL", "direction": "long",
           "signal_id": "sig_1", "entry_args": {"symbol": "AAPL"},
           "entry_qty": 100.0, "entry_price": 47.55, "stop": 46.20,
           "confirmed_qty": 100.0, "last_confirmed_at": 0.0,
           "unguarded_qty": 0.0, "warrant": {"grant_id": "wr_1"},
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


def test_marking_changes_state_and_carries_extras(tmp_path):
    state = EdgeState(tmp_path)
    record(state, _rec())
    mark(state, "ap_1", "fired", fired_at=99.0)
    row = load(state)["ap_1"]
    assert row["state"] == "fired"
    assert row["fired_at"] == 99.0


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
