"""`nakagai-edge brake off` must work with no network. That is the point of
having a local half at all."""

import json

from nakagai_edge.cli import main
from nakagai_edge.edge.brake import armed
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import record


def test_brake_off_disarms_without_touching_the_platform(tmp_path, monkeypatch,
                                                         capsys):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    assert main(["brake", "off"]) == 0
    assert armed(EdgeState(tmp_path)) is False


def test_brake_on_re_arms(tmp_path, monkeypatch):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    main(["brake", "off"])
    assert main(["brake", "on"]) == 0
    assert armed(EdgeState(tmp_path)) is True


def test_brake_off_for_one_position_leaves_the_rest_armed(tmp_path, monkeypatch):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    assert main(["brake", "off", "--position", "ap_1"]) == 0
    assert armed(EdgeState(tmp_path)) is True


def test_brake_status_prints_json(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    assert main(["brake", "status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["armed"] is True
    assert out["positions"] == []


def test_brake_status_shows_guarded_false_after_brake_off(tmp_path, monkeypatch,
                                                           capsys):
    """Fix round 1: `guarded` must reflect the very disarm this command just
    performed, not just the ledger's warrant/state pair. Before the fix,
    `brake off` followed by `brake status` still reported the position
    guarded, contradicting `armed: false` in the same payload."""
    monkeypatch.setenv("NAKAGAI_EDGE_ROOT", str(tmp_path))
    state = EdgeState(tmp_path)
    record(state, {
        "position_id": "ap_1", "symbol": "AAPL", "connector_id": "demo",
        "account": "123", "direction": "long", "entry_price": 100.0,
        "stop": 95.0, "entry_qty": 10.0, "confirmed_qty": 10.0,
        "state": "armed", "warrant": {"trigger": {"type": "price_below",
                                                   "level": 95.0},
                                      "expires_at": 4_102_444_800.0}})

    assert main(["brake", "off"]) == 0
    capsys.readouterr()
    assert main(["brake", "status"]) == 0
    out = json.loads(capsys.readouterr().out)

    assert out["armed"] is False
    assert out["positions"][0]["guarded"] is False
