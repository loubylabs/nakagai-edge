"""`nakagai-edge brake off` must work with no network. That is the point of
having a local half at all."""

import json

from nakagai_edge.cli import main
from nakagai_edge.edge.brake import armed
from nakagai_edge.edge.state import EdgeState


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
