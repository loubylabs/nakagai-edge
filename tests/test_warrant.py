"""The exit warrant: a standing, reduce-only authority to close ONE position.

Unlike an entry grant it authorizes a narrow SET of orders rather than one
exact order, so the bounds are the whole safety story and every one of them
is pinned here.
"""

import pytest

pytest.importorskip("cryptography")

from nakagai_edge.signing import build_payload, generate_keypair, sign_artifact
from nakagai_edge.warrant import (
    TRIGGER_ABOVE, TRIGGER_BELOW, WARRANT_KIND, authorizes, breached,
    build_warrant_payload,
)

PRIV, PUB = generate_keypair()
NOW = 1_800_000_000.0


def _warrant(*, max_qty=100.0, level=46.20, ttl_s=86400, agent_id="ag1",
             trigger_kind=TRIGGER_BELOW, symbol="AAPL", side="sell"):
    return sign_artifact(PRIV, build_warrant_payload(
        grant_id="wr_1", agent_id=agent_id, connector_id="demo",
        account="463605220", symbol=symbol, tool="place_equity_order",
        side=side, max_qty=max_qty, trigger_kind=trigger_kind, level=level,
        ttl_s=ttl_s, now=NOW))


def _exit(qty=100.0, symbol="AAPL", side="sell", tool="place_equity_order",
          connector_id="demo", account="463605220"):
    return {"connector_id": connector_id, "tool": tool, "symbol": symbol,
            "side": side, "qty": qty, "account": account}


def _ok(warrant, exit_order, *, held_qty=100.0, spent=False, agent_id="ag1"):
    return authorizes(PUB, warrant, exit_order, agent_id=agent_id,
                      held_qty=held_qty, spent=spent, now=NOW + 60)


def test_a_well_formed_warrant_authorizes_its_exit():
    assert _ok(_warrant(), _exit()) == ""


def test_the_payload_declares_its_kind_and_is_reduce_only():
    w = _warrant()
    assert w["kind"] == WARRANT_KIND
    assert w["reduce_only"] is True
    assert w["one_shot"] is True


def test_a_long_trigger_fires_at_or_below_the_level():
    trigger = {"kind": TRIGGER_BELOW, "level": 46.20}
    assert breached(trigger, 46.19) is True
    assert breached(trigger, 46.20) is True
    assert breached(trigger, 46.21) is False


def test_a_short_trigger_fires_at_or_above_the_level():
    trigger = {"kind": TRIGGER_ABOVE, "level": 46.20}
    assert breached(trigger, 46.21) is True
    assert breached(trigger, 46.19) is False


def test_an_unreadable_trigger_never_fires():
    # Not fail-open: a level we cannot interpret must not produce an
    # arbitrary exit. The caller alerts instead.
    assert breached({"kind": "sideways", "level": 46.20}, 1.0) is False
    assert breached({}, 1.0) is False
    assert breached("garbage", 1.0) is False
    assert breached([1, 2, 3], 1.0) is False


def test_a_string_price_that_crosses_the_level_still_breaches():
    # A quote crossing from a broker or the platform can arrive as a string;
    # the comparison must not require the caller to have already parsed it.
    trigger = {"kind": TRIGGER_BELOW, "level": 46.20}
    assert breached(trigger, "46.19") is True


def test_an_unparseable_price_never_fires():
    trigger = {"kind": TRIGGER_BELOW, "level": 46.20}
    assert breached(trigger, "not-a-number") is False


def test_a_malformed_exit_order_is_refused():
    assert "exit order" in _ok(_warrant(), None)


def test_a_tampered_warrant_is_refused():
    w = _warrant()
    w["max_qty"] = 10_000.0
    assert "signature" in _ok(w, _exit())


def test_an_entry_grant_is_not_an_exit_warrant():
    # verify_artifact is generic over payloads, so without the kind check an
    # entry grant and a warrant are the same shape of signed blob.
    grant = sign_artifact(PRIV, build_payload(
        approval_id="ap_1", agent_id="ag1", connector_id="demo",
        tool="place_equity_order", args={"qty": 1},
        account_arg_names=["account_number"], ttl_s=900, now=NOW))
    assert "not an exit warrant" in _ok(grant, _exit())


def test_another_agents_warrant_is_refused():
    assert "agent_id" in _ok(_warrant(agent_id="ag2"), _exit())


def test_an_expired_warrant_is_refused():
    assert "expired" in authorizes(
        PUB, _warrant(ttl_s=60), _exit(), agent_id="ag1", held_qty=100.0,
        spent=False, now=NOW + 120)


def test_a_spent_warrant_is_refused():
    assert "spent" in _ok(_warrant(), _exit(), spent=True)


def test_the_exit_may_not_exceed_the_warrant_ceiling():
    assert "ceiling" in _ok(_warrant(max_qty=100.0), _exit(qty=101.0),
                            held_qty=500.0)


def test_a_nan_ceiling_is_refused_outright():
    # NaN parses cleanly through float() and every comparison against it is
    # False, so `qty > ceiling` would never trip and silently wave the exit
    # through instead of bounding it. This must be caught before that
    # comparison ever runs.
    assert "not a usable number" in _ok(_warrant(max_qty=float("nan")),
                                        _exit(qty=1.0), held_qty=500.0)


def test_an_infinite_ceiling_is_refused_outright():
    # +inf compares as larger than any real qty, so it would authorize any
    # size at all rather than bound it.
    assert "not a usable number" in _ok(_warrant(max_qty=float("inf")),
                                        _exit(qty=1.0), held_qty=500.0)


def test_the_exit_may_not_exceed_what_is_actually_held():
    assert "held" in _ok(_warrant(max_qty=100.0), _exit(qty=100.0),
                         held_qty=40.0)


def test_a_zero_or_negative_exit_is_refused():
    assert "positive" in _ok(_warrant(), _exit(qty=0.0))
    assert "positive" in _ok(_warrant(), _exit(qty=-5.0))


@pytest.mark.parametrize("field,value,expected", [
    ("symbol", "MSFT", "symbol"),
    ("side", "buy", "side"),
    ("tool", "place_option_order", "tool"),
    ("connector_id", "other", "connector"),
    ("account", "999", "account"),
])
def test_the_exit_must_match_the_warrant(field, value, expected):
    order = _exit()
    order[field] = value
    assert expected in _ok(_warrant(), order)
