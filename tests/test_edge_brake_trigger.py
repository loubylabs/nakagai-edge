"""When the brake is allowed to believe a price.

Firing on a bad print sells a good position at a fictional number, so an
observation must be fresh, internally sane, and then confirmed by a second one
before anything reaches the broker.
"""

from nakagai_edge.edge.brake import (
    BREACH_CONFIRMATIONS, advance, confirmed, normalize_quote, usable,
)

NOW = 1_800_000_000.0


def _q(price=46.10, bid=46.09, ask=46.11, age=0.0):
    return {"price": price, "bid": bid, "ask": ask, "ts": NOW - age}


def test_a_fresh_sane_quote_is_usable():
    assert usable(_q(), 46.50, NOW) == ""


def test_a_stale_quote_is_refused():
    assert "stale" in usable(_q(age=120.0), 46.50, NOW)


def test_a_blown_out_spread_is_refused():
    assert "spread" in usable(_q(bid=40.0, ask=52.0), 46.50, NOW)


def test_an_impossible_jump_from_the_prior_price_is_refused():
    assert "jump" in usable(_q(price=20.0), 46.50, NOW)


def test_the_first_observation_has_no_prior_to_compare_against():
    assert usable(_q(), None, NOW) == ""


def test_a_nonpositive_price_is_refused():
    assert "price" in usable(_q(price=0.0), 46.50, NOW)


def test_a_quote_missing_both_sides_is_still_usable_on_its_last_price():
    # Not every broker returns a book. A last price alone is enough to run a
    # stop against; only the spread check is skipped.
    assert usable(_q(bid=None, ask=None), 46.50, NOW) == ""


def test_a_breach_run_advances_and_resets():
    assert advance(0, True) == 1
    assert advance(1, True) == 2
    assert advance(2, False) == 0


def test_confirmation_needs_two_consecutive_breaches_by_default():
    assert BREACH_CONFIRMATIONS == 2
    assert confirmed(1) is False
    assert confirmed(2) is True


def test_a_quote_payload_is_normalized_from_common_broker_shapes():
    assert normalize_quote(
        {"last_trade_price": "46.10", "bid_price": "46.09",
         "ask_price": "46.11"})["price"] == 46.10


def test_an_unreadable_quote_payload_normalizes_to_nothing():
    assert normalize_quote({"nope": 1}) is None
    assert normalize_quote(None) is None
