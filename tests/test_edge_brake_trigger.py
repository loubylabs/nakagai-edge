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


def test_a_mapped_quote_row_is_stamped_and_carries_its_book():
    # The row arrives through the connector's map, so the fields are already
    # canonical and already floats. All this adds is the receipt time.
    assert normalize_quote(
        {"symbol": "AAPL", "price": 46.10, "bid": 46.09, "ask": 46.11},
        NOW) == {"price": 46.10, "bid": 46.09, "ask": 46.11, "ts": NOW}


def test_a_row_still_wearing_the_brokers_own_key_names_normalizes_to_nothing():
    # Locating the price is the map's job now, and it is the only place an
    # owner can fix it. A second guess at the spelling here would be a second
    # answer to the same question, and the two would drift.
    assert normalize_quote({"last_trade_price": 46.10}, NOW) is None


def test_an_unreadable_quote_payload_normalizes_to_nothing():
    assert normalize_quote({"nope": 1}, NOW) is None
    assert normalize_quote(None, NOW) is None
    # coerce() drops anything it cannot turn into a finite float, so a string
    # here means the map never read it. Parsing it anyway would resurrect the
    # very value the capability layer refused.
    assert normalize_quote({"price": "46.10"}, NOW) is None


def test_a_quote_straight_out_of_normalize_quote_with_stale_received_at_is_refused():
    normalized = normalize_quote(
        {"price": 46.10, "bid": 46.09, "ask": 46.11}, NOW - 120.0)
    assert "stale" in usable(normalized, 46.50, NOW)


def test_a_quote_with_missing_ts_is_refused_as_unstamped():
    quote = {"price": 46.10, "bid": 46.09, "ask": 46.11}
    assert "unstamped" in usable(quote, 46.50, NOW)


def test_bid_zero_and_ask_ten_is_refused_for_spread():
    assert "spread" in usable(_q(bid=0.0, ask=10.0), 46.50, NOW)


def test_a_zero_prior_is_treated_as_absent_and_never_divides_by_zero():
    # This test pins documented semantics: prior_price=0.0 is treated as no
    # prior (same as None), and the jump check does not run. It also guards
    # against a future edit that might drop the > 0 clause, which would cause
    # abs(price - 0) / 0 * 100 to raise ZeroDivisionError inside the one
    # function whose entire job is to never let a bad input through. The test
    # cannot discriminate this round's change from pre-fix code (both skip
    # 0.0), but it guards the regression where the guard is removed.
    result = usable(_q(), 0.0, NOW)
    assert result == ""
    # Confirm no exception is raised.
    assert isinstance(result, str)
