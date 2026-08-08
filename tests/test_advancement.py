"""The fixed advancement offset.

These are invariant tests, not numerical ones. The rate itself is measured
elsewhere and will move when the data does; what must not move is the shape of
the correction -- centred within climber, zero where there is no time contrast,
signed so that an older send is treated as coming from a weaker climber.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from kaya.advancement import (
    ADV_INTERCEPT,
    ADV_SLOPE,
    advancement_offset,
    advancement_rate,
    describe,
    has_dates,
)


def frame(rows):
    """rows: (user_id, gym_id, grade, iso date)."""
    return pd.DataFrame(rows, columns=['user_id', 'gym_id', 'm', 'max_send_date'])


def test_the_rate_falls_with_ability_and_never_goes_negative():
    assert advancement_rate(1) > advancement_rate(5) > advancement_rate(9)
    # The fitted line crosses zero just past V9. Beyond it the measured bins
    # say "no further improvement", not "decline", so the curve floors at 0.
    assert advancement_rate(20) == 0.0
    assert advancement_rate(1) == pytest.approx(ADV_INTERCEPT + ADV_SLOPE)


def test_the_rate_is_clipped_to_the_range_it_was_measured_over():
    # Below V1 the straight line keeps rising with no data behind it.
    assert advancement_rate(-5) == advancement_rate(1)


def test_a_climber_at_one_gym_gets_no_correction():
    # No within-climber time contrast means nothing to correct, whatever the
    # date is. This is most of the dataset.
    off = advancement_offset(frame([('u1', 'A', 5.0, '2021-01-01')]))
    assert off.tolist() == [0.0]


def test_the_offset_is_centred_within_the_climber():
    off = advancement_offset(frame([
        ('u1', 'A', 5.0, '2021-01-01'),
        ('u1', 'B', 5.0, '2025-01-01'),
    ]))
    assert off.sum() == pytest.approx(0.0)
    # Older send -> negative shift: at that time the climber was weaker, so the
    # ceiling the likelihood compares against must be lowered, not the gym's.
    assert off[0] < 0 < off[1]


def test_the_gap_between_two_sends_equals_rate_times_elapsed_years():
    off = advancement_offset(frame([
        ('u1', 'A', 4.0, '2020-01-01'),
        ('u1', 'B', 4.0, '2024-01-01'),
    ]))
    # v is the climber's own hardest grade anywhere, so V4 here.
    expected = advancement_rate(4.0) * (pd.Timestamp('2024-01-01')
                                        - pd.Timestamp('2020-01-01')).days / 365.25
    assert (off[1] - off[0]) == pytest.approx(expected, rel=1e-9)


def test_a_stronger_climber_is_corrected_less_over_the_same_span():
    dates = [('u1', 'A', 2.0, '2020-01-01'), ('u1', 'B', 2.0, '2024-01-01')]
    strong = [('u2', 'A', 9.0, '2020-01-01'), ('u2', 'B', 9.0, '2024-01-01')]
    weak_off = advancement_offset(frame(dates))
    strong_off = advancement_offset(frame(strong))
    assert abs(weak_off[1] - weak_off[0]) > abs(strong_off[1] - strong_off[0])


def test_climbers_do_not_leak_into_each_other():
    """The reference date is per climber, so adding a climber changes nothing.

    A global mean date would fail this, and would smuggle a between-climber
    signal into a term the ability parameter already owns.
    """
    a = frame([('u1', 'A', 5.0, '2021-01-01'), ('u1', 'B', 5.0, '2023-01-01')])
    b = pd.concat([a, frame([('u2', 'A', 7.0, '2019-01-01'),
                             ('u2', 'B', 7.0, '2026-01-01')])],
                  ignore_index=True)
    assert advancement_offset(b)[:2] == pytest.approx(advancement_offset(a))


def test_a_missing_date_column_is_zero_rather_than_an_error():
    df = frame([('u1', 'A', 5.0, '2021-01-01')]).drop(columns=['max_send_date'])
    assert not has_dates(df)
    assert advancement_offset(df).tolist() == [0.0]


def test_an_undated_climber_gets_zero_rather_than_nan():
    """One NaN in the offset would silently make the whole log-likelihood NaN."""
    off = advancement_offset(frame([
        ('u1', 'A', 5.0, '2021-01-01'), ('u1', 'B', 5.0, '2023-01-01'),
        ('u2', 'A', 6.0, None), ('u2', 'B', 6.0, None),
    ]))
    assert np.isfinite(off).all()
    assert off[2:].tolist() == [0.0, 0.0]


def test_describe_reports_whether_the_correction_actually_applied():
    single = describe(frame([('u1', 'A', 5.0, '2021-01-01')]))
    assert single['applied'] is False and single['n_corrected'] == 0

    paired = describe(frame([('u1', 'A', 5.0, '2021-01-01'),
                             ('u1', 'B', 5.0, '2025-01-01')]))
    assert paired['applied'] is True and paired['n_corrected'] == 2
    assert paired['intercept'] == ADV_INTERCEPT
    assert paired['slope'] == ADV_SLOPE
