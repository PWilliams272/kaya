"""Climber advancement as a FIXED, per-observation offset on the ceiling.

The problem it solves
---------------------
The model compares a climber's hardest send at gym A against their hardest at
gym B and attributes the difference to the gyms. That is only fair if the two
sends happened at the same time. They routinely do not: Cliffs of Id opened
years before Class 5, so a climber's Cliffs record is largely old sends and
their Class 5 record is entirely recent ones. Climbers improve, so the newer
gym looks softer than it is -- the gym correction absorbs the climber's own
progress. Measured on the modelled data, that mis-timing is worth a median
**0.059 grades** per gym pair and up to **0.292** on the worst pair, against a
gym-correction spread of 0.284 grades (standard deviation). The worst case is
as large as the entire signal.

Why FIXED and not fitted
------------------------
A free advancement parameter has nothing to separate it from the gym
corrections: both explain the same date/grade covariance, and the gyms have
4,241 climbers' worth of leverage while advancement has one number. Fitted, it
absorbed **3.4x its true value** on simulated data and dragged every gym
correction with it. So the rate is measured once, outside the model, from a
source the gym corrections cannot contaminate -- climbers' progress *within a
single gym*, where no cross-gym comparison is involved -- and then held fixed.

Where the rate comes from
-------------------------
`scripts/build_v2_time.py` (`steady_rate`) fits gain = r * horizon through the
origin across all six elapsed horizons at once, per starting grade bin, and
then fits a straight line through those per-bin rates:

    r(v) = ADV_INTERCEPT + ADV_SLOPE * v      grades per year

giving +0.30/yr for a V1 climber falling to +0.02/yr at V9 -- improvement that
flattens out as people approach their ceiling, which is what the per-bin table
shows directly. Re-running that script rewrites `advancement.fit` in
`viewer_static/v2_time.json`; if it moves, update the constants here to match
and say so in the commit. They are duplicated rather than imported because the
model must not depend on a viewer payload.

The offset
----------
For observation (climber u, gym g), with t the date of the hardest send there:

    adv[u, g] = r(v_u) * (t[u, g] - mean_g' t[u, g'])        in grades

Centering **per climber** is the point: only *within*-climber date differences
are used, which is exactly the contrast the paired-difference identity rests
on. Centering on a global date instead would inject a between-climber signal
that the ability term already owns. A climber seen at one gym gets exactly
zero, correctly -- there is no within-climber time contrast to correct.

`v_u` is the climber's own hardest grade anywhere, clipped to the range the
rate curve was measured over. The rate itself is clipped at zero: the fitted
line goes slightly negative past V9, but the measured bins there are
0.037 +/- 0.031 and 0.020 +/- 0.052 -- consistent with no further improvement,
not with decline.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# From `advancement.fit` in v2_time.json -- see the module docstring for why
# these are copied rather than imported.
ADV_INTERCEPT = 0.338
ADV_SLOPE = -0.035

# The grade range the rate curve was actually measured over. Outside it the
# straight line is extrapolation, so the input is clipped rather than trusted.
ADV_V_MIN = 1.0
ADV_V_MAX = 11.0

# The column carrying the date of the hardest send at each gym, which is the
# observation the likelihood actually models.
DATE_COL = 'max_send_date'

DAYS_PER_YEAR = 365.25


def advancement_rate(v: np.ndarray | float) -> np.ndarray:
    """Grades per year gained by a climber currently at V-grade `v`.

    Clipped at zero on the top end: the fitted line crosses zero just past V9,
    and the measured bins beyond it are consistent with no further improvement
    rather than with getting worse.
    """
    vv = np.clip(np.asarray(v, float), ADV_V_MIN, ADV_V_MAX)
    return np.maximum(ADV_INTERCEPT + ADV_SLOPE * vv, 0.0)


def advancement_offset(obs: pd.DataFrame, *, date_col: str = DATE_COL,
                       user_col: str = 'user_id',
                       grade_col: str = 'm') -> np.ndarray:
    """Per-observation ceiling offset, in grades, aligned to `obs`'s row order.

    Returns all zeros -- with no error -- when the dates are absent or entirely
    missing, so an old snapshot still fits. Callers that need the correction to
    have actually been applied should check `has_dates` first rather than
    inferring it from a zero vector, since a legitimately single-gym-per-climber
    dataset also produces zeros.
    """
    n = len(obs)
    if not has_dates(obs, date_col=date_col):
        return np.zeros(n)

    t = pd.to_datetime(obs[date_col], errors='coerce', utc=True)
    # NaT casts to the int64 sentinel rather than propagating, so the missing
    # rows are restored explicitly afterwards.
    days = t.astype('int64').to_numpy(float) / 86_400e9
    days[t.isna().to_numpy()] = np.nan
    years = days / DAYS_PER_YEAR

    u = obs[user_col].to_numpy()
    frame = pd.DataFrame({'u': u, 'y': years, 'm': obs[grade_col].to_numpy(float)})
    # Climber-level quantities: the reference date each observation is measured
    # against, and the level whose rate applies. Both use only that climber's
    # own rows, so nothing leaks across climbers.
    ref = frame.groupby('u')['y'].transform('mean')
    level = frame.groupby('u')['m'].transform('max')

    dt = (frame['y'] - ref).to_numpy()
    off = advancement_rate(level.to_numpy()) * dt
    # A climber with no dated rows at all gets no correction rather than a NaN
    # that would silently poison the whole likelihood.
    return np.nan_to_num(off, nan=0.0)


def has_dates(obs: pd.DataFrame, *, date_col: str = DATE_COL) -> bool:
    """Can this dataset support the advancement correction at all?"""
    if date_col not in obs.columns:
        return False
    return bool(pd.to_datetime(obs[date_col], errors='coerce').notna().any())


def describe(obs: pd.DataFrame, **kw) -> dict:
    """Summary of the offset actually applied, for run records and logs."""
    off = advancement_offset(obs, **kw)
    nz = off[off != 0.0]
    return {'applied': bool(len(nz)),
            'n_obs': int(len(off)),
            'n_corrected': int(len(nz)),
            'mean_abs': float(np.abs(off).mean()) if len(off) else 0.0,
            'max_abs': float(np.abs(off).max()) if len(off) else 0.0,
            'intercept': ADV_INTERCEPT, 'slope': ADV_SLOPE}
