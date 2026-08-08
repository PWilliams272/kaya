"""Gym grading drift: build the time-resolved cells, and measure the drift from them.

WHY THIS EXISTS. `docs/two-stage-and-grade-compression.md` §6.5 reports gym drift
as measured — tau 0.163 grades/yr, 0.62 grades accumulated over the span, per-gym
rates named — and **nothing in the repo computes it**. The numbers live only in
prose. That is the same provenance gap `base_bouldering.pkl` had until
`build_base_snapshot.py` was written: a result the project rests on that cannot
be regenerated, checked, or updated when new gyms land.

So this script does two jobs:

  1. Builds the TIME-RESOLVED CELLS — (climber, gym, 90-day window) — which is
     the dataset a drift-carrying model needs and which does not otherwise
     exist. `base_bouldering.pkl` aggregates to one row per (climber, gym) and
     throws the within-pair time structure away.
  2. Re-derives §6.5 from those cells, so the doc's numbers become reproducible
     and any disagreement shows up here rather than in a model built on top.

THE ESTIMATOR, and why it is clean. Take one climber, in one 90-day window, who
climbed at two gyms:

    level_A - level_B  =  correction_A - correction_B + noise

Same person, same moment, so this cancels their ability, their improvement
(advancement hits both sides equally), and any tendency to visit a new gym while
peaking. Ask whether that difference trends with CALENDAR DATE: if it does, one
gym drifted relative to the other.

WHAT IT CANNOT SEE: drift common to every gym, which cancels in the difference
exactly as global grade compression does. This is RELATIVE drift only — there is
no external anchor. Do not read a per-gym rate as an absolute claim about that
gym's standards.

log(n_sends) is a control covariate. A window's max grade rises with how many
sends went into it, so a pair whose send counts move over time would otherwise
fake a grading drift.

Two traps this script checks rather than assumes:

  * CONNECTIVITY. Raising the per-pair observation threshold splits the gym
    graph into components, and relative rates BETWEEN components are genuinely
    unidentifiable. §6.5 records a first attempt that returned standard errors
    of ~19,000 at a 40-observation threshold because of exactly this. The graph
    is checked before the solve, and the solve refuses to run on a split graph.
  * TWO INDEPENDENT ROUTES. The per-gym solve is validated against the paired
    data it came from: per-gym sd should land near tau/sqrt(2). They are
    different computations and agreement is the check.

    python scripts/probe_gym_drift.py                 # measure, write nothing
    python scripts/probe_gym_drift.py --write-cells   # also write the dataset
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
CELLS_OUT = RUNS / 'time_resolved_cells.pkl'
REPORT_OUT = RUNS / 'results' / 'gym_drift.json'

WINDOW_DAYS = 90
MIN_SENDS_PER_CELL = 3
MIN_PAIR_OBS = 25
DAYS_PER_YEAR = 365.25


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--network', default='net50')
    p.add_argument('--source', default='local_db',
                   choices=['local_db', 's3_raw', 'aws_db'])
    p.add_argument('--window-days', type=int, default=WINDOW_DAYS)
    p.add_argument('--min-sends', type=int, default=MIN_SENDS_PER_CELL,
                   help='sends needed before a (climber, gym, window) cell counts')
    p.add_argument('--min-pair-obs', type=int, default=MIN_PAIR_OBS,
                   help='paired observations needed before a gym pair is fitted')
    p.add_argument('--write-cells', action='store_true',
                   help=f'write the cells to {CELLS_OUT.name}')
    p.add_argument('--write-report', action='store_true',
                   help=f'write the measurement to {REPORT_OUT.name}')
    return p.parse_args(argv)


# --- 1. the cells -------------------------------------------------------

def build_cells(network: str, source: str, window_days: int, min_sends: int):
    """(climber, gym, window) -> max grade, send count, mid-date.

    The max is the right summary because it is what the model's ceiling
    observation already is: `m` in the main dataset is a climber's hardest send
    at a gym. Slicing by window makes it their hardest send at that gym in that
    window, which is the same quantity at finer time resolution.
    """
    sys.path.insert(0, str(ROOT / 'src'))
    from kaya.data_access import KayaDataAccessor
    from kaya.grading_model_v2 import BOULDER_GRADE_TO_NUM

    gyms = set(json.loads((RUNS / 'networks.json').read_text())['networks'][network])

    acc = KayaDataAccessor()
    sends = acc.read_sends(source=source,
                           columns=['user_id', 'gym_id', 'date', 'grade',
                                    'climb_type'],
                           parse_dates=False, order_by=False)
    ct = sends['climb_type'].fillna('').astype(str).str.lower()
    sends = sends[ct.str.contains('boulder')].copy()
    sends['m'] = sends['grade'].map(BOULDER_GRADE_TO_NUM)
    sends['gym_id'] = sends['gym_id'].astype(str)
    sends = sends[sends.gym_id.isin(gyms)]
    sends['date'] = pd.to_datetime(sends['date'], errors='coerce')
    sends = sends.dropna(subset=['m', 'date', 'user_id'])

    t0 = sends['date'].min()
    span_days = (sends['date'].max() - t0).days
    sends['window'] = ((sends['date'] - t0).dt.days // window_days).astype(int)

    g = sends.groupby(['user_id', 'gym_id', 'window'], sort=False)
    cells = g.agg(m_max=('m', 'max'), n_sends=('m', 'size'),
                  date_mid=('date', 'mean')).reset_index()
    cells = cells[cells.n_sends >= min_sends].copy()
    # Years from the middle of the span: centring keeps the intercept and the
    # slope from being correlated, which matters when a pair covers a short
    # window late in the record.
    mid = t0 + pd.Timedelta(days=span_days / 2)
    cells['t_years'] = (cells['date_mid'] - mid).dt.days / DAYS_PER_YEAR
    cells['log_n'] = np.log(cells['n_sends'])

    meta = {
        'network': network, 'source': source, 'window_days': window_days,
        'min_sends_per_cell': min_sends,
        'n_sends': int(len(sends)), 'n_cells': int(len(cells)),
        'n_climbers': int(sends.user_id.nunique()),
        'n_gyms': int(sends.gym_id.nunique()),
        'date_min': str(sends['date'].min().date()),
        'date_max': str(sends['date'].max().date()),
        'span_years': round(span_days / DAYS_PER_YEAR, 2),
    }
    return cells, meta


# --- 2. paired same-window differences ----------------------------------

def pair_slopes(cells: pd.DataFrame, min_pair_obs: int):
    """Per gym pair: the drift of (level_A - level_B) against calendar date.

    One row per (climber, window, gym pair). Regressed on [1, t, dlog_n], so
    the reported slope is grades per year of RELATIVE drift with the send-count
    confound removed.
    """
    multi = cells.groupby(['user_id', 'window'])['gym_id'].transform('nunique')
    cells = cells[multi >= 2]

    rows = []
    for (_uid, _w), grp in cells.groupby(['user_id', 'window'], sort=False):
        recs = grp[['gym_id', 'm_max', 't_years', 'log_n']].values
        for i in range(len(recs)):
            for j in range(i + 1, len(recs)):
                a, b = recs[i], recs[j]
                if a[0] == b[0]:
                    continue
                # Order the pair so every observation of one pair of gyms has
                # the same sign convention.
                if str(a[0]) > str(b[0]):
                    a, b = b, a
                rows.append((str(a[0]), str(b[0]), a[1] - b[1],
                             (a[2] + b[2]) / 2, a[3] - b[3]))
    paired = pd.DataFrame(rows, columns=['gym_a', 'gym_b', 'dy', 't', 'dlog_n'])

    fits = []
    for (ga, gb), grp in paired.groupby(['gym_a', 'gym_b']):
        n = len(grp)
        if n < min_pair_obs:
            continue
        X = np.column_stack([np.ones(n), grp['t'].values, grp['dlog_n'].values])
        y = grp['dy'].values
        # Guard the degenerate pair: all observations in one window carry no
        # calendar information at all, and lstsq would return a slope anyway.
        if np.ptp(grp['t'].values) < 0.25:
            continue
        beta, *_ = np.linalg.lstsq(X, y, rcond=None)
        resid = y - X @ beta
        dof = n - X.shape[1]
        if dof <= 0:
            continue
        s2 = float(resid @ resid) / dof
        cov = s2 * np.linalg.pinv(X.T @ X)
        fits.append({'gym_a': ga, 'gym_b': gb, 'n': n,
                     'slope': float(beta[1]), 'se': float(np.sqrt(cov[1, 1])),
                     'span_years': float(np.ptp(grp['t'].values))})
    return paired, pd.DataFrame(fits)


# --- 3. meta-analysis ---------------------------------------------------

def heterogeneity(fits: pd.DataFrame):
    """DerSimonian-Laird: is there more spread between pairs than error explains?

    Q is the weighted spread of the pair slopes about their weighted mean. Under
    "every pair drifts identically" it is chi-square on k-1 degrees of freedom,
    so Q much larger than df means real between-pair variation. I-squared is the
    share of total variation that is real rather than sampling error, and tau is
    that variation as a standard deviation in grades/yr.
    """
    b, se = fits['slope'].values, fits['se'].values
    w = 1.0 / se ** 2
    mean = float((w * b).sum() / w.sum())
    Q = float((w * (b - mean) ** 2).sum())
    k = len(b)
    df = k - 1
    c = w.sum() - (w ** 2).sum() / w.sum()
    tau2 = max(0.0, (Q - df) / c) if c > 0 else 0.0
    from scipy.stats import chi2
    return {
        'k_pairs': k, 'Q': Q, 'df': df,
        'chi2_crit_95': float(chi2.ppf(0.95, df)) if df > 0 else float('nan'),
        'I2': float(max(0.0, (Q - df) / Q)) if Q > 0 else 0.0,
        'tau': float(np.sqrt(tau2)),
        'pooled_mean_slope': mean,
        'median_pair_se': float(np.median(se)),
        'median_pair_n': float(np.median(fits['n'].values)),
        'median_pair_span_years': float(np.median(fits['span_years'].values)),
    }


# --- 4. contrasts -> per-gym rates --------------------------------------

def components(fits: pd.DataFrame, gyms):
    """Connected components of the gym graph. Rates across components are not
    comparable, because no observation ever ties them together."""
    adj = {g: set() for g in gyms}
    for _, r in fits.iterrows():
        adj[r.gym_a].add(r.gym_b)
        adj[r.gym_b].add(r.gym_a)
    seen, out = set(), []
    for g in gyms:
        if g in seen:
            continue
        stack, comp = [g], []
        seen.add(g)
        while stack:
            x = stack.pop()
            comp.append(x)
            for y in adj[x] - seen:
                seen.add(y)
                stack.append(y)
        out.append(sorted(comp))
    return sorted(out, key=len, reverse=True)


def solve_rates(fits: pd.DataFrame, gyms, tau: float, *, pool: bool = True):
    """Contrasts -> per-gym rates: weighted least squares, zero-sum, POOLED.

    Each pair says rate_a - rate_b = slope. Stacking those is a contrast system
    with one null direction -- adding a constant to every gym changes no
    difference -- which the zero-sum row removes.

    THE POOLING IS NOT OPTIONAL, and getting it wrong is the trap this whole
    function is about. 123 noisy contrasts constraining 29 unknowns will happily
    absorb their own sampling error into the fitted rates: run this unpooled and
    the spread across gyms comes out at 0.281 grades/yr against a tau/sqrt(2) of
    0.115, i.e. 2.4x too large, with a range of -0.47 to +0.68 that is mostly
    noise wearing a gym's name. That is the same failure as the ~19,000 standard
    errors recorded in docs §6.5, one notch milder -- there the graph was
    disconnected, here it is connected but unconstrained.

    So each rate carries its own prior, rate_g ~ N(0, tau_gym^2), added as n
    extra rows pulling toward zero with weight 1/tau_gym^2. tau_gym = tau/sqrt(2)
    because tau measures the spread of DIFFERENCES between two gyms, and the
    variance of a difference of two independent rates is twice the variance of
    one. This is ridge regression with the penalty fixed by a measurement rather
    than chosen, and it is what "partial pooling at the measured tau" means.

    `pool=False` exists so the difference can be shown rather than asserted.
    """
    idx = {g: i for i, g in enumerate(gyms)}
    n = len(gyms)
    tau_gym = tau / np.sqrt(2.0)
    rows, y, w = [], [], []
    for _, r in fits.iterrows():
        row = np.zeros(n)
        row[idx[r.gym_a]] = 1.0
        row[idx[r.gym_b]] = -1.0
        rows.append(row)
        y.append(r.slope)
        # The pair's own error only. Adding tau here as well would be counting
        # the between-gym spread twice: once as noise, once as the prior below.
        w.append(1.0 / r.se ** 2)
    if pool and tau_gym > 0:
        for g in gyms:
            row = np.zeros(n)
            row[idx[g]] = 1.0
            rows.append(row)
            y.append(0.0)
            w.append(1.0 / tau_gym ** 2)
    rows.append(np.ones(n))          # zero-sum: fixes the unidentified level
    y.append(0.0)
    w.append(1e6)

    X = np.array(rows)
    y = np.array(y)
    W = np.diag(w)
    XtWX = X.T @ W @ X
    beta = np.linalg.solve(XtWX, X.T @ W @ y)
    cov = np.linalg.inv(XtWX)
    return pd.DataFrame({'gym_id': gyms, 'rate': beta,
                         'se': np.sqrt(np.diag(cov))})


def main(argv=None) -> int:
    args = parse_args(argv)

    print(f'building (climber, gym, {args.window_days}-day window) cells '
          f'from source={args.source!r}')
    cells, meta = build_cells(args.network, args.source,
                              args.window_days, args.min_sends)
    print(f"  {meta['n_sends']:,} dated boulder sends at {meta['n_gyms']} gyms, "
          f"{meta['n_climbers']:,} climbers")
    print(f"  {meta['date_min']} -> {meta['date_max']}  "
          f"({meta['span_years']} years)")
    print(f"  {meta['n_cells']:,} cells with >= {args.min_sends} sends")

    paired, fits = pair_slopes(cells, args.min_pair_obs)
    multi_cells = cells.groupby(['user_id', 'window'])['gym_id'].nunique()
    n_multi = int((multi_cells >= 2).sum())
    print(f'\n  {n_multi:,} (climber, window) cells span two or more gyms')
    print(f'  {len(paired):,} paired same-window observations')
    print(f'  {len(fits)} gym pairs with >= {args.min_pair_obs} of them')
    if len(fits) < 2:
        print('  not enough pairs to say anything')
        return 1

    het = heterogeneity(fits)
    print('\nIs the drift real? (Q above its critical value = yes)\n')
    print(f"  Q / df                    {het['Q']:.1f} / {het['df']} "
          f"(critical {het['chi2_crit_95']:.1f})")
    print(f"  I^2                       {100 * het['I2']:.0f}%")
    print(f"  tau (relative drift sd)   {het['tau']:.3f} grades/yr")
    print(f"  median pair SE            {het['median_pair_se']:.3f} grades/yr")
    print(f"  median pair observations  {het['median_pair_n']:.0f}")
    print(f"  median pair span          {het['median_pair_span_years']:.1f} years")

    gyms = sorted(set(fits.gym_a) | set(fits.gym_b))
    comps = components(fits, gyms)
    print(f'\n  gym graph: {len(comps)} component(s), sizes '
          f'{[len(c) for c in comps]}')
    if len(comps) > 1:
        print('  REFUSING to solve per-gym rates: relative drift between '
              'disconnected\n  components is unidentifiable. Lower '
              '--min-pair-obs until the graph connects.')
        rates = None
    else:
        rates = solve_rates(fits, gyms, het['tau'])
        unpooled = solve_rates(fits, gyms, het['tau'], pool=False)
        sd = float(rates['rate'].std(ddof=0))
        sd_unpooled = float(unpooled['rate'].std(ddof=0))
        print(f'\nPer-gym rates ({len(gyms)} gyms, zero-sum, pooled at tau)\n')
        print(f'  sd across gyms            {sd:.3f} grades/yr')
        print(f'  tau / sqrt(2)             {het["tau"] / np.sqrt(2):.3f} '
              f'grades/yr   <- independent route, should agree')
        print(f'  same solve, UNPOOLED      {sd_unpooled:.3f} grades/yr   '
              f'<- {sd_unpooled / (het["tau"] / np.sqrt(2)):.1f}x too large; '
              f'this is what overfitting\n' + ' ' * 60
              + '   123 contrasts with 29 unknowns looks like')
        print(f'  range                     {rates.rate.min():+.3f} to '
              f'{rates.rate.max():+.3f}')
        print(f'  median per-gym SE         {rates.se.median():.3f}')
        print(f"  accumulated over {meta['span_years']}y      "
              f"{sd * meta['span_years']:.2f} grades (sd across gyms)")
        top = rates.reindex(rates.rate.abs().sort_values(ascending=False).index)
        print('\n  fastest movers (+ = stiffening)')
        for _, r in top.head(6).iterrows():
            print(f'    gym {r.gym_id:>5}  {r.rate:+.3f} +/- {r.se:.3f}')

    if args.write_cells:
        cells.to_pickle(CELLS_OUT)
        print(f'\nwrote {CELLS_OUT.relative_to(ROOT)} '
              f'({len(cells):,} rows) — the dataset a drift-carrying model reads')
    if args.write_report:
        REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)
        REPORT_OUT.write_text(json.dumps({
            'meta': meta, 'heterogeneity': het,
            'min_pair_obs': args.min_pair_obs,
            'n_multi_gym_cells': n_multi,
            'n_paired_obs': int(len(paired)),
            'components': [len(c) for c in comps],
            'rates': None if rates is None else rates.to_dict('records'),
        }, indent=2, default=str))
        print(f'wrote {REPORT_OUT.relative_to(ROOT)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
