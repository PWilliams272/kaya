"""Time analyses for the Grading Model v2 write-up, and their data file.

Two questions, one pass over the sends table (which is the expensive part):

1. **How fast do climbers advance, and does it depend on where they are?**
   Both interesting on its own and the input any time-drift term in the
   grading model needs -- a constant drift is the wrong shape if people move
   through V2->V4 far faster than V7->V9.

2. **Are the gym corrections confounded with *when* their climbers logged?**
   The grading model has no time in it at all, so a climber who logged at gym
   A in 2022 and gym B in 2025 has three years of improvement charged to the
   gyms rather than to themselves.

Writes src/kaya/viewer_static/v2_time.json and prints both tables.

Run from the repo root -- running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd

from kaya.data_access import KayaDataAccessor
from kaya.grading_model_v2 import BOULDER_GRADE_TO_NUM

ROOT = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
# The job scratch directory is deleted when the job is; runs/ is the durable
# archive. Each input resolves to runs/ when it is there and falls back to
# scratch otherwise, so a batch still mid-copy keeps working.
_SCRATCH = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')


def _pick(*candidates):
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def data_file(fname):
    """base_bouldering.pkl, networks.json, csv inputs."""
    return _pick(RUNS / fname, _SCRATCH / fname)


def trace_file(name):
    return _pick(RUNS / 'traces' / f'idata_{name}.nc',
                 _SCRATCH / f'idata_{name}.nc')


def result_file(name):
    return _pick(RUNS / 'results' / f'result_{name}.json',
                 _SCRATCH / f'result_{name}.json')


def trace_names(*globs):
    """Fit names that have BOTH a trace and a result, from either location."""
    import fnmatch
    names = set()
    for d in (RUNS / 'traces', _SCRATCH):
        if not d.is_dir():
            continue
        for p in d.glob('idata_*.nc'):
            n = p.stem[len('idata_'):]
            if any(fnmatch.fnmatch(n, g) for g in globs):
                names.add(n)
    return sorted(n for n in names if result_file(n).exists())
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_time.json'

WINDOW_D = 90        # days per activity window
MIN_SENDS = 5        # sends needed before a window gets a level
MAX_GAP_Y = 1.25     # ignore pairs further apart than this
# Two single-horizon curves, kept only for the comparison. Neither is the
# headline: dividing a gain by one chosen elapsed time is valid only if the
# answer does not depend on which time you chose, and at low grades it does.
# steady_rate() fits across all of ACCRUAL_H instead and tests the premise.
HORIZON_Y = 0.25
LONG_Y = 1.0
ACCRUAL_H = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0]
ACCRUAL_TOL = {0.25: .12, 0.5: .12, 0.75: .15, 1.0: .25, 1.5: .25, 2.0: .35}


def load_sends():
    acc = KayaDataAccessor()
    s = acc.read_sends(source='local_db',
                       columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type'],
                       parse_dates=False, order_by=False)
    ct = s['climb_type'].fillna('').astype(str).str.lower()
    s = s[ct.str.contains('boulder')].copy()
    s['m'] = s['grade'].map(BOULDER_GRADE_TO_NUM)
    s = s[s['m'].notna() & s['user_id'].notna() & s['gym_id'].notna()]
    s['date'] = pd.to_datetime(s['date'], errors='coerce', utc=True).dt.tz_localize(None)
    s = s[s['date'].notna()].copy()
    for c in ('user_id', 'gym_id'):
        s[c] = s[c].astype(str)
    return s


def windows(s):
    """One row per (climber, 90-day window) with enough sends to have a level.

    Level is the max grade in the window -- windowed, not cumulative. A
    cumulative max-so-far is monotone by construction and would manufacture
    improvement out of noise; a windowed max can fall as well as rise.
    """
    t0 = s['date'].min()
    s = s.assign(w=((s['date'] - t0).dt.days // WINDOW_D).astype(int))
    win = (s.groupby(['user_id', 'w'])
             .agg(level=('m', 'max'), n=('m', 'size'), mid=('date', 'mean'))
             .reset_index())
    win = win[win['n'] >= MIN_SENDS].sort_values(['user_id', 'w'])
    win['t'] = (win['mid'] - t0).dt.days / 365.25
    return win.reset_index(drop=True)


def _bin_rows(d, ratio_of_means=False):
    """Collapse a per-triple frame into one row per grade bin.

    ratio_of_means divides once, at the end: mean(dl) / mean(dt) rather than
    mean(dl/dt). Individual elapsed times are short and a window's level is an
    integer, so a climber who gained a grade over 0.09 years would otherwise
    enter the average at +11 grades/yr. The effect turns out to be modest --
    the 0.08-year floor already excludes the worst of it -- but it is free to
    get right, and the per-row quartiles are meaningless either way at a short
    horizon, so `up`/`down` fractions replace them.
    """
    rows = []
    for lvl in range(0, 13):
        b = d[d['level'] == lvl]
        if len(b) < 40:
            continue
        if ratio_of_means:
            dtm = float(b['dt'].mean())
            mean = float(b['dl'].mean()) / dtm
            sem = float(b['dl'].std() / np.sqrt(len(b))) / dtm
        else:
            mean = float(b['rate'].mean())
            sem = float(b['rate'].std() / np.sqrt(len(b)))
        rows.append({
            'v': lvl, 'n': int(len(b)),
            'mean': round(mean, 3), 'sem': round(sem, 3),
            'median': round(float(b['rate'].median()), 3),
            'p25': round(float(b['rate'].quantile(.25)), 3),
            'p75': round(float(b['rate'].quantile(.75)), 3),
            'up': round(float((b['dl'] > 0).mean()), 3),
            'down': round(float((b['dl'] < 0).mean()), 3),
        })
    return rows


def rate_table(win, debias):
    """Grades per year over the next available window. Both flawed variants.

    debias=False bins the change by its own starting level. That level is a
    max over a handful of sends, so a lucky window inflates it and the next
    window looks like a decline -- regression to the max, which mechanically
    tilts the whole curve downward.

    debias=True fixes that by binning on an EARLIER, non-overlapping window:
    w0 assigns the grade bin, the change is measured from w1 to w2. But it
    still divides each climber's change by that climber's own elapsed time,
    and those elapsed times are short (median 3 months) and variable. A whole
    grade gained over 0.09 years enters the average as +11 grades/yr, so the
    mean of the ratios sits far above the ratio of the means. Both are kept
    because the size of the two corrections is the point; the headline number
    comes from horizon_table instead.
    """
    g = win.groupby('user_id')
    d = win.copy()
    if debias:
        d['la'], d['ma'] = g['level'].shift(-1), g['mid'].shift(-1)
        d['lb'], d['mb'] = g['level'].shift(-2), g['mid'].shift(-2)
    else:
        d['la'], d['ma'] = d['level'], d['mid']
        d['lb'], d['mb'] = g['level'].shift(-1), g['mid'].shift(-1)
    d = d.dropna(subset=['la', 'lb']).copy()
    d['dy'] = (d['mb'] - d['ma']).dt.days / 365.25
    d = d[(d['dy'] > 0.08) & (d['dy'] <= MAX_GAP_Y)]
    d['dl'], d['dt'] = d['lb'] - d['la'], d['dy']
    d['rate'] = d['dl'] / d['dt']
    return _bin_rows(d), int(d['user_id'].nunique()), int(len(d)), d


def triples(win, horizons):
    """Every (bin, horizon, change) triple, for each requested horizon.

    w0 still assigns the grade bin and w1 still starts the measurement, so
    the de-biasing is intact. What changes is w2: instead of "the next
    qualifying window", take the qualifying window whose midpoint lands
    closest to `h` years after w1, and only keep it if it lands within
    tolerance. That fixes the denominator, which is what the next-window
    estimator gets wrong.
    """
    out = []
    for uid, sub in win.groupby('user_id', sort=False):
        ts, ls = sub['t'].to_numpy(), sub['level'].to_numpy()
        for i in range(len(sub) - 1):
            t1, l1, lvl = ts[i + 1], ls[i + 1], ls[i]
            for h in horizons:
                j = int(np.argmin(np.abs(ts - (t1 + h))))
                if j <= i + 1 or abs((ts[j] - t1) - h) > ACCRUAL_TOL[h]:
                    continue
                out.append((uid, lvl, h, ls[j] - l1, ts[j] - t1, l1))
    return pd.DataFrame(out,
                        columns=['user_id', 'level', 'h', 'dl', 'dt', 'l1'])


def horizon_table(tri, h):
    """The curve at one fixed horizon, dividing once at the end."""
    d = tri[tri['h'] == h].copy()
    d['rate'] = d['dl'] / d['dt']
    return (_bin_rows(d, ratio_of_means=True),
            int(d['user_id'].nunique()), int(len(d)))


def steady_rate(tri):
    """The headline curve: one rate per bin, fitted across every horizon.

    A rate is a claim that the gain grows in proportion to elapsed time, so
    fit gain = r * h through the origin using all six horizons at once and
    report the chi2 that says whether that claim holds. This is what neither
    single-horizon estimator can do. Picking one horizon and dividing is only
    valid if the answer does not depend on which one you picked -- and at low
    grades it badly does: the V1 gain is the same +0.22 grades after three
    months as after a year, which is a step, not a rate. Dividing that step by
    0.25 manufactures +0.88 grades/yr, and the one-year window would divide
    the identical step by 1.0 and call it +0.22. The fit refuses both and
    tests the premise instead.
    """
    rows = []
    for lvl in range(0, 13):
        xs, ys, es = [], [], []
        for h in ACCRUAL_H:
            c = tri[(tri['level'] == lvl) & (tri['h'] == h)]
            if len(c) < 40:
                continue
            sd = float(c['dl'].std())
            if not sd > 0:
                continue
            xs.append(float(c['dt'].mean()))
            ys.append(float(c['dl'].mean()))
            es.append(sd / np.sqrt(len(c)))
        if len(xs) < 4:
            continue
        xs, ys, es = np.array(xs), np.array(ys), np.array(es)
        w = 1.0 / es**2
        denom = float((w * xs * xs).sum())
        r = float((w * xs * ys).sum()) / denom
        rows.append({
            'v': lvl,
            'mean': round(r, 3),
            'sem': round(float(np.sqrt(1.0 / denom)), 3),
            'chi2': round(float((w * (ys - r * xs)**2).sum()) / (len(xs) - 1), 2),
            'n': int((tri['level'] == lvl).sum()),
            'n_h': len(xs),
        })
    return rows


def by_horizon(tri):
    """The same curve at every horizon -- the evidence for choosing a short one.

    A long second leg credits the starting bin with climbing done after the
    climber has already left it, so the rate is pulled toward the population
    average. That only bites where people move fast enough to leave, which
    makes it a testable prediction: the low bins should fall as the horizon
    grows and the high bins should not.
    """
    out = {}
    for h in ACCRUAL_H:
        rows, _, n = horizon_table(tri, h)
        if n >= 200:
            out[str(h)] = rows
    return out


def start_levels(tri, h):
    """Mean level when measurement starts, against the bin label.

    Bin labels come from a noisy window, so each bin is stocked with climbers
    whose real level sits closer to the population mean. That compresses the
    x-axis and attenuates any slope fitted against it.
    """
    d = tri[tri['h'] == h]
    g = d.groupby('level')
    return [{'v': int(v), 'l1': round(float(s['l1'].mean()), 2), 'n': int(len(s))}
            for v, s in g if len(s) >= 40]


def accrual_table(tri):
    """Mean change against elapsed time, pooled over the bulk of the sample.

    If change accrues linearly the implied annual rate is flat across
    horizons, which says the short-window estimator's problem is arithmetic
    rather than a real plateau in how people improve.
    """
    b = tri[tri['level'].between(3, 8)]
    rows = []
    for h in ACCRUAL_H:
        c = b[b['h'] == h]
        if len(c) < 40:
            continue
        rows.append({
            'h': h, 'n': int(len(c)),
            'dl': round(float(c['dl'].mean()), 4),
            'sem': round(float(c['dl'].std() / np.sqrt(len(c))), 4),
            'rate': round(float(c['dl'].mean() / c['dt'].mean()), 3),
        })
    return rows


def gym_time(s):
    """Per gym: how late its rows sit in its own climbers' careers.

    For each (climber, gym) the date of that climber's hardest send there,
    centred within climber. That within-climber centring is what makes it a
    confound rather than a description -- it asks whether this gym's row is
    late *for this person*, which is exactly the quantity an unmodelled
    improvement trend would smuggle into the gym correction.
    """
    nets = json.loads(data_file('networks.json').read_text())['networks']
    s = s[s['gym_id'].isin(set(nets['net50']))]
    g = ['user_id', 'gym_id']
    hard = s.sort_values(g + ['m']).groupby(g, as_index=False).tail(1)
    hard = hard[g + ['m', 'date']].rename(columns={'date': 'max_date'})
    hard['t'] = (hard['max_date'] - pd.Timestamp('2020-01-01')).dt.days / 365.25
    hard['t_c'] = hard['t'] - hard.groupby('user_id')['t'].transform('mean')

    n_gyms = hard.groupby('user_id')['gym_id'].transform('nunique')
    multi = hard[n_gyms >= 2]
    gap = (multi.groupby('user_id')['t'].max() - multi.groupby('user_id')['t'].min())

    per = multi.groupby('gym_id').agg(t_c=('t_c', 'mean'), n=('t_c', 'size'))
    corr = json.loads((ROOT / 'src/kaya/viewer_static/v2_results.json').read_text())['gyms']
    cf = pd.DataFrame(corr).set_index('i')
    per = per.join(cf[['g', 'b', 'm']], how='inner')

    def stats(x, y):
        return {'r': round(float(np.corrcoef(x, y)[0, 1]), 3),
                'slope': round(float(np.polyfit(x, y, 1)[0]), 3)}

    dd = per.copy()
    for c in ('t_c', 'm'):
        dd[c] = dd[c] - dd.groupby('b')[c].transform('mean')
    by_brand = {b: stats(sub['t_c'], sub['m'])
                for b, sub in per.groupby('b') if len(sub) >= 4}

    return {
        'gyms': [{'id': i, 'g': r['g'], 'b': r['b'], 't_c': round(r['t_c'], 3),
                  'm': round(r['m'], 3), 'n': int(r['n'])}
                 for i, r in per.iterrows()],
        'raw': stats(per['t_c'], per['m']),
        'within_brand': stats(dd['t_c'], dd['m']),
        'by_brand': by_brand,
        'n_multi': int(multi['user_id'].nunique()),
        'gap_median': round(float(gap.median()), 2),
        'gap_p90': round(float(gap.quantile(.90)), 2),
        'gap_over_1y': round(float((gap > 1).mean()), 3),
        'spread_t_c': [round(float(per['t_c'].min()), 3), round(float(per['t_c'].max()), 3)],
    }


def main():
    s = load_sends()
    print(f'{len(s):,} boulder sends, {s.user_id.nunique():,} climbers')
    win = windows(s)

    naive, n_u_naive, n_p_naive, _ = rate_table(win, debias=False)
    short, n_u_short, n_p_short, sd = rate_table(win, debias=True)
    tri = triples(win, ACCRUAL_H)
    shortw, n_u_deb, n_p_deb = horizon_table(tri, HORIZON_Y)
    lng, _, n_p_long = horizon_table(tri, LONG_Y)
    deb = steady_rate(tri)
    acc = accrual_table(tri)
    byh = by_horizon(tri)
    starts = start_levels(tri, HORIZON_Y)

    for tag, rows in (('naive', naive), ('de-biased, next window', short),
                      (f'{HORIZON_Y} yr window only', shortw),
                      (f'{LONG_Y} yr window only', lng)):
        print(f'\n=== {tag} ===')
        print(f'{"grade":>7} {"n":>8} {"mean":>7} {"+/-":>6} {"up":>6} {"down":>6}')
        for r in rows:
            print(f'{"V"+str(r["v"]):>7} {r["n"]:>8,} {r["mean"]:>7.2f} '
                  f'{r["sem"]:>6.2f} {r["up"]:>6.0%} {r["down"]:>6.0%}')

    print('\n=== HEADLINE: steady rate fitted across all horizons ===')
    print(f'{"grade":>7} {"rate":>7} {"+/-":>6} {"chi2/dof":>9} {"horizons":>9} {"n":>8}')
    for r in deb:
        print(f'{"V"+str(r["v"]):>7} {r["mean"]:>+7.2f} {r["sem"]:>6.2f} '
              f'{r["chi2"]:>9.2f} {r["n_h"]:>9} {r["n"]:>8,}')
    fit = np.polyfit([r['v'] for r in deb], [r['mean'] for r in deb], 1)
    print(f'\nheadline mean rate vs grade: slope {fit[0]:+.3f}, intercept {fit[1]:+.3f}')

    print('\nrate by bin and horizon (the dilution test)')
    hs = sorted(byh, key=float)
    print(f'{"bin":>5}' + ''.join(f'{h+"y":>9}' for h in hs))
    for v in range(1, 10):
        cells = []
        for h in hs:
            r = next((x for x in byh[h] if x['v'] == v), None)
            cells.append(f'{r["mean"]:>+9.2f}' if r else f'{".":>9}')
        print(f'{"V"+str(v):>5}' + ''.join(cells))
    print('\nbin label vs actual level when measurement starts')
    print('  ' + '  '.join(f'V{r["v"]}->{r["l1"]:.1f}' for r in starts))

    gaps = sd['dy'] * 12
    print(f'\nnext-window gap, months: median {gaps.median():.1f}  '
          f'p25 {gaps.quantile(.25):.1f}  p75 {gaps.quantile(.75):.1f}  '
          f'p95 {gaps.quantile(.95):.1f}')
    print('\naccrual (V3-V8 pooled): change vs elapsed time')
    for r in acc:
        print(f'  {r["h"]:>4.2f} yr   mean {r["dl"]:>+6.3f}   '
              f'implied rate {r["rate"]:>+6.2f}/yr   n={r["n"]:,}')

    gt = gym_time(s)
    print(f"\ngym/date: r raw {gt['raw']['r']:+.3f}, "
          f"within-brand {gt['within_brand']['r']:+.3f}")

    payload = {
        'window_days': WINDOW_D, 'min_sends': MIN_SENDS, 'max_gap_y': MAX_GAP_Y,
        'horizon_y': HORIZON_Y, 'long_y': LONG_Y,
        'advancement': {
            'naive': naive, 'short': short, 'debiased': deb,
            'short_win': shortw, 'long': lng,
            'accrual': acc, 'by_horizon': byh, 'starts': starts,
            'n_climbers': n_u_deb, 'n_pairs': n_p_deb, 'n_pairs_long': n_p_long,
            'n_climbers_short': n_u_short, 'n_pairs_short': n_p_short,
            'n_climbers_naive': n_u_naive, 'n_pairs_naive': n_p_naive,
            'gap_months': {k: round(float(gaps.quantile(q)), 1)
                           for k, q in (('p25', .25), ('median', .5),
                                        ('p75', .75), ('p95', .95))},
            'fit': {'slope': round(float(fit[0]), 3), 'intercept': round(float(fit[1]), 3)},
        },
        'gym_time': gt,
    }
    OUT.write_text(json.dumps(payload, separators=(',', ':')))
    print(f'\nwrote {OUT}  {OUT.stat().st_size / 1024:.0f} KB')


if __name__ == '__main__':
    main()
