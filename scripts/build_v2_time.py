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
TMP = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_time.json'

WINDOW_D = 90        # days per activity window
MIN_SENDS = 5        # sends needed before a window gets a level
MAX_GAP_Y = 1.25     # ignore pairs further apart than this


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
    return win[win['n'] >= MIN_SENDS].sort_values(['user_id', 'w'])


def rate_table(win, debias):
    """Grades per year, binned by grade.

    debias=False bins the change by its own starting level. That level is a
    max over a handful of sends, so a lucky window inflates it and the next
    window looks like a decline -- regression to the max, which mechanically
    tilts the whole curve downward. It is reported anyway because the size of
    the difference is the point.

    debias=True bins by an EARLIER, non-overlapping window instead: window w0
    assigns the grade bin, the rate is measured from w1 to w2. The noise in
    the binning variable is then independent of the noise in the measured
    change, so it cannot induce a slope. Costs a third of the sample.
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
    d['rate'] = (d['lb'] - d['la']) / d['dy']

    rows = []
    for lvl in range(0, 13):
        b = d[d['level'] == lvl]
        if len(b) < 40:
            continue
        rows.append({
            'v': lvl, 'n': int(len(b)),
            'mean': round(float(b['rate'].mean()), 3),
            'median': round(float(b['rate'].median()), 3),
            'p25': round(float(b['rate'].quantile(.25)), 3),
            'p75': round(float(b['rate'].quantile(.75)), 3),
            'sem': round(float(b['rate'].std() / np.sqrt(len(b))), 3),
        })
    return rows, int(d['user_id'].nunique()), int(len(d))


def gym_time(s):
    """Per gym: how late its rows sit in its own climbers' careers.

    For each (climber, gym) the date of that climber's hardest send there,
    centred within climber. That within-climber centring is what makes it a
    confound rather than a description -- it asks whether this gym's row is
    late *for this person*, which is exactly the quantity an unmodelled
    improvement trend would smuggle into the gym correction.
    """
    nets = json.loads((TMP / 'networks.json').read_text())['networks']
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

    naive, n_u_naive, n_p_naive = rate_table(win, debias=False)
    deb, n_u_deb, n_p_deb = rate_table(win, debias=True)
    for tag, rows in (('naive', naive), ('de-biased', deb)):
        print(f'\n=== {tag} ===')
        print(f'{"grade":>7} {"n":>8} {"mean":>7} {"median":>7} {"p25":>7} {"p75":>7}')
        for r in rows:
            print(f'{"V"+str(r["v"]):>7} {r["n"]:>8,} {r["mean"]:>7.2f} '
                  f'{r["median"]:>7.2f} {r["p25"]:>7.2f} {r["p75"]:>7.2f}')
    fit = np.polyfit([r['v'] for r in deb], [r['mean'] for r in deb], 1)
    print(f'\nde-biased mean rate vs grade: slope {fit[0]:+.3f}, intercept {fit[1]:+.3f}')

    gt = gym_time(s)
    print(f"\ngym/date: r raw {gt['raw']['r']:+.3f}, "
          f"within-brand {gt['within_brand']['r']:+.3f}")

    payload = {
        'window_days': WINDOW_D, 'min_sends': MIN_SENDS, 'max_gap_y': MAX_GAP_Y,
        'advancement': {
            'naive': naive, 'debiased': deb,
            'n_climbers': n_u_deb, 'n_pairs': n_p_deb,
            'n_climbers_naive': n_u_naive, 'n_pairs_naive': n_p_naive,
            'fit': {'slope': round(float(fit[0]), 3), 'intercept': round(float(fit[1]), 3)},
        },
        'gym_time': gt,
    }
    OUT.write_text(json.dumps(payload, separators=(',', ':')))
    print(f'\nwrote {OUT}  {OUT.stat().st_size / 1024:.0f} KB')


if __name__ == '__main__':
    main()
