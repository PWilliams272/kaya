"""Advancement rate by grade, with regression-to-the-max removed.

Run: .venv/bin/python scripts/climber_advancement.py   (from the repo root --
running from src/kaya breaks numpy, since src/kaya/secrets.py shadows the
stdlib module numpy's bit generator imports.)

Answers "how fast do climbers move up, and does that depend on where they
already are?" -- both as a standalone question worth a page of its own, and
as the input a grade-dependent time-drift term in the grading model needs.

The naive estimator bins the change (level2 - level1) by level1 -- but level1
is a MAX over a handful of sends, so a lucky window inflates it and the next
window looks like a decline. That mechanically manufactures a downward slope
of rate against level, which is why the naive version has V11 climbers losing
3.6 grades a year: nobody does that.

Fix: bin by a level measured in an EARLIER, non-overlapping window than the
pair being differenced. Window w0 assigns the climber to a grade bin; the
rate is measured from w1 to w2. The noise in the binning variable is then
independent of the noise in the measured change, so it cannot induce a slope.

Cost: needs three windows per climber instead of two, so the sample shrinks.
"""
import warnings
from pathlib import Path
warnings.filterwarnings('ignore')
import numpy as np, pandas as pd
from kaya.data_access import KayaDataAccessor
from kaya.grading_model_v2 import BOULDER_GRADE_TO_NUM

OUT = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')
WINDOW_D, MIN_SENDS, MAX_GAP_Y = 90, 5, 1.25

acc = KayaDataAccessor()
s = acc.read_sends(source='local_db',
                   columns=['user_id', 'date', 'grade', 'climb_type'],
                   parse_dates=False, order_by=False)
ct = s['climb_type'].fillna('').astype(str).str.lower()
s = s[ct.str.contains('boulder')].copy()
s['m'] = s['grade'].map(BOULDER_GRADE_TO_NUM)
s = s[s['m'].notna() & s['user_id'].notna()]
s['date'] = pd.to_datetime(s['date'], errors='coerce', utc=True).dt.tz_localize(None)
s = s[s['date'].notna()]
s['user_id'] = s['user_id'].astype(str)
t0 = s['date'].min()
s['w'] = ((s['date'] - t0).dt.days // WINDOW_D).astype(int)

win = (s.groupby(['user_id', 'w'])
         .agg(level=('m', 'max'), n=('m', 'size'), mid=('date', 'mean'))
         .reset_index())
win = win[win['n'] >= MIN_SENDS].sort_values(['user_id', 'w'])

g = win.groupby('user_id')
win['l1'] = g['level'].shift(-1)      # the pair being differenced
win['m1'] = g['mid'].shift(-1)
win['l2'] = g['level'].shift(-2)
win['m2'] = g['mid'].shift(-2)
p = win.dropna(subset=['l1', 'l2']).copy()
p['dy'] = (p['m2'] - p['m1']).dt.days / 365.25
p = p[(p['dy'] > 0.08) & (p['dy'] <= MAX_GAP_Y)]
p['rate'] = (p['l2'] - p['l1']) / p['dy']

print(f'{len(p):,} triples, {p.user_id.nunique():,} climbers '
      f'(binned by window w0, rate measured w1->w2)\n')
print(f'{"V-grade":>8} {"n":>8} {"mean":>7} {"median":>7} {"p25":>7} {"p75":>7}'
      '   grades/yr, de-biased')
rows = []
for lvl in range(0, 12):
    b = p[p['level'] == lvl]
    if len(b) < 40:
        continue
    rows.append({'level': lvl, 'n': len(b), 'mean': round(b['rate'].mean(), 3),
                 'median': round(b['rate'].median(), 3),
                 'p25': round(b['rate'].quantile(.25), 3),
                 'p75': round(b['rate'].quantile(.75), 3),
                 'sem': round(b['rate'].std() / np.sqrt(len(b)), 3)})
    print(f'{"V"+str(lvl):>8} {len(b):>8,} {b["rate"].mean():>7.2f} '
          f'{b["rate"].median():>7.2f} {b["rate"].quantile(.25):>7.2f} '
          f'{b["rate"].quantile(.75):>7.2f}')
df = pd.DataFrame(rows)
df.to_csv(OUT / 'advancement_debiased.csv', index=False)
print(f'\nwrote {OUT / "advancement_debiased.csv"}')
print('\nmean rate vs grade, least squares:',
      np.polyfit(df['level'], df['mean'], 1).round(3))
