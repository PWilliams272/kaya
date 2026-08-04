"""Is the gym correction confounded with when its climbers were climbing?

The model has no time in it at all. An observation is a (user, gym) hardest
send, so if a climber logged at gym A in 2022 and gym B in 2025, the model
attributes the whole difference to the gyms and none of it to three years of
getting better. That biases a gym's correction by how *late* its climbers'
sends sit relative to those same climbers' sends elsewhere.

The smoking-gun quantity is the within-user centred date: for each (user,
gym), the date of their max there minus the mean of their own max-dates
across all their gyms. Average that per gym and correlate with the fitted
correction. A positive correlation means recent gyms look stiffer purely
because their climbers were better by then.
"""
import json, pickle
from pathlib import Path
import numpy as np
import pandas as pd
from kaya.data_access import KayaDataAccessor
from kaya.grading_model_v2 import BOULDER_GRADE_TO_NUM

T = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')
nets = json.loads((T / 'networks.json').read_text())['networks']
keep = set(nets['net50'])

acc = KayaDataAccessor()
sends = acc.read_sends(source='local_db',
                       columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type'],
                       parse_dates=False, order_by=False)
ct = sends['climb_type'].fillna('').astype(str).str.lower()
sends = sends[ct.str.contains('boulder')].copy()
sends['m'] = sends['grade'].map(BOULDER_GRADE_TO_NUM)
sends = sends[sends['m'].notna() & sends['user_id'].notna() & sends['gym_id'].notna()]
for c in ('user_id', 'gym_id'):
    sends[c] = sends[c].astype(str)
sends = sends[sends['gym_id'].isin(keep)].copy()
sends['date'] = pd.to_datetime(sends['date'], errors='coerce', utc=True).dt.tz_localize(None)
sends = sends[sends['date'].notna()]
print(f'{len(sends):,} boulder sends across {sends.gym_id.nunique()} net50 gyms')

g = ['user_id', 'gym_id']
# Date of each (user, gym) hardest send -- the row the model actually uses.
hardest = sends.sort_values(g + ['m']).groupby(g, as_index=False).tail(1)
hardest = hardest[g + ['m', 'date']].rename(columns={'date': 'max_date'})
hardest['t'] = (hardest['max_date'] - pd.Timestamp('2020-01-01')).dt.days / 365.25

# Within-user centring: how late this gym's row sits in this climber's own life.
hardest['t_user_mean'] = hardest.groupby('user_id')['t'].transform('mean')
hardest['t_c'] = hardest['t'] - hardest['t_user_mean']
n_gyms_per_user = hardest.groupby('user_id')['gym_id'].transform('nunique')
multi = hardest[n_gyms_per_user >= 2]
print(f'{multi.user_id.nunique():,} multi-gym climbers carry the gym contrasts')

gap = (multi.groupby('user_id')['t'].max() - multi.groupby('user_id')['t'].min())
print('\nyears between a multi-gym climber\'s first and last max:')
print(f'  median {gap.median():.2f}   p75 {gap.quantile(.75):.2f}   '
      f'p90 {gap.quantile(.90):.2f}   >1yr: {(gap > 1).mean()*100:.0f}%   '
      f'>2yr: {(gap > 2).mean()*100:.0f}%')

per_gym = multi.groupby('gym_id').agg(
    t_c=('t_c', 'mean'), med_date=('max_date', 'median'), n=('t_c', 'size'))
corr = json.loads((Path('src/kaya/viewer_static/v2_results.json')).read_text())['gyms']
cf = pd.DataFrame(corr).set_index('i')
per_gym = per_gym.join(cf[['g', 'b', 'm']], how='inner')

r_tc = np.corrcoef(per_gym['t_c'], per_gym['m'])[0, 1]
age = (per_gym['med_date'] - pd.Timestamp('2020-01-01')).dt.days / 365.25
r_age = np.corrcoef(age, per_gym['m'])[0, 1]
print(f'\ncorr(mean within-user centred date, gym correction) = {r_tc:+.3f}')
print(f'corr(gym median max-date,           gym correction) = {r_age:+.3f}')
print(f'spread of mean within-user centred date across gyms: '
      f'{per_gym["t_c"].min():+.2f} to {per_gym["t_c"].max():+.2f} years')

pd.set_option('display.width', 200)
out = per_gym.sort_values('t_c')[['g', 'b', 't_c', 'm', 'n']]
out.columns = ['gym', 'brand', 'mean_within_user_yrs', 'correction', 'n_multi']
print('\n' + out.to_string(float_format=lambda v: f'{v:+.3f}'))
