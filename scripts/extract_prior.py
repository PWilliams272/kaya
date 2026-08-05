"""Sample the prior for the same model v3_conf was fitted with, and merge the
draws into v2_posterior.json so the viewer can overlay prior against posterior.

This is the "did the data actually say this, or did my prior?" check. Prior
sampling needs no NUTS, so it is seconds, not hours.
"""
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore', category=FutureWarning)
import numpy as np
import pymc as pm

from kaya.grading_model_v2 import build_model_v2, make_dataset

TMP = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')
OUT = Path('/Users/peterwilliams/projects/kaya/src/kaya/viewer_static/v2_posterior.json')
N_PRIOR = 2000
THIN_TO = 200

payload = json.loads(OUT.read_text())
wanted = list(payload['params'])

with open(TMP / 'base_bouldering.pkl', 'rb') as f:
    base = pickle.load(f)
nets = json.loads((TMP / 'networks.json').read_text())['networks']

# Same arguments batch_v3.sh used for v3_conf.
ds = make_dataset(base, nets['net50'], name_filter='confident', label='net50/confident')
model = build_model_v2(ds, height_form='quadratic_x_gender', gender_mode='point',
                       estimate_sigma_link=False)

with model:
    pr = pm.sample_prior_predictive(draws=N_PRIOR, var_names=wanted, random_seed=11)

prior = pr.prior
n_missing = 0
for name in wanted:
    if name not in prior:
        print('  no prior for', name); n_missing += 1; continue
    v = np.asarray(prior[name].values).reshape(-1)
    step = max(1, v.size // THIN_TO)
    payload['params'][name]['prior'] = [round(float(x), 5) for x in v[::step][:THIN_TO]]

payload['prior_draws'] = N_PRIOR
OUT.write_text(json.dumps(payload, separators=(',', ':')))
print('merged priors for', len(wanted) - n_missing, 'of', len(wanted), 'params')
print('size', round(OUT.stat().st_size / 1024, 1), 'KB')
