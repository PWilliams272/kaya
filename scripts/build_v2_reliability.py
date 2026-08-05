"""Is the height-form comparison measuring anything? Data for the write-up.

The comparison ranks seven height forms by leave-one-out cross-validation.
This script asks whether that ranking is real, by two tests:

1. **Refit the same model.** Identical model, identical data, different random
   seed. Whatever separates two such runs is noise, and it is the yardstick
   every gap between different models has to clear.

2. **Score only well-observed climbers.** An observation is one (climber, gym)
   pair, and 59% of climbers contribute exactly one. Hide it and nothing is
   left to estimate that climber's ability offset from, so leave-one-out is
   extrapolating for those rows. Cross-validation already produces a score per
   observation, so restricting to climbers with several rows needs no
   refitting -- just a different subset of the same numbers, the same subset
   for every model.

Writes src/kaya/viewer_static/v2_reliability.json.

Run from the repo root -- running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np
import xarray as xr

from kaya.convergence import RHAT_GATE
from kaya.grading_model_v2 import make_dataset

ROOT = Path(__file__).resolve().parents[1]
# The durable archive, not the job scratch directory: these traces are
# 30-60 minutes of sampling each and scratch is deleted with the job.
RUNS = ROOT / 'runs'
TMP = RUNS / 'traces'
RESULTS = RUNS / 'results'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_reliability.json'

# label -> (trace name, is this a refit of another entry?)
#
# Two arms, kept side by side on purpose. UNMARGINALIZED is the original
# model, where every climber carries their own ability offset; it is what
# produced the 31.1-elpd run-to-run noise. MARGINALIZED integrates out the
# offsets of climbers with a single observation. Reporting that the fix
# worked requires keeping the thing it fixed.
UNMARGINALIZED = {
    'straight line': ('v3_lin', None),
    'straight line, refit #2': ('v4_lin_b', 'straight line'),
    'straight line, refit #3': ('v4_lin_c', 'straight line'),
    'straight line, refit #4': ('v4_lin_d', 'straight line'),
    'straight line, refit #5': ('v4_lin_e', 'straight line'),
    'straight line, differs by gender': ('v4_linxg', None),
    'curve, differs by gender': ('v3_conf', None),
    'saturating curve': ('v3_sat', None),
    'plain curve': ('v3_quad', None),
    'no height term': ('v3_zero', None),
}
MARGINALIZED = {
    label: (name + '_marg', rep)
    for label, (name, rep) in UNMARGINALIZED.items()
}
ARMS = {'unmarginalized': UNMARGINALIZED, 'marginalized': MARGINALIZED}
SUBSETS = [1, 2, 3, 5]
# RHAT_GATE (imported from kaya.convergence, the one place it is defined) is
# the bar below which a fit is not evidence: above it a score measures a broken
# chain rather than sampling noise, so the fit is kept out of the noise floor
# and named in `noise_excluded`.


def _merged(idata):
    """Put the marginalized arm's two likelihood groups back in row order.

    The marginalized model has two observed variables -- climbers with one send
    and climbers with several -- so it writes two log-likelihood arrays, each
    carrying the original row indices it covers. Cross-validation needs one
    array in the dataset's own row order, or the per-row scores line up with
    the wrong rows and every subset below is silently wrong.
    """
    ll = getattr(idata, 'log_likelihood', None)
    if ll is None or 'm_single' not in ll or 'm_multi' not in ll:
        return idata
    parts, idx = [], []
    for nm, dim in (('m_single', 'obs_single'), ('m_multi', 'obs_multi')):
        da = ll[nm]
        idx.append(np.asarray(da[dim].values))
        parts.append(da.rename({dim: 'obs'}).assign_coords(
            obs=np.arange(da.sizes[dim])))
    merged = xr.concat(parts, dim='obs')
    merged = merged.assign_coords(obs=np.arange(merged.sizes['obs']))
    merged = merged.isel(obs=np.argsort(np.concatenate(idx)))
    idata.log_likelihood = xr.Dataset({'m_obs': merged})
    return idata


def score_arm(fits, obs, rows_per_user):
    """Load an arm's fits and score every model on every subset.

    Returns None if none of the arm's traces exist yet, so the page can show
    the unmarginalized results before the marginalized fits have finished.
    """
    elpd, kbad, rhat, present = {}, {}, {}, []
    for label, (name, _) in fits.items():
        f = TMP / f'idata_{name}.nc'
        if not f.exists():
            continue
        lo = az.loo(_merged(az.from_netcdf(str(f))), pointwise=True)
        v = np.asarray(lo.loo_i).ravel()
        if len(v) != len(obs):
            print(f'!! {label}: {len(v)} pointwise values vs {len(obs)} rows')
            continue
        elpd[label] = v
        kbad[label] = np.asarray(lo.pareto_k).ravel()
        rj = RESULTS / f'result_{name}.json'
        rhat[label] = (json.loads(rj.read_text())['max_rhat']
                       if rj.exists() else float('nan'))
        present.append(label)
    if not present:
        return None

    models = []
    for label in present:
        row = {'label': label, 'replicate_of': fits[label][1], 'by_subset': {}}
        for k in SUBSETS:
            mask = rows_per_user >= k
            best = max(elpd[m][mask].sum() for m in present)
            row['by_subset'][str(k)] = {
                'total': round(float(elpd[label][mask].sum()), 2),
                'gap': round(float(elpd[label][mask].sum() - best), 2),
                'bad_k': round(float((kbad[label][mask] > 0.7).mean()), 4),
            }
        models.append(row)

    # The noise floor: how far apart refits of the identical model land.
    #
    # A fit that did not converge is not a draw from the posterior, so its
    # score does not measure sampling noise -- it measures a failure. Including
    # one turns the floor into a statement about the worst chain rather than
    # about the method. They are excluded and named, not silently dropped: a
    # refit failing to converge is itself worth reporting.
    reps = [m for m in present if fits[m][1] is not None]
    base_label = fits[reps[0]][1] if reps else None
    noise, excluded = {}, []
    if base_label and base_label in elpd:
        group = [base_label] + reps
        ok = [m for m in group if rhat.get(m, 0) <= RHAT_GATE]
        excluded = [{'label': m, 'max_rhat': round(rhat[m], 3)}
                    for m in group if m not in ok]
        for k in SUBSETS:
            mask = rows_per_user >= k
            vals = np.array([elpd[m][mask].sum() for m in ok])
            noise[str(k)] = {
                'n_runs': int(len(vals)),
                'sd': round(float(vals.std(ddof=1)), 2) if len(vals) > 1 else None,
                'range': round(float(vals.max() - vals.min()), 2) if len(vals) else None,
            }
    return {'models': models, 'noise': noise, 'n_fits': len(present),
            'noise_excluded': excluded, 'rhat_gate': RHAT_GATE,
            'replicate_group': ([base_label] + reps) if base_label else []}


def show(name, arm):
    print(f'\n=== {name} ===  ({arm["n_fits"]} fits)')
    print('noise floor -- spread across refits of the identical model:')
    for k, v in arm['noise'].items():
        sd = f'{v["sd"]:.2f}' if v['sd'] is not None else 'n/a'
        print(f'  >={k} rows: {v["n_runs"]} runs, sd {sd}, range {v["range"]:.2f}')
    for ex in arm.get('noise_excluded', []):
        print(f'  !! excluded from the floor: {ex["label"]} did not converge '
              f'(max R-hat {ex["max_rhat"]}, gate {arm.get("rhat_gate")})')
    print('\ngap from the best model in each column '
          '(0 = best; more negative = predicts worse)')
    hdr = ''.join(f'{">="+str(k)+" rows":>13}' for k in SUBSETS)
    print(f'{"model":>36}{hdr}')
    for m in arm['models']:
        cells = ''.join(f'{m["by_subset"][str(k)]["gap"]:>+13.1f}' for k in SUBSETS)
        print(f'{m["label"]:>36}{cells}')


def main():
    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    obs = ds.observations
    uid = obs['user_id'].to_numpy()
    _, inv = np.unique(uid, return_inverse=True)
    rows_per_user = np.bincount(inv)[inv]

    subsets = []
    for k in SUBSETS:
        mask = rows_per_user >= k
        subsets.append({'k': k, 'rows': int(mask.sum()),
                        'share': round(float(mask.mean()), 4),
                        'climbers': int(len(set(uid[mask])))})

    arms = {}
    for name, fits in ARMS.items():
        arm = score_arm(fits, obs, rows_per_user)
        if arm:
            arms[name] = arm
            show(name, arm)
        else:
            print(f'\n=== {name} ===  no traces found yet, skipping')

    payload = {'n_obs': int(len(obs)), 'n_climbers': int(len(set(uid))),
               'subsets': subsets, 'arms': arms,
               # The page defaults to the marginalized arm once it exists.
               'primary': 'marginalized' if 'marginalized' in arms
                          else 'unmarginalized'}
    OUT.write_text(json.dumps(payload, separators=(',', ':')))

    print('\nsubsets: ' + ', '.join(
        f'>={s["k"]} rows -> {s["rows"]:,} rows / {s["climbers"]:,} climbers'
        for s in subsets))
    print(f'\nwrote {OUT}  {OUT.stat().st_size/1024:.0f} KB')


if __name__ == '__main__':
    main()
