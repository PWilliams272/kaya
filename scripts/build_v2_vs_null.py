"""Score every height form against a FIXED reference, with a pairwise error.

Two changes from build_v2_reliability.py, both about how a comparison should be
read rather than about the models themselves.

**A fixed reference instead of "gap from the best".** Whichever model happens to
win is a noisy choice, and it is chosen using the same data it is scored on, so
the winner's score is biased upward and every other model is measured against an
inflated benchmark. The winner also changes between columns, which silently
moves the zero point and makes a row unreadable across columns. The "no height
term" model is a fixed anchor: it is the same model in every column and both
arms, and a gap from it answers the question the whole comparison exists to ask
-- does modelling height buy anything?

**A pairwise standard error instead of the two totals' errors.** Both models
score the same rows, and their per-row scores are strongly correlated: both find
the same climbers hard to predict. That shared difficulty cancels in the
difference, so the error on the difference is far smaller than the error on
either total. Computing it means differencing per observation *first* --
sqrt(sum of squared individual errors) is not the same number and is much too
big.

The refit noise floor from build_v2_reliability.py measures something else
again, and both are needed: this SE says "given these data, is A really better
than B", the noise floor says "would refitting the identical model move it this
far anyway". A gap must clear both to be worth believing.

Writes src/kaya/viewer_static/v2_vs_null.json, and caches the pointwise scores
in runs/results/pointwise_<arm>.npz so this need not reload 8 GB of traces.

Run from the repo root.
"""
import json
import pickle
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np
import xarray as xr

from kaya.grading_model_v2 import make_dataset

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
TRACES = ROOT / 'runs' / 'traces'
CACHE = ROOT / 'runs' / 'results'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_vs_null.json'

REFERENCE = 'no height term'
SUBSETS = [1, 2, 3, 5]

UNMARGINALIZED = {
    'no height term': 'v3_zero',
    'straight line': 'v3_lin',
    'straight line, refit #2': 'v4_lin_b',
    'straight line, differs by gender': 'v4_linxg',
    'plain curve': 'v3_quad',
    'saturating curve': 'v3_sat',
    'curve, differs by gender': 'v3_conf',
}
ARMS = {
    'unmarginalized': UNMARGINALIZED,
    'marginalized': {k: v + '_marg' for k, v in UNMARGINALIZED.items()},
}


def merged(idata):
    """Marginalized fits write two log-likelihood arrays; restore row order."""
    ll = getattr(idata, 'log_likelihood', None)
    if ll is None or 'm_single' not in ll or 'm_multi' not in ll:
        return idata
    parts, idx = [], []
    for nm, dim in (('m_single', 'obs_single'), ('m_multi', 'obs_multi')):
        da = ll[nm]
        idx.append(np.asarray(da[dim].values))
        parts.append(da.rename({dim: 'obs'}).assign_coords(
            obs=np.arange(da.sizes[dim])))
    m = xr.concat(parts, dim='obs')
    m = m.assign_coords(obs=np.arange(m.sizes['obs']))
    idata.log_likelihood = xr.Dataset({'m_obs': m.isel(obs=np.argsort(
        np.concatenate(idx)))})
    return idata


def pointwise(arm, fits, n_obs):
    """Per-observation cross-validation scores for every fit in an arm."""
    cf = CACHE / f'pointwise_{arm}.npz'
    have = dict(np.load(cf)) if cf.exists() else {}
    changed = False
    for label, name in fits.items():
        if label in have:
            continue
        f = TRACES / f'idata_{name}.nc'
        if not f.exists():
            continue
        lo = az.loo(merged(az.from_netcdf(str(f))), pointwise=True)
        v = np.asarray(lo.loo_i).ravel()
        if len(v) != n_obs:
            print(f'!! {label}: {len(v)} scores vs {n_obs} rows, skipping')
            continue
        have[label] = v
        changed = True
        print(f'   scored {label}')
    if changed:
        np.savez_compressed(cf, **have)
    return have


def main():
    base = pickle.loads((ROOT / 'runs' / 'base_bouldering.pkl').read_bytes())
    nets = json.loads((ROOT / 'runs' / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    _, inv = np.unique(ds.observations['user_id'].to_numpy(), return_inverse=True)
    rows_per_user = np.bincount(inv)[inv]
    n_obs = len(inv)

    out = {'reference': REFERENCE, 'subsets': SUBSETS, 'arms': {}}
    for arm, fits in ARMS.items():
        print(f'\n=== {arm} ===')
        sc = pointwise(arm, fits, n_obs)
        if REFERENCE not in sc:
            print(f'-- reference model "{REFERENCE}" not fitted yet, skipping')
            continue
        ref = sc[REFERENCE]
        models = []
        for label in fits:
            if label not in sc or label == REFERENCE:
                continue
            row = {'label': label, 'by_subset': {}}
            for k in SUBSETS:
                m = rows_per_user >= k
                d = sc[label][m] - ref[m]
                # SE of the total difference: spread of the per-row differences
                # times sqrt(n). The correlated part cancelled row by row.
                se = float(np.std(d, ddof=1) * np.sqrt(len(d)))
                tot = float(d.sum())
                row['by_subset'][str(k)] = {
                    'diff': round(tot, 2), 'se': round(se, 2),
                    'z': round(tot / se, 2) if se > 0 else None}
            models.append(row)
        out['arms'][arm] = {'models': models, 'n_fits': len(sc)}

        w = max(len(m['label']) for m in models) + 2
        print(f'\nbetter than "{REFERENCE}" by, in elpd (higher = predicts '
              f'better); +/- is the\nerror on the difference itself, not on '
              f'either total\n')
        print(' ' * w + ''.join(f'{">=" + str(k) + " sends":>22}' for k in SUBSETS))
        for m in models:
            line = f'{m["label"]:>{w}}'
            for k in SUBSETS:
                b = m['by_subset'][str(k)]
                line += f'{b["diff"]:>+13.1f} +/-{b["se"]:<5.1f}'
            print(line)
        print('\nsame numbers as a multiple of their own error '
              '(|z| > 2 = a real difference)\n')
        print(' ' * w + ''.join(f'{">=" + str(k) + " sends":>16}' for k in SUBSETS))
        for m in models:
            line = f'{m["label"]:>{w}}'
            for k in SUBSETS:
                z = m['by_subset'][str(k)]['z']
                line += f'{z:>16.1f}' if z is not None else f'{"n/a":>16}'
            print(line)

    OUT.write_text(json.dumps(out, separators=(',', ':')))
    print(f'\nwrote {OUT}')


if __name__ == '__main__':
    main()
