"""How leave-one-out is actually computed, and where it breaks.

Cross-validation here never refits. Refitting 20,014 times at ~85 minutes each
is not a thing anyone does; instead the model is fitted ONCE and the held-out
predictions are estimated from those draws by importance sampling. This script
produces the numbers the write-up uses to show when that estimate can be
trusted and when it cannot.

The weight for draw s on observation i is

    w_s = 1 / p(y_i | theta_s)

-- one over how well that draw explained the row being held out. Draws that fit
row i especially well were over-represented *because* row i was in the fit, so
they get pushed back down.

The intuition that trips people up: for a climber with one observation the
model fits that row almost perfectly, so p(y_i | theta) is large and every
weight is *small*. True, and irrelevant -- weights are normalised, so only
their SPREAD matters. What sinks the estimate is a few draws thousands of times
larger than the rest, and that is exactly what a single-observation climber
produces: removing their only row widens their ability posterior from ~0.5
grades back to the prior's ~1.6, and the draws needed to represent that wider
target are the rare ones out in the tail.

Outputs, per group of climbers by how many rows they have:
  * concentration curve -- what share of the estimate the top x% of draws carry
  * the ratio of the largest weight to a typical one
  * the share of rows whose Pareto k exceeds the reliability threshold

Writes src/kaya/viewer_static/v2_psis.json. Run from the repo root.
"""
import argparse
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_psis.json'

# The ORIGINAL model by default: this is the problem being shown. Pass
# --trace v3_lin_marg to measure whether marginalizing actually fixed it.
TRACE = 'v3_lin'
GROUPS = [(1, 'exactly 1'), (2, 'exactly 2'), (3, 'exactly 3'), (5, '5 or more')]
SAMPLE_ROWS = 400         # rows sampled per group; the curves are medians
# Where to read the concentration curve. Log-ish spacing: the interesting part
# is the first handful of draws, not the bulk.
CURVE_AT = [1, 2, 5, 10, 20, 50, 100, 200, 400, 800, 1200, 1600, 2000]


def merged_log_likelihood(idata):
    """One 'm_obs' group in original row order, whichever arm this trace is.

    The marginalized model has two observed variables -- m_single (the
    climber's offset integrated out) and m_multi (offset still sampled) -- so
    the saved trace carries two log-likelihood groups with no way for arviz to
    know they are one dataset. run_fit.py stitches them for its own LOO call
    but writes the netcdf BEFORE doing so, so anything reading the file back
    has to repeat the stitch. The dimension coordinates hold the original row
    indices, which is what makes the reordering possible.
    """
    import xarray as xr

    ll = idata.log_likelihood
    if 'm_obs' in ll.data_vars:
        return idata.log_likelihood['m_obs'].values
    parts, idx = [], []
    for nm, dim in (('m_single', 'obs_single'), ('m_multi', 'obs_multi')):
        da = ll[nm]
        idx.append(da[dim].values)
        parts.append(da.rename({dim: 'obs'}).assign_coords(
            obs=np.arange(da.sizes[dim])))
    order = np.argsort(np.concatenate(idx))
    merged = xr.concat(parts, dim='obs')
    merged = merged.assign_coords(obs=np.arange(merged.sizes['obs']))
    merged = merged.isel(obs=order)
    # Write it back so az.loo() below sees one group and scores the same rows
    # in the same order as the concentration curves computed from the array.
    idata.log_likelihood = xr.Dataset({'m_obs': merged})
    return merged.values


def main():
    from kaya.grading_model_v2 import make_dataset

    p = argparse.ArgumentParser()
    p.add_argument('--trace', default=TRACE)
    p.add_argument('--out', default=None)
    args = p.parse_args()
    trace = args.trace
    out = Path(args.out) if args.out else OUT

    idata = az.from_netcdf(str(RUNS / 'traces' / f'idata_{trace}.nc'))
    ll = merged_log_likelihood(idata)
    n_draws = ll.shape[0] * ll.shape[1]
    ll = ll.reshape(n_draws, -1)

    base = pickle.loads((RUNS / 'base_bouldering.pkl').read_bytes())
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident', label='')
    uid = ds.observations['user_id'].to_numpy()
    _, inv = np.unique(uid, return_inverse=True)
    rows_per = np.bincount(inv)[inv]

    # Pareto k straight from arviz, so the page quotes the same diagnostic the
    # cross-validation itself used rather than a re-derivation of it.
    loo = az.loo(idata, pointwise=True)
    khat = np.asarray(loo.pareto_k).ravel()

    xs = [c for c in CURVE_AT if c <= n_draws]
    groups = []
    rng = np.random.default_rng(0)
    for k, label in GROUPS:
        idx = (np.flatnonzero(rows_per == k) if k < 5
               else np.flatnonzero(rows_per >= 5))
        pick = rng.choice(idx, size=min(SAMPLE_ROWS, len(idx)), replace=False)
        curves, ratios, tops = [], [], []
        for i in pick:
            logw = -ll[:, i]                    # log(1 / p(y_i | theta))
            w = np.exp(logw - logw.max())       # stabilise before normalising
            w /= w.sum()
            sw = np.sort(w)[::-1]
            curves.append(np.cumsum(sw)[np.array(xs) - 1])
            ratios.append(sw[0] / max(np.median(w), 1e-300))
            tops.append(sw[0])
        groups.append({
            'k': k, 'label': label,
            'n_rows': int(len(idx)),
            'n_climbers': int(len(np.unique(inv[idx]))),
            # Median curve across sampled rows: the typical row's concentration,
            # not an average dragged around by one pathological row.
            'curve': [round(float(v), 5) for v in np.median(curves, axis=0)],
            'max_over_median': round(float(np.median(ratios)), 1),
            'top_draw_share': round(float(np.median(tops)), 5),
            'bad_k': round(float((khat[idx] > 0.7).mean()), 4),
            'k_median': round(float(np.median(khat[idx])), 3),
            'k_p90': round(float(np.percentile(khat[idx], 90)), 3),
        })

    payload = {
        'trace': trace, 'n_draws': int(n_draws), 'curve_x': xs,
        'groups': groups,
        'k_threshold': 0.7,
        'overall_bad_k': round(float((khat > 0.7).mean()), 4),
        # The mechanism in one pair of numbers: how much wider the target gets.
        'sigma_user': round(float(idata.posterior['sigma_user'].values.mean()), 3),
    }
    out.write_text(json.dumps(payload, separators=(',', ':')))

    print(f'{trace}: {n_draws} draws, threshold k > {payload["k_threshold"]}\n')
    print(f'{"climber has":<14}{"rows":>9}{"max/median w":>15}'
          f'{"top draw":>11}{"top 10":>9}{"k>0.7":>9}{"median k":>10}')
    for g in groups:
        top10 = g['curve'][xs.index(10)] if 10 in xs else float('nan')
        print(f'{g["label"]:<14}{g["n_rows"]:>9,}{g["max_over_median"]:>15,.0f}'
              f'{g["top_draw_share"]:>10.1%}{top10:>9.1%}'
              f'{g["bad_k"]:>9.1%}{g["k_median"]:>10.2f}')
    print(f'\nall rows: {payload["overall_bad_k"]:.1%} exceed the threshold')
    print(f'wrote {out}')


if __name__ == '__main__':
    main()
