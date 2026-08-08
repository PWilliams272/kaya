"""Build `v2_prelim.json` — the v10 height sweep, as preliminary results.

Why a separate payload from `v2_posterior.json`
-----------------------------------------------
That one carries the v3/v4 fits, which sampled every climber offset. The v10
fits integrate all 4,241 of them out by Gauss-Hermite quadrature, so they have
40 sampled parameters instead of 4,241, a different log-likelihood layout
(`log_lik_multi` in the posterior group rather than `log_likelihood`), and a
different leave-one-out UNIT. Mixing them into one payload would let the viewer
put two incomparable elpds in one column, which is the exact failure this
sweep exists to avoid.

Frozen chains are dropped, loudly
---------------------------------
Two of these fits reported R-hat 1.53 / ESS 7 / 1,500 divergences — identical
numbers from models sharing no height parameters, because in each one chain had
adapted its step size to exactly zero and never left its initial point. R-hat
and ESS are BETWEEN-chain statistics, so a dead chain reports as a broken
model. Every summary here is computed on the chains that actually sampled, and
`fit['frozen_chains']` records which were dropped so the page can say so rather
than quietly showing three-chain numbers as if they were four.

The elpd is per CLIMBER
-----------------------
Integrating out a climber's offset makes their rows conditionally dependent, so
leave-one-ROW-out no longer exists in closed form. Each climber contributes one
term: `m_single` for the 6,156 who logged at one gym, the `log_lik_multi`
deterministic for the 4,201 who logged at several. Comparable across these five
fits and to nothing else. Computed by scripts/elpd, read in via --elpd.

The paired difference is the number that matters
------------------------------------------------
Two fits score the SAME climbers, so climber-to-climber variation — which is
almost all of the raw standard error — cancels when the scores are differenced
per climber before summing. On this sweep the raw per-fit error is ~285 and the
paired error is ~0.6: a 450x difference, and quoting the raw one would make
every comparison here look unanswerable.

    python scripts/build_v2_prelim.py --elpd path/to/elpd.json

Run from the repo root. Writes src/kaya/viewer_static/v2_prelim.json.
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

import arviz as az
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
STATIC = ROOT / 'src' / 'kaya' / 'viewer_static'
OUT = STATIC / 'v2_prelim.json'

# Ordered worst-to-best on the model-free probe, which is also the order the
# page walks them in. The label is what the reader sees.
FITS = [
    ('v10_zero_marg', 'zero', 'no height at all'),
    ('v10_linxg_marg', 'linear_x_gender', 'straight line, slope differs by gender'),
    ('v10_lin_marg', 'linear', 'one straight line'),
    ('v10_quad_marg', 'quadratic', 'one curve'),
    ('v10_conf_marg', 'quadratic_x_gender', 'curve whose shape differs by gender'),
]
REFERENCE = 'v10_lin_marg'      # every paired difference is taken against this

# Sampled parameters worth showing. gym_correction* are 29-vectors and belong
# to the gyms section, not here.
PARAMS = ['beta0', 'sigma_user', 'sigma_gym', 'log_lambda0', 'lambda0',
          'kappa', 'rho', 'beta_gender', 'gamma1', 'gamma2', 'gamma1_x',
          'gamma2_x', 'delta1', 'delta2', 'beta_h_missing', 'beta_a_missing']

# Draws kept per chain for traces and corner panels. Thinned with the SAME
# stride across every parameter of a fit, so draw i of one parameter still
# corresponds to draw i of another -- that joint structure is the only thing
# that makes a corner panel meaningful.
THIN_TO = 200

# Grid for the body-dimension curves, in inches away from the median climber.
# +/-8in is roughly the 5th-95th percentile, so the curve stays inside the data
# instead of extrapolating into a range nobody occupies.
BODY_HALF_RANGE = 8.0
BODY_POINTS = 41
BAND = 0.89        # highest-density interval for the shaded band


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--elpd', default=None,
                   help='JSON of per-climber PSIS-LOO, as written by the elpd '
                        'pass. Omitted, the payload carries no scores.')
    p.add_argument('--out', default=str(OUT))
    p.add_argument('--thin-to', type=int, default=THIN_TO)
    return p.parse_args(argv)


def hdi_bounds(x, prob=BAND):
    """Narrowest interval containing `prob` of the mass, along the last axis."""
    x = np.sort(np.asarray(x), axis=-1)
    n = x.shape[-1]
    k = max(1, int(np.floor(prob * n)))
    widths = x[..., k:] - x[..., :n - k]
    i = np.argmin(widths, axis=-1)
    lo = np.take_along_axis(x, i[..., None], axis=-1)[..., 0]
    hi = np.take_along_axis(x, (i + k)[..., None], axis=-1)[..., 0]
    return lo, hi


def frozen(idata):
    sys.path.insert(0, str(ROOT / 'src'))
    from kaya.convergence import frozen_chains
    return frozen_chains(idata)


def design_constants():
    """The centring and scaling `prepare_design` applies, so the curves can be
    drawn back in inches rather than in the model's z-units."""
    sys.path.insert(0, str(ROOT / 'src'))
    from kaya.grading_model_v2 import make_dataset

    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident')
    u = ds.users
    h, a = u['height'].to_numpy(float), u['ape_index'].to_numpy(float)
    return {
        'h_med': float(np.nanmedian(h)), 'h_sd': float(np.nanstd(h)) or 1.0,
        'a_med': float(np.nanmedian(a)), 'a_sd': float(np.nanstd(a)) or 1.0,
        'w_female_mean': float(np.nanmean(u['w_female'].fillna(0.5))),
        'n_users': int(len(u)),
        'height_p5': float(np.nanpercentile(h, 5)),
        'height_p95': float(np.nanpercentile(h, 95)),
        'ape_p5': float(np.nanpercentile(a, 5)),
        'ape_p95': float(np.nanpercentile(a, 95)),
    }


def draws(post, name):
    """Flat draw vector for one parameter, or None when the fit lacks it."""
    if name not in post:
        return None
    return np.asarray(post[name].values).reshape(-1)


def body_curve(post, consts, kind, gender):
    """Ability contribution across body size, with a credible band.

    `kind` is 'height' or 'ape'. `gender` is the gender weight plugged into the
    interaction columns: 0 male-coded, 1 female-coded, or the population mean.

    Each posterior draw gives a whole curve; the curve is centred to mean zero
    across the grid *within that draw* before the band is taken. Centring
    matters: the design matrix is mean-centred during fitting, so the level of
    an uncentred curve is arbitrary and its band would be dominated by that
    arbitrary level rather than by the shape under discussion.
    """
    if kind == 'height':
        med, sd, lin, quad = consts['h_med'], consts['h_sd'], 'gamma1', 'gamma2'
        lin_x, quad_x = 'gamma1_x', 'gamma2_x'
    else:
        med, sd, lin, quad = consts['a_med'], consts['a_sd'], 'delta1', 'delta2'
        lin_x, quad_x = 'delta1_x', 'delta2_x'

    inches = np.linspace(-BODY_HALF_RANGE, BODY_HALF_RANGE, BODY_POINTS)
    z = inches / sd                                    # model units

    b1 = draws(post, lin)
    if b1 is None:
        return None
    b2 = draws(post, quad)
    b1x, b2x = draws(post, lin_x), draws(post, quad_x)

    # (n_draws, n_grid)
    y = b1[:, None] * z[None, :]
    if b2 is not None:
        y = y + b2[:, None] * (z ** 2)[None, :]
    if b1x is not None:
        y = y + gender * b1x[:, None] * z[None, :]
    if b2x is not None:
        y = y + gender * b2x[:, None] * (z ** 2)[None, :]

    y = y - y.mean(axis=1, keepdims=True)
    lo, hi = hdi_bounds(y.T, BAND)
    return {
        'x': [round(float(med + v), 2) for v in inches],
        'x_offset': [round(float(v), 2) for v in inches],
        'mean': [round(float(v), 4) for v in y.mean(axis=0)],
        'lo': [round(float(v), 4) for v in lo],
        'hi': [round(float(v), 4) for v in hi],
        # Peak-to-trough of the mean curve: the single number the ledger quotes.
        'span': round(float(np.ptp(y.mean(axis=0))), 3),
        'has_gender_terms': bool(b1x is not None or b2x is not None),
    }


def summarise(name, label, blurb, consts, thin_to):
    trace = RUNS / 'traces' / f'idata_{name}.nc'
    if not trace.exists():
        return None
    idata = az.from_netcdf(str(trace))
    fz = frozen(idata)
    keep = [c for c in range(idata.posterior.sizes['chain']) if c not in fz]
    kept = idata.sel(chain=keep)
    post = kept.posterior

    n_chains, n_draws = post.sizes['chain'], post.sizes['draw']
    stride = max(1, n_draws // thin_to)

    params = {}
    for p in PARAMS:
        if p not in post:
            continue
        v = np.asarray(post[p].values)             # (chain, draw)
        if v.ndim != 2:
            continue
        flat = v.reshape(-1)
        lo, hi = hdi_bounds(flat[None, :], BAND)
        params[p] = {
            'chains': [[round(float(x), 5) for x in c[::stride]] for c in v],
            'mean': round(float(flat.mean()), 5),
            'sd': round(float(flat.std(ddof=1)), 5),
            'lo': round(float(lo[0]), 5), 'hi': round(float(hi[0]), 5),
            'rhat': round(float(az.rhat(kept, var_names=[p])[p].values), 4),
            'ess_bulk': round(float(az.ess(kept, var_names=[p])[p].values)),
            'ess_tail': round(float(
                az.ess(kept, var_names=[p], method='tail')[p].values)),
        }

    st = kept.sample_stats
    div = int(np.asarray(st['diverging'].values).sum())
    td = np.asarray(st['tree_depth'].values)
    step = np.asarray(st['step_size'].values)

    res_path = RUNS / 'results' / f'result_{name}.json'
    res = json.loads(res_path.read_text()) if res_path.exists() else {}

    gender_variants = {}
    for gkey, gval in [('male', 0.0), ('female', 1.0),
                       ('average', consts['w_female_mean'])]:
        gender_variants[gkey] = {
            'height': body_curve(post, consts, 'height', gval),
            'ape': body_curve(post, consts, 'ape', gval),
        }

    return {
        'name': name, 'height_form': label, 'blurb': blurb,
        'n_chains_run': int(idata.posterior.sizes['chain']),
        'n_chains_kept': n_chains,
        'frozen_chains': fz,
        'n_draws': int(n_draws), 'thin_stride': int(stride),
        'max_rhat': round(max((v['rhat'] for v in params.values()), default=0.0), 4),
        'min_ess': round(min((v['ess_bulk'] for v in params.values()), default=0.0)),
        'divergences': div,
        'step_size': round(float(np.median(step[:, -1] if step.ndim > 1 else step)), 5),
        'tree_depth_mean': round(float(td.mean()), 2),
        'minutes': round(float(res.get('elapsed_min', 0.0)), 1),
        'params': params,
        'curves': gender_variants,
    }


def attach_scores(fits, elpd_path):
    """Fold in per-climber PSIS-LOO and the paired differences."""
    raw = json.loads(Path(elpd_path).read_text())
    ref = np.asarray(raw[REFERENCE]['pw'], float)
    for f in fits:
        r = raw.get(f['name'])
        if not r:
            continue
        pw = np.asarray(r['pw'], float)
        d = pw - ref
        n = len(d)
        # Paired standard error: the scores are differenced PER CLIMBER before
        # summing, so everything the two fits share cancels. Quoting the raw
        # per-fit SE instead compares how much CLIMBERS differ, not how much
        # MODELS differ, and is ~450x larger here.
        se_paired = float(np.sqrt(n) * d.std(ddof=1)) if n > 1 else float('nan')
        total = float(d.sum())
        f['loo'] = {
            'elpd': round(float(r['elpd']), 1),
            'se_raw': round(float(r['se']), 1),
            'n_climbers': int(r['n_single'] + r['n_multi']),
            'n_single': int(r['n_single']), 'n_multi': int(r['n_multi']),
            'pareto_k_max': round(float(r['kmax']), 3),
            'pareto_k_over_0p7': int(r['k_over']),
            'vs_reference': {
                'reference': REFERENCE,
                'delta': round(total, 2),
                'se_paired': round(se_paired, 2),
                'sigma': (None if not se_paired or np.isnan(se_paired)
                          else round(total / se_paired, 2)),
            },
        }
    return fits


def main(argv=None) -> int:
    args = parse_args(argv)
    consts = design_constants()
    print(f'design constants: height median {consts["h_med"]:.1f}in '
          f'(sd {consts["h_sd"]:.2f}), ape median {consts["a_med"]:.1f}in '
          f'(sd {consts["a_sd"]:.2f})')

    fits = []
    for name, label, blurb in FITS:
        f = summarise(name, label, blurb, consts, args.thin_to)
        if f is None:
            print(f'  {name}: no trace, skipped')
            continue
        note = (f'  DROPPED chain(s) {f["frozen_chains"]}'
                if f['frozen_chains'] else '')
        print(f'  {name:<18} {f["n_chains_kept"]}/{f["n_chains_run"]} chains  '
              f'R-hat {f["max_rhat"]:.3f}  ESS {f["min_ess"]:.0f}{note}')
        fits.append(f)

    if args.elpd and Path(args.elpd).exists():
        fits = attach_scores(fits, args.elpd)
        print('\nper-climber PSIS-LOO, paired against '
              f'{REFERENCE} (higher elpd is better):')
        for f in fits:
            lo = f.get('loo')
            if not lo:
                continue
            v = lo['vs_reference']
            s = '  reference' if f['name'] == REFERENCE else (
                f'  {v["delta"]:+8.2f} +/- {v["se_paired"]:.2f}'
                f'  = {v["sigma"]:+.2f} sigma')
            print(f'  {f["name"]:<18} elpd {lo["elpd"]:>+12,.1f}{s}')
    elif args.elpd:
        print(f'\n!! {args.elpd} not found — payload written without scores')

    payload = {
        'built_at': __import__('pandas').Timestamp.now().strftime('%Y-%m-%d %H:%M'),
        'network': 'net50', 'name_filter': 'confident',
        'reference': REFERENCE, 'band': BAND, 'thin_to': args.thin_to,
        'consts': consts, 'fits': fits,
        'loo_unit': 'climber',
        'status': 'preliminary',
    }
    Path(args.out).write_text(json.dumps(payload, default=float))
    print(f'\nwrote {Path(args.out).relative_to(ROOT)} '
          f'({Path(args.out).stat().st_size / 1e6:.2f} MB)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
