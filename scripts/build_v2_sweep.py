"""The height-form sweep, and why its ranking cannot be read yet.

Seven height forms, refit on 2026-08-06 with a long warm-up on the raw
parameter basis. The obvious thing to publish is the leaderboard: sort by
leave-one-out score, declare a winner. That would be wrong, and this payload
exists to make the reason visible rather than to hide it in a footnote.

The whole spread from best form to worst is ~33 elpd. Two fits of the
*identical* model, differing only by random seed, have scored 31.1 apart in
this project. So the entire ranking sits inside its own measurement error, and
until Phase 3 measures that floor properly, no form is distinguishable from any
other.

Two further things the sweep settled that are worth their own panels:

  * **Convergence.** Six of seven still miss the R-hat bar and all seven miss
    the effective-sample-size floor -- with zero divergences and the sampler
    saturating its tree depth, which is a conditioning signature rather than a
    "needs more draws" one.
  * **Chains versus draws.** Measured by subsetting these very traces: both
    levers scale effective sample size roughly linearly, but only draws move
    R-hat. That kills the "run 4 chains now, add more later" strategy this
    project was built around, because PyMC cannot resume a chain -- draws are a
    one-shot decision at launch.

Writes src/kaya/viewer_static/v2_sweep.json. Run from the repo root.
"""
import argparse
import json
import re
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_sweep.json'
LOGS = ROOT / 'runs' / 'logs' / 'overnight'

# The sweep, in the order the height forms nest: each adds one term to the one
# before it, except the last two which are different shapes entirely.
FITS = [
    ('v7_v3_zero', 'zero', 'no height term at all', 'the null'),
    ('v7_v3_lin', 'linear', 'one slope', 'grades rise steadily with height'),
    ('v7_v3_quad', 'quadratic', 'slope + curvature',
     'allows an optimum rather than a trend'),
    ('v7_v4_linxg', 'linear × gender', 'one slope per gender',
     'lets the height effect differ by gender'),
    ('v7_v3_conf', 'quadratic × gender', 'curvature per gender',
     'the page primary'),
    ('v7_v3_sat', 'saturating', 'rises then flattens',
     'height helps until it stops helping'),
    ('v7_v3_vtx', 'vertex quadratic', 'quadratic in peak/curvature form',
     'same shape as quadratic, different parameters'),
]

# Reported scalars -- the quantities the page actually quotes. Deliberately not
# the 4,201 per-climber offsets, whose diagnostics nobody reads.
VARS = ['beta0', 'beta_gender', 'sigma_gym', 'sigma_user', 'delta1', 'delta2',
        'log_lambda0', 'kappa', 'rho']

RHAT_BAR = 1.01
ESS_FLOOR = 400

# Measured in this project, recorded in docs/run-plan.md: two fits of one model
# differing only by seed. Any gap smaller than this is not a result.
NOISE_FLOOR_ELPD = 31.1


def _verdict(spread):
    """Is the leaderboard readable, given how noisy a refit is?

    `separated` needs the spread to clear the floor by enough that it is not
    itself a coin flip. 2x is the threshold used here and it is a judgement,
    not a theorem -- but a 5% margin over the noise floor is plainly not one.
    """
    if spread is None:
        return 'unknown'
    if spread > 2 * NOISE_FLOOR_ELPD:
        return 'separated'
    if spread > NOISE_FLOOR_ELPD:
        return 'marginal'
    return 'inside-noise'


def trace_path(name):
    return ROOT / 'runs' / 'traces' / f'idata_{name}.nc'


def parse_log(name):
    """Wall time, divergences and LOO, read back out of the run's own log."""
    f = LOGS / f'{name}.log'
    out = {'minutes': None, 'divergences': None, 'loo': None, 'loo_se': None,
           'max_treedepth': 0}
    if not f.exists():
        return out
    txt = f.read_text(errors='ignore')
    m = re.search(r'sampled in ([\d.]+) min \| divergences (\d+)/(\d+)', txt)
    if m:
        out['minutes'] = float(m.group(1))
        out['divergences'] = int(m.group(2))
        out['draws_total'] = int(m.group(3))
    m = re.search(r'LOO elpd = (-?[\d.]+) \+/- ([\d.]+)', txt)
    if m:
        out['loo'] = float(m.group(1))
        out['loo_se'] = float(m.group(2))
    out['max_treedepth'] = len(re.findall(r'reached the maximum tree depth', txt))
    return out


def diagnostics(idata):
    """Worst R-hat and worst effective sample size across the reported scalars."""
    post = idata.posterior
    vs = [v for v in VARS if v in post.data_vars]
    rh = az.rhat(post, var_names=vs)
    es = az.ess(post, var_names=vs)
    worst_r = max(((float(np.asarray(rh[v].values).max()), v) for v in vs))
    worst_e = min(((float(np.asarray(es[v].values).min()), v) for v in vs))
    return {
        'rhat': worst_r[0], 'rhat_param': worst_r[1],
        'ess': worst_e[0], 'ess_param': worst_e[1],
        'chains': int(post.sizes['chain']), 'draws': int(post.sizes['draw']),
    }


def scaling(idata):
    """Do chains and draws move R-hat differently? Measured on this trace.

    Subset by chain (holding draws fixed) and by draw (holding chains fixed),
    and recompute both diagnostics each time. If ESS is additive across chains
    the chain ratios track the chain count; if R-hat only responds to chain
    LENGTH, the chain column stays flat while the draw column falls.
    """
    post = idata.posterior
    vs = [v for v in VARS if v in post.data_vars]
    n_ch, n_dr = int(post.sizes['chain']), int(post.sizes['draw'])

    def score(sub):
        es = az.ess(sub, var_names=vs)
        tot = float(sum(np.asarray(es[v].values).sum() for v in vs))
        if int(sub.sizes['chain']) < 2:
            return tot, None
        rh = az.rhat(sub, var_names=vs)
        return tot, float(max(np.asarray(rh[v].values).max() for v in vs))

    by_chain, by_draw = [], []
    base = None
    for k in range(1, n_ch + 1):
        ess, rhat = score(post.isel(chain=slice(0, k)))
        base = ess if k == 1 else base
        by_chain.append({'n': k, 'ess': ess, 'ratio': ess / base, 'rhat': rhat})
    base = None
    for n in [int(n_dr * f) for f in (0.25, 0.5, 0.75, 1.0)]:
        ess, rhat = score(post.isel(draw=slice(0, n)))
        base = ess if base is None else base
        by_draw.append({'n': n, 'ess': ess, 'ratio': ess / base, 'rhat': rhat,
                        'ess_per_draw': ess / n})
    return {'by_chain': by_chain, 'by_draw': by_draw}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scaling-fit', default='v7_v3_quad',
                    help='which trace to run the chains-vs-draws study on')
    args = ap.parse_args()

    rows, missing = [], []
    for name, form, terms, note in FITS:
        p = trace_path(name)
        if not p.exists():
            missing.append(name)
            continue
        idata = az.from_netcdf(str(p))
        d = diagnostics(idata)
        log = parse_log(name)
        rows.append({
            'fit': name, 'form': form, 'terms': terms, 'note': note,
            **d, **log,
            'rhat_ok': d['rhat'] <= RHAT_BAR,
            'ess_ok': d['ess'] >= ESS_FLOOR,
        })

    if not rows:
        raise SystemExit('no v7 traces found; nothing to build')

    loos = [r['loo'] for r in rows if r['loo'] is not None]
    best = max(loos) if loos else None
    for r in rows:
        r['delta_loo'] = None if r['loo'] is None or best is None \
            else r['loo'] - best

    spread = (max(loos) - min(loos)) if len(loos) > 1 else None
    sc = scaling(az.from_netcdf(str(trace_path(args.scaling_fit))))

    payload = {
        'generated_from': 'runs/traces/idata_v7_*.nc',
        'fits': rows,
        'missing': missing,
        'bars': {'rhat': RHAT_BAR, 'ess': ESS_FLOOR},
        'ranking': {
            'spread_elpd': spread,
            'noise_floor_elpd': NOISE_FLOOR_ELPD,
            'ratio': None if spread is None else spread / NOISE_FLOOR_ELPD,
            # Three states, not a boolean. The spread being a hair over the
            # floor is not "readable" -- a difference you can only see because
            # it beat the noise by 5% is a difference you cannot see. Anything
            # under ~2x the floor should be read as "no form is separated from
            # any other yet".
            'verdict': _verdict(spread),
            'best_form': max(rows, key=lambda r: (r['loo'] is not None, r['loo']))['form'],
        },
        'convergence': {
            'n': len(rows),
            'rhat_pass': sum(1 for r in rows if r['rhat_ok']),
            'ess_pass': sum(1 for r in rows if r['ess_ok']),
            'divergences_total': sum(r.get('divergences') or 0 for r in rows),
            'treedepth_fits': sum(1 for r in rows if r['max_treedepth'] > 0),
        },
        'scaling': {'fit': args.scaling_fit, **sc},
        'settings': {
            'tune': 2000, 'draws': rows[0]['draws'], 'chains': rows[0]['chains'],
            'basis': 'raw', 'network': 'net50/confident',
        },
    }

    OUT.write_text(json.dumps(payload, indent=1, default=float))
    print(f'wrote {OUT.relative_to(ROOT)}  ({OUT.stat().st_size / 1024:.0f} KB)')
    print(f'  {len(rows)} fits, {len(missing)} missing')
    print(f"  R-hat pass {payload['convergence']['rhat_pass']}/{len(rows)}, "
          f"ESS pass {payload['convergence']['ess_pass']}/{len(rows)}")
    if spread is not None:
        print(f'  LOO spread {spread:.1f} vs noise floor {NOISE_FLOOR_ELPD} '
              f"({spread / NOISE_FLOOR_ELPD:.2f}x) -> {payload['ranking']['verdict']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
