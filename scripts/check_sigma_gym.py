"""Why do the two samplers disagree about sigma_gym, and is it a bug?

emcee and PyMC/NUTS agree to within 0.24 posterior standard deviations on
eleven of the twelve parameters they share, and disagree by 1.86 on the
twelfth: sigma_gym, the spread of grading style across gyms, which is the
headline number of the whole model.

Something that lands on eleven parameters and misses one is far more likely to
be a sampling difference than a coding error -- but "more likely" is not a
check. This script eliminates the three places a coding error could hide.

1. THE PRIOR. `check_pymc_marginal.py` compares
   `model.logp(vars=model.observed_RVs)` -- the LIKELIHOOD only. The prior has
   never been compared between the two implementations, and it is where a
   sigma_gym-specific error would most plausibly live: the parameter is sampled
   on the log scale in NumPy and the natural scale in PyMC, so a missing
   Jacobian would tilt exactly this one profile and nothing else.

   Profiled across a grid of sigma_gym with everything else held fixed, the two
   log-priors must differ by a CONSTANT. A constant offset is harmless -- it is
   the epsilon_raw prior PyMC carries and NumPy integrates out. Any variation
   across the grid is a different prior shape, which is a different model.

2. THE QUADRATURE. The likelihood cross-check pins every multi-observation
   climber's ability offset to zero, which collapses the Gauss-Hermite integral
   to a single node -- so the quadrature itself is the one piece of the NumPy
   implementation that check never exercises. An under-resolved integral over
   climber offsets would bias precisely the variance components.

   Re-evaluated at 31, 61, 121 and 201 nodes, the log-posterior difference
   between the two candidate sigma_gym values must be stable.

3. THE SAMPLERS' OWN DIAGNOSTICS, which is what is left if 1 and 2 pass.
   Reported, not judged: PyMC's effective sample size and R-hat for this
   parameter against emcee's.

Writes runs/results/sigma_gym_check.json, which build_v2_emcee.py folds into
the viewer payload so the page can quote these numbers rather than assert them.
Run from the repo root.
"""
import argparse
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

from kaya.grading_model_v2 import build_model_v2, make_dataset
from kaya.marginal_v2 import MarginalModel
from kaya.viewer_paths import data_file, result_file

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'runs' / 'results' / 'sigma_gym_check.json'
NODES = (31, 61, 121, 201)


def dataset():
    with open(data_file('base_bouldering.pkl'), 'rb') as f:
        base = pickle.load(f)
    nets = json.loads(data_file('networks.json').read_text())['networks']
    return make_dataset(base, nets['net50'], name_filter='confident',
                        label='net50/confident')


def pymc_point(mm, model, ip, theta):
    """mm's flat vector as a PyMC point, with every climber offset at zero."""
    p = mm.unpack(theta)
    pt_ = {}
    for k in ip:
        b = k.split('_log__')[0].split('_zerosum__')[0]
        if b == 'beta0':
            pt_[k] = np.array(p['beta0'])
        elif b == 'sigma_user':
            pt_[k] = np.array(p['log_sigma_user'])
        elif b == 'sigma_gym':
            pt_[k] = np.array(p['log_sigma_gym'])
        elif b == 'epsilon_raw':
            pt_[k] = np.zeros(ip[k].shape)
        elif b == 'gym_correction_raw':
            pt_[k] = p['gym_raw'][:ip[k].shape[0]]
        elif b == 'log_lambda0':
            pt_[k] = np.array(p['log_lambda0'])
        elif b == 'kappa':
            pt_[k] = np.array(p['kappa'])
        elif b == 'rho':
            pt_[k] = np.array(p['rho'])
        elif b in mm.Xnames:
            pt_[k] = np.array(p['beta'][mm.Xnames.index(b)])
        else:
            raise SystemExit(f'unmapped variable {k!r}')
    return pt_


def check_prior(ds, mm, theta, jsg, grid):
    model = build_model_v2(ds, height_form='linear', gender_mode='point',
                           estimate_sigma_link=False, sigma_link_fixed=0.5,
                           marginalize_singles=True)
    ip = model.initial_point()
    # Every free random variable's log-probability, transforms included, and
    # nothing from the likelihood.
    fn = model.compile_fn(model.logp(vars=model.free_RVs, sum=True))

    print('1. PRIOR SHAPE\n')
    print(f'{"sigma_gym":>10} {"log_sigma_gym":>14} {"NumPy":>16} '
          f'{"PyMC":>16} {"difference":>14}')
    diffs = []
    for lsg in grid:
        th = theta.copy()
        th[jsg] = lsg
        a = mm.log_prior(th)
        b = float(fn(pymc_point(mm, model, ip, th)))
        diffs.append(a - b)
        print(f'{np.exp(lsg):>10.4f} {lsg:>14.3f} {a:>16.6f} {b:>16.6f} '
              f'{a - b:>14.6f}')
    spread = float(np.ptp(diffs))
    print(f'\n   spread of the difference across the grid: {spread:.3e}')
    print('   PASS -- same prior shape' if spread < 1e-8 else
          '   FAIL -- the two priors are different functions of sigma_gym')
    return spread


def check_quadrature(ds, theta, jsg, points):
    print('\n2. QUADRATURE RESOLUTION\n')
    print(f'{"nodes":>7}' + ''.join(
        f'{f"logp @ {np.exp(v):.3f}":>20}' for v in points))
    gaps = {}
    for nq in NODES:
        mm = MarginalModel.from_dataset(ds, height_form='linear',
                                        sigma_link_fixed=0.5, n_quad=nq)
        vals = []
        for lsg in points:
            th = theta.copy()
            th[jsg] = lsg
            vals.append(mm.log_posterior(th))
        gaps[nq] = float(vals[1] - vals[0])
        print(f'{nq:>7} ' + ''.join(f'{v:>20.6f}' for v in vals))
    ref = gaps[NODES[-1]]
    drift = max(abs(g - ref) for g in gaps.values())
    print('\n   difference between the two points, by node count:')
    for nq, g in gaps.items():
        print(f'     n_quad={nq:>4}: {g:+.6f}')
    print(f'   largest movement from the finest grid: {drift:.3e}')
    print('   PASS -- 31 nodes resolve the integral' if drift < 1e-2 else
          '   FAIL -- the quadrature is not converged at the node count used')
    return drift, gaps


def pymc_fleet(names):
    """sigma_gym across every PyMC fit that reports it.

    One fit disagreeing with emcee is ambiguous. A set of fits spanning
    different height forms, both design bases and a 4x longer warm-up, all
    landing in the same narrow band, is not: whatever emcee is doing, it is not
    a property of the settings of any single PyMC run.
    """
    rows = []
    for n in names:
        f = result_file(n)
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        p = r['params'].get('sigma_gym')
        if not p:
            continue
        rows.append({'fit': n, 'mean': p.get('mean'), 'lo': p.get('hdi_5.5%'),
                     'hi': p.get('hdi_94.5%'), 'ess': p.get('ess_bulk'),
                     'rhat': p.get('r_hat'),
                     'tune': (r.get('args') or {}).get('tune'),
                     'draws': (r.get('args') or {}).get('draws'),
                     'height_form': (r.get('args') or {}).get('height_form'),
                     'orthogonal': bool((r.get('args') or {}).get('orthogonal_design'))})
    return rows


def sampler_diagnostics(emcee_tag, pymc_name):
    """What each sampler says about how well it drew this parameter."""
    out = {}
    f = ROOT / 'runs' / 'results' / f'emcee_{emcee_tag}.npz'
    if f.exists():
        d = np.load(f, allow_pickle=True)
        names = [str(s) for s in d['param_names']]
        j = names.index('log_sigma_gym')
        x = d['chain'][:, :, j]
        sg = np.exp(x.ravel())
        tau = float(np.asarray(d['tau'], float)[j])
        out['emcee'] = {
            'mean': float(sg.mean()),
            'lo': float(np.percentile(sg, 5.5)),
            'hi': float(np.percentile(sg, 94.5)),
            'ess': float(x.size / max(tau, 1.0)),
            'tau': tau,
            # A running mean still trending at the end would mean emcee is the
            # unconverged one, so the drift over the second half is the
            # relevant number rather than the endpoint.
            'drift_2nd_half': float(abs(x[x.shape[0] // 2:].mean()
                                        - x[:x.shape[0] // 2].mean())),
        }
    r = result_file(pymc_name)
    if r.exists():
        p = json.loads(r.read_text())['params'].get('sigma_gym', {})
        out['pymc'] = {
            'mean': p.get('mean'), 'lo': p.get('hdi_5.5%'),
            'hi': p.get('hdi_94.5%'), 'ess': p.get('ess_bulk'),
            'rhat': p.get('r_hat'),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--emcee', default='lin2')
    ap.add_argument('--pymc', default='v3_lin_marg')
    ap.add_argument('--n-quad', type=int, default=31,
                    help='node count the fits actually used')
    ap.add_argument('--pymc-all', default=(
        'v3_lin_marg,v3_conf_marg,v5_conf_marg_long,v6_conf_orth,v3_zero_orth,'
        'v3_lin_orth,v3_quad_orth,v4_linxg_orth,v3_sat_orth,v3_vtx_orth'),
        help='every PyMC fit to poll for sigma_gym, as corroboration')
    args = ap.parse_args()

    ds = dataset()
    mm = MarginalModel.from_dataset(ds, height_form='linear',
                                    sigma_link_fixed=0.5, n_quad=args.n_quad)
    jsg = mm.param_names.index('log_sigma_gym')

    diag = sampler_diagnostics(args.emcee, args.pymc)
    if 'emcee' not in diag or 'pymc' not in diag:
        raise SystemExit('need both an emcee npz and a PyMC result to compare')
    # The two contested values, taken from the runs rather than hard-coded, so
    # the profile is evaluated exactly where the samplers actually disagree.
    points = (float(np.log(diag['pymc']['mean'])),
              float(np.log(diag['emcee']['mean'])))
    print(f"emcee says sigma_gym = {diag['emcee']['mean']:.4f}, "
          f"{args.pymc} says {diag['pymc']['mean']:.4f}\n")

    # A real draw, not the initial point: the profile should be taken somewhere
    # the posterior actually has mass.
    d = np.load(ROOT / 'runs' / 'results' / f'emcee_{args.emcee}.npz',
                allow_pickle=True)
    theta = d['chain'][-1, 0, :].copy()

    grid = np.linspace(points[0] - 0.6, points[1] + 0.6, 9)
    prior_spread = check_prior(ds, mm, theta, jsg, grid)
    quad_drift, gaps = check_quadrature(ds, theta, jsg, points)

    print('\n3. WHAT EACH SAMPLER SAYS ABOUT ITS OWN DRAW OF THIS PARAMETER\n')
    e, p = diag['emcee'], diag['pymc']
    print(f"   emcee : sigma_gym {e['mean']:.4f} "
          f"[{e['lo']:.4f}, {e['hi']:.4f}]  ESS {e['ess']:,.0f}  "
          f"tau {e['tau']:.0f}  running-mean drift {e['drift_2nd_half']:.4f}")
    print(f"   PyMC  : sigma_gym {p['mean']:.4f} "
          f"[{p['lo']:.4f}, {p['hi']:.4f}]  ESS {p['ess']:,.0f}  "
          f"R-hat {p['rhat']}")

    fleet = pymc_fleet([n.strip() for n in args.pymc_all.split(',') if n.strip()])
    if fleet:
        print('\n4. EVERY PyMC FIT THAT REPORTS sigma_gym\n')
        print(f"{'fit':>20} {'form':>20} {'basis':>6} {'tune/draws':>11} "
              f"{'sigma_gym':>10} {'ess':>6} {'rhat':>6}")
        for r in sorted(fleet, key=lambda r: r['mean']):
            basis = 'orth' if r['orthogonal'] else 'raw'
            print(f"{r['fit']:>20} {str(r['height_form']):>20} {basis:>6} "
                  f"{str(r['tune']) + '/' + str(r['draws']):>11} "
                  f"{r['mean']:>10.3f} {r['ess']:>6.0f} {r['rhat']:>6}")
        lo = min(r['mean'] for r in fleet)
        hi = max(r['mean'] for r in fleet)
        print(f'\n   {len(fleet)} fits span {lo:.3f}-{hi:.3f}; '
              f"emcee is at {diag['emcee']['mean']:.3f}")

    verdict = ('sampling' if prior_spread < 1e-8 and quad_drift < 1e-2
               else 'implementation')
    print(f'\nVERDICT: the disagreement is a {verdict.upper()} difference.')
    if verdict == 'sampling':
        print('Neither the prior, the likelihood nor the quadrature differs '
              'between the\ntwo implementations, so the gap is in how the '
              'posterior was explored.')
        if len(fleet) > 2 and not (lo <= diag['emcee']['mean'] <= hi):
            print(f'\nAnd emcee sits OUTSIDE the band {len(fleet)} PyMC fits '
                  'agree on, across different\nheight forms, both design bases '
                  'and a 4x longer warm-up. A single fit\ndisagreeing would be '
                  'ambiguous; this is not -- the outlier is emcee.')

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        'emcee_tag': args.emcee, 'pymc_name': args.pymc,
        'prior_spread': prior_spread,
        'quad_nodes': list(NODES), 'quad_gaps': gaps, 'quad_drift': quad_drift,
        'n_quad_used': args.n_quad,
        'verdict': verdict, 'pymc_fleet': fleet, **diag,
    }, indent=1, default=float))
    print(f'\nwrote {OUT}')


if __name__ == '__main__':
    main()
