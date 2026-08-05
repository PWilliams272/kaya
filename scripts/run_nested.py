"""Nested sampling on the marginalized model: the evidence, and Bayes factors.

An independent route to "which height form does the data prefer" that shares
none of the machinery that has been failing. The leave-one-out ranking depends
on PSIS importance sampling -- that is where the 8,400 effective parameters,
the Pareto k warnings, and the 31-elpd run-to-run noise all live. Nested
sampling computes the marginal likelihood

    Z = INT L(theta) pi(theta) d theta

directly, and the ratio of two models' Z is the Bayes factor. No held-out
points, no importance weights, no Pareto diagnostics.

The catch, stated because it matters: **Z is prior-sensitive in a way
cross-validation is not.** The Bayes factor against a curved height term
depends on the prior width on the curvature coefficient, which was chosen as a
reasonable default rather than elicited. Widen it and the curved model looks
worse, with nothing about the data having changed (Jeffreys-Lindley). So this
runs with a --prior-scale sweep, and a Bayes factor that flips across the
sweep is not a result.

Requires the marginalized model (40 parameters). Nested sampling's constrained
sampling step gets expensive above a few tens of dimensions, and the original
10,397-parameter model is far out of reach.

Run from the repo root. Writes runs/results/nested_<name>.json.
"""
import argparse
import json
import pickle
import time
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np
from scipy.special import ndtri

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'


class PriorCube:
    """Map the unit cube to parameters, which is nested sampling's interface.

    Every prior here is Normal or HalfNormal, so the transform is the normal
    quantile function scaled per coordinate -- exact, not an approximation.
    Sigmas are sampled on the log scale in the model, and a HalfNormal on the
    natural scale is a Normal folded at zero, so its log has a density that is
    NOT Gaussian. Rather than fight that, the cube maps to log-sigma with a
    wide Normal and the model's own log_prior supplies the difference, which
    keeps the target identical to the one PyMC and emcee sample.
    """

    def __init__(self, mm, scale=1.0):
        self.mm = mm
        sd = [5.0, 1.5]                                  # beta0, log_sigma_user
        sd += [mm.prior_sd[nm] * scale for nm in mm.Xnames]
        sd += [1.5]                                      # log_sigma_gym
        sd += [1.0] * (mm.n_gyms - 1)                    # gym offsets
        sd += [1.0, 0.5, 0.5]                            # log_lambda0, kappa, rho
        self.sd = np.array(sd)
        self.mu = np.zeros(mm.n_params)
        self.mu[0] = mm.median_m
        assert len(self.sd) == mm.n_params, (len(self.sd), mm.n_params)

    def __call__(self, u):
        return self.mu + self.sd * ndtri(np.clip(u, 1e-12, 1 - 1e-12))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--name', default='lin')
    ap.add_argument('--height-form', default='linear')
    ap.add_argument('--nlive', type=int, default=800)
    ap.add_argument('--n-quad', type=int, default=31)
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--prior-scale', type=float, default=1.0,
                    help='multiply the coefficient prior widths; the sweep '
                         'that tests whether the Bayes factor is an artefact')
    ap.add_argument('--dlogz', type=float, default=0.5)
    args = ap.parse_args()

    # One BLAS thread per worker. Without this, every one of the N worker
    # processes spawns its own thread pool and they contend for the same
    # cores -- measured at 99 runnable threads on 10 cores, with no gain.
    import os
    for v in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
              'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        os.environ.setdefault(v, '1')

    import dynesty

    from kaya.grading_model_v2 import make_dataset
    from kaya.marginal_v2 import MarginalModel

    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    mm = MarginalModel.from_dataset(ds, height_form=args.height_form,
                                    sigma_link_fixed=0.5, n_quad=args.n_quad)
    cube = PriorCube(mm, scale=args.prior_scale)
    tag = f'{args.name}_s{args.prior_scale:g}'
    print(f'[{tag}] {mm.n_params} parameters, nlive {args.nlive}, '
          f'prior scale {args.prior_scale}')

    def loglike(theta):
        v = mm.log_likelihood(theta)
        return v if np.isfinite(v) else -1e100

    t0 = time.time()
    from multiprocessing import Pool
    with Pool(args.threads) as pool:
        # 'rslice' rather than the default: at 40 dimensions the bounding
        # ellipsoid proposals reject almost everything, and slice sampling is
        # what keeps nested sampling usable at this size.
        s = dynesty.NestedSampler(loglike, cube, mm.n_params,
                                  nlive=args.nlive, sample='rslice',
                                  pool=pool, queue_size=args.threads)
        s.run_nested(dlogz=args.dlogz, print_progress=False)
    el = time.time() - t0
    res = s.results

    logz, logzerr = float(res.logz[-1]), float(res.logzerr[-1])
    print(f'\n[{tag}] log Z = {logz:.2f} +/- {logzerr:.2f}   ({el/60:.1f} min)')
    print(f'[{tag}] {res.niter} iterations, {int(res.ncall)} likelihood calls')

    # Posterior summaries from the weighted samples, for comparison with the
    # other samplers.
    w = np.exp(res.logwt - res.logz[-1])
    mean = np.average(res.samples, weights=w, axis=0)
    var = np.average((res.samples - mean) ** 2, weights=w, axis=0)
    print(f'\n{"parameter":>22} {"mean":>10} {"sd":>9}')
    for j, nm in enumerate(mm.param_names):
        if j > 8 and not nm.startswith(('log_', 'kappa', 'rho')):
            continue
        print(f'{nm:>22} {mean[j]:>10.4f} {np.sqrt(var[j]):>9.4f}')

    out = RUNS / 'results' / f'nested_{tag}.json'
    out.write_text(json.dumps({
        'name': args.name, 'height_form': args.height_form,
        'prior_scale': args.prior_scale, 'nlive': args.nlive,
        'logz': logz, 'logzerr': logzerr, 'niter': int(res.niter),
        'ncall': int(res.ncall), 'elapsed_min': el / 60,
        'param_names': mm.param_names,
        'mean': mean.tolist(), 'sd': np.sqrt(var).tolist(),
    }, indent=2))
    print(f'\n[{tag}] wrote {out}')


if __name__ == '__main__':
    main()
