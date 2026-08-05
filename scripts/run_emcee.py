"""Sample the marginalized model with emcee, as an independent check on PyMC.

Why bother when PyMC already samples it: the two implementations of this
likelihood are different code -- PyTensor graph versus plain NumPy -- and a
quadrature nested inside an ExGaussian is the kind of thing that silently
computes a plausible wrong number. Two samplers, two implementations, one
posterior. If they agree, the graph is right. If they don't, one of them is
wrong and neither was trustworthy.

emcee is an affine-invariant ensemble sampler: a population of walkers propose
moves by looking at where the other walkers are, using no gradients. That
makes it useless on the original 10,397-parameter model -- it needs more
walkers than parameters, and its efficiency collapses in high dimensions
anyway -- but the marginalized model has 40, which is comfortably inside its
range.

Run from the repo root. Writes runs/results/emcee_<name>.npz.
"""
import argparse
import json
import pickle
import time
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--name', default='emcee_lin')
    ap.add_argument('--height-form', default='linear')
    ap.add_argument('--walkers', type=int, default=128)
    ap.add_argument('--steps', type=int, default=4000)
    ap.add_argument('--burn', type=int, default=1000)
    ap.add_argument('--n-quad', type=int, default=31)
    ap.add_argument('--threads', type=int, default=8)
    ap.add_argument('--moves', default='de', choices=['de', 'stretch'],
                    help="'de' mixes differential-evolution moves, which mix "
                         "far better than emcee's default stretch move above "
                         "~10 dimensions; 'stretch' is the default move")
    args = ap.parse_args()

    # One BLAS thread per worker. Without this, every one of the N worker
    # processes spawns its own thread pool and they contend for the same
    # cores -- measured at 99 runnable threads on 10 cores, with no gain.
    import os
    for v in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
              'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        os.environ.setdefault(v, '1')

    import emcee

    from kaya.grading_model_v2 import make_dataset
    from kaya.marginal_v2 import MarginalModel

    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    mm = MarginalModel.from_dataset(ds, height_form=args.height_form,
                                    sigma_link_fixed=0.5, n_quad=args.n_quad)
    ndim = mm.n_params
    nw = max(args.walkers, 2 * ndim + 2)
    print(f'[{args.name}] {ndim} parameters, {nw} walkers, {args.steps} steps, '
          f'{args.moves} moves')
    print(f'[{args.name}] parameters: {", ".join(mm.param_names[:8])}, ...')

    # Start the ensemble as a tight ball around a sensible point. Wide starts
    # in 40 dimensions put most walkers where the likelihood underflows, and
    # the ensemble then spends thousands of steps crawling back.
    rng = np.random.default_rng(0)
    p0 = mm.initial_point(rng)
    start = p0 + 0.02 * rng.standard_normal((nw, ndim))
    bad = [i for i in range(nw) if not np.isfinite(mm.log_posterior(start[i]))]
    if bad:
        raise SystemExit(f'{len(bad)} walkers start at zero probability')

    t0 = time.time()
    from multiprocessing import Pool
    with Pool(args.threads) as pool:
        # emcee's default stretch move degrades badly with dimension: on the
        # first run of this model it left an integrated autocorrelation time of
        # 560 steps, so a 4,000-step chain held only ~7 independent samples per
        # walker and emcee refused to call it converged. The
        # differential-evolution pair is the standard remedy at 40 dimensions --
        # proposals follow the ensemble's own covariance instead of a scalar
        # stretch along one direction.
        moves = ([(emcee.moves.DEMove(), 0.8),
                  (emcee.moves.DESnookerMove(), 0.2)]
                 if args.moves == 'de' else None)
        sampler = emcee.EnsembleSampler(nw, ndim, mm.log_posterior, pool=pool,
                                        moves=moves)
        # progress=False: emcee's tqdm bar does not flush to a redirected log.
        for i, _ in enumerate(sampler.sample(start, iterations=args.steps,
                                             progress=False), 1):
            if i % 100 == 0:
                el = time.time() - t0
                print(f'[{args.name}] step {i}/{args.steps}  '
                      f'{el/60:.1f} min  acc {np.mean(sampler.acceptance_fraction):.3f}',
                      flush=True)
    el = time.time() - t0

    try:
        # Integrated autocorrelation time: how many steps before a draw is
        # effectively independent. emcee raises rather than quietly returning
        # a number it does not trust, which is the useful behaviour.
        tau = sampler.get_autocorr_time(tol=0)
        # Called for the exception, not the value: this is the strict call that
        # raises when the chain is too short to trust its own estimate.
        sampler.get_autocorr_time()
        tau_note = 'converged'
    except Exception as e:
        tau = sampler.get_autocorr_time(tol=0)
        tau_note = f'NOT converged: {e}'
    flat = sampler.get_chain(discard=args.burn, flat=True)
    ess = len(flat) / max(np.nanmax(tau), 1.0)

    print(f'\n[{args.name}] {el/60:.1f} min, '
          f'acceptance {np.mean(sampler.acceptance_fraction):.3f}')
    print(f'[{args.name}] autocorrelation time: max {np.nanmax(tau):.0f} steps '
          f'({tau_note})')
    print(f'[{args.name}] effective sample size ~ {ess:,.0f} '
          f'from {len(flat):,} draws')
    print(f'\n{"parameter":>22} {"mean":>10} {"sd":>9} {"tau":>7}')
    for j, nm in enumerate(mm.param_names):
        if j > 8 and not nm.startswith(('log_', 'kappa', 'rho')):
            continue
        print(f'{nm:>22} {flat[:, j].mean():>10.4f} {flat[:, j].std():>9.4f} '
              f'{tau[j]:>7.0f}')

    out = RUNS / 'results' / f'emcee_{args.name}.npz'
    np.savez_compressed(
        out, chain=sampler.get_chain(discard=args.burn), tau=tau,
        acceptance=sampler.acceptance_fraction,
        param_names=np.array(mm.param_names), elapsed_min=el / 60,
        height_form=args.height_form, n_quad=args.n_quad)
    print(f'\n[{args.name}] wrote {out}  {out.stat().st_size/1e6:.0f} MB')


if __name__ == '__main__':
    main()
