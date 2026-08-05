"""Bridge sampling: the evidence from MCMC draws already in hand.

The cheap third opinion on the same question nested sampling answers. Given
posterior draws from any sampler, bridge sampling estimates the marginal
likelihood Z without a dedicated run -- it fits a Gaussian proposal to the
draws and iterates a fixed-point equation relating draws from the posterior to
draws from the proposal (Meng & Wong 1996).

Less robust than nested sampling: it assumes the fitted proposal overlaps the
posterior well, which is safe for a unimodal 40-parameter posterior and not
safe in general. Its value here is that it costs minutes rather than hours and
uses a completely different estimator, so agreement with nested sampling is
evidence for both and disagreement means neither should be quoted.

Run from the repo root. Reads an emcee run and writes
runs/results/bridge_<name>.json.
"""
import argparse
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np
from scipy.special import logsumexp
from scipy.stats import multivariate_normal

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'


def bridge_logz(log_post, draws, rng, n_prop=None, tol=1e-8, max_iter=1000):
    """Warp-I bridge sampling with a Gaussian proposal.

    Split the draws in two: one half fits the proposal, the other enters the
    estimator. Fitting and evaluating on the same draws biases Z upward, and
    the bias is invisible without the split.
    """
    n = len(draws)
    fit, use = draws[: n // 2], draws[n // 2:]
    mu, cov = fit.mean(axis=0), np.cov(fit, rowvar=False)
    # Ridge the covariance: with 40 parameters and correlated draws the sample
    # covariance is often near-singular, and a singular proposal silently
    # produces inf.
    cov = cov + 1e-8 * np.eye(len(mu)) * np.trace(cov) / len(mu)
    prop = multivariate_normal(mu, cov, allow_singular=True)

    n_prop = n_prop or len(use)
    q_draws = prop.rvs(size=n_prop, random_state=rng)

    l1 = np.array([log_post(x) for x in use]) - prop.logpdf(use)
    l2 = np.array([log_post(x) for x in q_draws]) - prop.logpdf(q_draws)
    l1, l2 = l1[np.isfinite(l1)], l2[np.isfinite(l2)]
    n1, n2 = len(l1), len(l2)
    s1, s2 = n1 / (n1 + n2), n2 / (n1 + n2)

    logz = 0.0
    for it in range(max_iter):
        num = logsumexp(l2 - np.logaddexp(np.log(s1) + l2, np.log(s2) + logz)) - np.log(n2)
        den = logsumexp(-np.logaddexp(np.log(s1) + l1, np.log(s2) + logz)) - np.log(n1)
        new = num - den
        if abs(new - logz) < tol:
            logz = new
            break
        logz = new
    return float(logz), it + 1, n1, n2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--emcee', required=True,
                    help='name of an emcee run, e.g. emcee_lin')
    ap.add_argument('--thin', type=int, default=20,
                    help='thin the chain; bridge sampling wants roughly '
                         'independent draws, not many correlated ones')
    ap.add_argument('--n-quad', type=int, default=31)
    args = ap.parse_args()

    from kaya.grading_model_v2 import make_dataset
    from kaya.marginal_v2 import MarginalModel

    f = RUNS / 'results' / f'{args.emcee}.npz'
    if not f.exists():
        raise SystemExit(f'no such emcee run: {f}')
    d = np.load(f, allow_pickle=True)
    height_form = str(d['height_form'])
    chain = d['chain']                                   # (steps, walkers, ndim)
    flat = chain.reshape(-1, chain.shape[-1])[:: args.thin]
    print(f'[{args.emcee}] height form {height_form}, '
          f'{len(flat):,} draws after thinning by {args.thin}')

    with open(RUNS / 'base_bouldering.pkl', 'rb') as fh:
        base = pickle.load(fh)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    mm = MarginalModel.from_dataset(ds, height_form=height_form,
                                    sigma_link_fixed=0.5, n_quad=args.n_quad)

    rng = np.random.default_rng(0)
    logz, iters, n1, n2 = bridge_logz(mm.log_posterior, flat, rng)
    print(f'[{args.emcee}] log Z = {logz:.2f}  '
          f'({iters} fixed-point iterations, {n1:,} posterior / {n2:,} proposal draws)')

    # Repeat on a different split to get a crude spread. Not a standard error
    # -- a proper one needs the autocorrelation of the draws -- but it does
    # catch the case where the answer depends on which half fitted the proposal.
    reps = [logz]
    for seed in (1, 2, 3):
        r = np.random.default_rng(seed)
        perm = r.permutation(len(flat))
        reps.append(bridge_logz(mm.log_posterior, flat[perm], r)[0])
    print(f'[{args.emcee}] across 4 splits: '
          f'{np.mean(reps):.2f} +/- {np.std(reps):.2f} '
          f'(range {max(reps)-min(reps):.2f})')

    out = RUNS / 'results' / f'bridge_{args.emcee}.json'
    out.write_text(json.dumps({
        'emcee_run': args.emcee, 'height_form': height_form,
        'logz': logz, 'logz_reps': reps,
        'logz_mean': float(np.mean(reps)), 'logz_sd': float(np.std(reps)),
        'n_draws': int(len(flat)), 'thin': args.thin,
    }, indent=2))
    print(f'[{args.emcee}] wrote {out}')


if __name__ == '__main__':
    main()
