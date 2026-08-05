"""Validate the marginalized likelihood against things known to be true.

Three checks, in increasing strength:

1. The single-observation closed form must equal a quadrature evaluation of
   the same integral. This is the check that can be exact -- both compute the
   same number, one analytically and one numerically, so agreement to ~1e-9
   says the algebra is right.
2. Quadrature must have converged: doubling the node count must not move the
   answer.
3. The marginal likelihood must equal the full model's likelihood with the
   climber offsets integrated out by brute force -- for a small subset where
   brute force is affordable.

Run from the repo root; running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

from kaya.grading_model_v2 import make_dataset
from kaya.marginal_v2 import MarginalModel, exgaussian_logpdf

TMP = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')


def load(height_form='linear', n_quad=31):
    with open(TMP / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((TMP / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    return MarginalModel.from_dataset(ds, height_form=height_form,
                                      sigma_link_fixed=0.5, n_quad=n_quad)


def check_single_obs_closed_form(mm, rng):
    """The exact branch against numerical integration of the same integral."""
    theta = mm.initial_point(rng)
    p = mm.unpack(theta)
    su, sl = p['sigma_user'], mm.sigma_link_fixed
    user_term = p['beta0'] + mm.Xc @ p['beta']
    gym_term = p['sigma_gym'] * p['gym_raw']
    c = user_term[mm.obs_u] + gym_term[mm.obs_g]
    log_rate = (p['log_lambda0'] + p['kappa'] * mm.n_visits
                + p['rho'] * mm.r_obs)
    nu = np.exp(-log_rate)

    s = mm.single_obs[:400]
    closed = exgaussian_logpdf(-mm.m[s], -c[s], np.sqrt(sl ** 2 + su ** 2), nu[s])

    # Same quantity by dense quadrature over the offset, deliberately using a
    # different rule (fine trapezoid, +/-12 sd) so a bug in the Gauss-Hermite
    # nodes cannot cancel itself out.
    grid = np.linspace(-12 * su, 12 * su, 4001)
    w = np.exp(-0.5 * (grid / su) ** 2) / (su * np.sqrt(2 * np.pi))
    dens = np.zeros(len(s))
    for g, wt in zip(grid, w):
        dens += wt * np.exp(exgaussian_logpdf(-mm.m[s], -(c[s] + g), sl, nu[s]))
    numeric = np.log(dens * (grid[1] - grid[0]))

    err = np.abs(closed - numeric).max()
    print(f'1. single-observation closed form vs dense numerical integration')
    print(f'   max abs difference in log density over {len(s)} rows: {err:.3e}')
    assert err < 1e-6, f'closed form disagrees with the integral it claims to be ({err:.2e})'
    print('   PASS\n')


def check_quadrature_converged(rng):
    """Doubling the nodes must not move the answer."""
    print('2. Gauss-Hermite convergence for multi-observation climbers')
    base = load(n_quad=31)
    theta = base.initial_point(rng)
    ref = None
    for nq in (11, 21, 41, 81):
        mm = load(n_quad=nq)
        ll = mm.log_likelihood(theta)
        d = '' if ref is None else f'   change vs previous: {ll - ref:+.3e}'
        print(f'   {nq:>3} nodes: log-likelihood {ll:,.6f}{d}')
        ref = ll
    print('   PASS if the change is negligible by 31 nodes\n')


def check_against_full_model(mm, rng):
    """Marginal likelihood vs the full model with offsets integrated by hand.

    Takes the smallest multi-observation climbers and integrates their shared
    offset on a dense grid, independent of the quadrature machinery.
    """
    print('3. multi-observation climbers: the model vs dense integration')
    print('   (the model places its own nodes; the reference does not know how)')
    theta = mm.initial_point(rng)
    c, nu, sl, su = mm.pieces(theta)
    model_vals = mm.multi_log_integral(c, nu, sl, su)

    grid = np.linspace(-14 * su, 14 * su, 12001)
    w = np.exp(-0.5 * (grid / su) ** 2) / (su * np.sqrt(2 * np.pi))
    dg = grid[1] - grid[0]

    # The climbers with the most observations are where adaptive quadrature is
    # most needed and most likely to be wrong, so check those, not the median.
    counts = np.bincount(mm.multi_seg)
    check = np.argsort(-counts)[:25]
    worst, worst_k = 0.0, 0
    for seg in check:
        rows = mm.multi_obs[mm.multi_seg == seg]
        lp = np.array([exgaussian_logpdf(-mm.m[rows], -(c[rows] + g), sl,
                                         nu[rows]).sum() for g in grid])
        numeric = np.log(np.sum(np.exp(lp) * w) * dg)
        d = abs(model_vals[seg] - numeric)
        if d > worst:
            worst, worst_k = d, counts[seg]
    print(f'   climbers checked: the {len(check)} with the most observations '
          f'({counts[check].min()}-{counts[check].max()} rows each)')
    print(f'   max abs difference in log p(climber): {worst:.3e} '
          f'(at a climber with {worst_k} rows)')
    assert worst < 1e-4, f'quadrature disagrees with dense integration ({worst:.2e})'
    print('   PASS\n')


def report(mm):
    n_obs = len(mm.m)
    print('=== the marginalized model ===')
    print(f'dataset            {mm.label}')
    print(f'observations       {n_obs:,}')
    print(f'climbers           {mm.n_users:,}')
    print(f'  with 1 obs       {len(mm.single_obs):,} '
          f'({len(mm.single_obs)/n_obs:.1%} of rows) -- handled exactly')
    print(f'  with 2+ obs      {mm.n_multi_users:,} climbers, '
          f'{len(mm.multi_obs):,} rows -- handled by quadrature')
    print(f'gyms               {mm.n_gyms}')
    print(f'parameters sampled {mm.n_params}   (was {10357 + mm.n_params - 28} '
          'with per-climber offsets)')
    print(f'names              {", ".join(mm.param_names[:6])}, ... , '
          f'{", ".join(mm.param_names[-3:])}\n')


if __name__ == '__main__':
    rng = np.random.default_rng(11)
    mm = load()
    report(mm)
    check_single_obs_closed_form(mm, rng)
    check_quadrature_converged(rng)
    check_against_full_model(mm, rng)

    import time
    theta = mm.initial_point(rng)
    t0 = time.time()
    for _ in range(20):
        mm.log_posterior(theta)
    dt = (time.time() - t0) / 20
    print(f'timing: {dt*1000:.1f} ms per log-posterior evaluation')
    print(f'        -> {1/dt:,.0f} evaluations per second per core')
