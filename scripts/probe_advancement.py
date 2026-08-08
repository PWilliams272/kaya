"""What the fixed advancement offset does to the gym corrections, model-free.

Why not just fit it
-------------------
The full model takes ~4 hours per configuration, which is a slow way to answer
"does this change anything". The gym corrections are recoverable far faster
without any model at all: least squares on

    m[u, g] = ability[u] + correction[g]

is the same two-way decomposition the Bayesian model performs, minus the priors
and the ex-Gaussian shortfall term. It gets the corrections to within a few
hundredths of a grade of the fitted ones and runs in seconds, so it is the
right instrument for a before/after. It is NOT a replacement for the fit --
it has no uncertainties and no shortfall model.

What is measured
----------------
Solve twice: once on the raw hardest grades, once on grades with the known
advancement offset removed. The difference in the recovered corrections is
exactly the part of each gym's apparent stiffness that was really its
climbers' own progress.

The permutation null
--------------------
A shrinking correction spread proves nothing on its own -- subtracting *any*
climber-centred vector of the same size removes some variance. So the same
measurement is repeated with each climber's offsets randomly reassigned among
their own gyms. That preserves every magnitude and preserves the within-climber
centring, and destroys only the link between a gym and WHEN that climber was
there. If the real assignment does not shrink the spread further than the
shuffled ones do, the correction is removing noise rather than the confound.

    python scripts/probe_advancement.py                    # net50, 200 shuffles
    python scripts/probe_advancement.py --network net20 --n-perm 500

Run from the repo root.
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.linalg import lsqr

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'

# The sum-to-zero row's weight. The decomposition is identified only up to an
# additive shift between ability and correction, so a constraint row is
# required; 100 makes it bind hard without making the system ill-conditioned.
ZEROSUM_WEIGHT = 100.0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--network', default='net50')
    p.add_argument('--name-filter', default='confident',
                   choices=['all', 'confident'])
    p.add_argument('--n-perm', type=int, default=200,
                   help='shuffles for the null; 0 skips it')
    p.add_argument('--seed', type=int, default=20260807)
    p.add_argument('--top', type=int, default=12,
                   help='how many gyms to list, largest shift first')
    return p.parse_args(argv)


def two_way(obs: pd.DataFrame, y: np.ndarray, uidx, gidx, n_users, n_gyms):
    """Least-squares corrections from m = ability[u] + correction[g]."""
    n = len(obs)
    r = np.arange(n)
    rows = np.concatenate([r, r, np.full(n_gyms, n)])
    cols = np.concatenate([uidx, n_users + gidx, n_users + np.arange(n_gyms)])
    data = np.concatenate([np.ones(2 * n), np.full(n_gyms, ZEROSUM_WEIGHT)])
    A = coo_matrix((data, (rows, cols)),
                   shape=(n + 1, n_users + n_gyms)).tocsr()
    sol = lsqr(A, np.append(y, 0.0), atol=1e-12, btol=1e-12, iter_lim=8000)[0]
    corr = sol[n_users:]
    return corr - corr.mean()


def shuffle_within_climber(off: np.ndarray, uidx: np.ndarray,
                           rng: np.random.Generator) -> np.ndarray:
    """Permute each climber's offsets among their own rows.

    Every magnitude survives and so does the within-climber sum, so the null
    differs from the real thing in exactly one respect: which gym each shift
    lands on.
    """
    out = off.copy()
    order = np.argsort(uidx, kind='stable')
    grouped = off[order]
    bounds = np.flatnonzero(np.diff(uidx[order])) + 1
    for chunk in np.split(np.arange(len(order)), bounds):
        if len(chunk) > 1:
            grouped[chunk] = rng.permutation(grouped[chunk])
    out[order] = grouped
    return out


def main(argv=None) -> int:
    args = parse_args(argv)
    sys.path.insert(0, str(ROOT / 'src'))
    from kaya.advancement import advancement_offset, describe
    from kaya.grading_model_v2 import make_dataset

    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets[args.network], name_filter=args.name_filter,
                      label=f'{args.network}/{args.name_filter}')
    obs = ds.observations

    names = {}
    cfg = ROOT / 'src' / 'kaya' / 'config' / 'gyms_available.json'
    if cfg.exists():
        raw = json.loads(cfg.read_text())
        for g in (raw if isinstance(raw, list) else raw.get('gyms', [])):
            gid = g.get('gym_id') or g.get('id')
            if gid is not None:
                names[str(gid)] = g.get('gym_name') or g.get('name') or str(gid)

    gyms = sorted(obs['gym_id'].unique())
    users = sorted(obs['user_id'].unique())
    gidx = obs['gym_id'].map({g: i for i, g in enumerate(gyms)}).to_numpy()
    uidx = obs['user_id'].map({u: i for i, u in enumerate(users)}).to_numpy()
    n_users, n_gyms = len(users), len(gyms)

    m = obs['m'].to_numpy(float)
    off = advancement_offset(obs)
    d = describe(obs)

    print(f'{args.network}/{args.name_filter}: {len(obs):,} observations, '
          f'{n_users:,} climbers, {n_gyms} gyms')
    print(f'offset applied to {d["n_corrected"]:,} rows '
          f'({100 * d["n_corrected"] / d["n_obs"]:.0f}%), '
          f'mean |shift| {d["mean_abs"]:.3f} grades, max {d["max_abs"]:.3f}')
    print(f'rate curve: r(v) = {d["intercept"]:.3f} {d["slope"]:+.3f} v '
          '  grades per year\n')

    base_kw = dict(uidx=uidx, gidx=gidx, n_users=n_users, n_gyms=n_gyms)
    c0 = two_way(obs, m, **base_kw)
    # Subtract, not add: the offset raises the ceiling the model predicts, so
    # removing it from the observed grade is the equivalent left-hand-side move.
    c1 = two_way(obs, m - off, **base_kw)
    shift = c1 - c0

    print(f'correction spread (SD)   before {c0.std():.4f}   '
          f'after {c1.std():.4f}   ({100 * (c1.std() / c0.std() - 1):+.1f}%)')
    print(f'shift in corrections     SD {shift.std():.4f}   '
          f'max |shift| {np.abs(shift).max():.4f}')
    print(f'correlation before/after {np.corrcoef(c0, c1)[0, 1]:.4f}')
    rank0, rank1 = np.argsort(np.argsort(c0)), np.argsort(np.argsort(c1))
    print(f'gyms changing rank       {int((rank0 != rank1).sum())} of {n_gyms}')

    if args.n_perm:
        rng = np.random.default_rng(args.seed)
        null = np.array([two_way(obs, m - shuffle_within_climber(off, uidx, rng),
                                 **base_kw).std()
                         for _ in range(args.n_perm)])
        # One-sided: the claim is that the real timing shrinks the spread MORE
        # than a random reassignment of the same shifts.
        p = float((null <= c1.std()).mean())
        z = (null.mean() - c1.std()) / null.std() if null.std() else np.inf
        print(f'\npermutation null ({args.n_perm} shuffles, dates reassigned '
              'within each climber)')
        print(f'  shuffled spread   {null.mean():.4f} +/- {null.std():.4f}')
        print(f'  real spread       {c1.std():.4f}')
        print(f'  z                 {z:+.2f}   p = {p:.4f}')
        print('  ' + ('the timing carries real information -- the shrink is '
                      'not what any climber-centred vector would do'
                      if p < 0.05 else
                      'NOT distinguishable from shuffling the same shifts '
                      'around; treat the shrink as noise removal'))

    print(f'\n{"gym":<40}{"before":>9}{"after":>9}{"shift":>9}')
    for i in np.argsort(-np.abs(shift))[:args.top]:
        nm = names.get(str(gyms[i]), str(gyms[i]))[:38]
        print(f'{nm:<40}{c0[i]:+9.3f}{c1[i]:+9.3f}{shift[i]:+9.3f}')

    print('\nLeast squares, no priors and no shortfall term -- directional '
          'evidence for\nwhether the fit is worth running, not a substitute '
          'for it.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
