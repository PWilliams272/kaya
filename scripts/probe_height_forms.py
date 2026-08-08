"""Which height form does the data actually want? Answered without a sampler.

Motivation. The v10 sweep scores seven height forms by PSIS-LOO (Pareto-smoothed
importance sampling leave-one-out cross-validation), at three to six hours each.
The first two to land -- `linear` and `linear_x_gender` -- came back 0.25 elpd
apart against a paired standard error of 0.63, i.e. indistinguishable. It would
be easy to read that as "height form does not matter." It does not show that.
Both of those forms are straight lines; a comparison between two straight lines
is silent about curvature, and an interaction with a slope is not an interaction
with a bend. This probe asks the questions that pair cannot.

    elpd = expected log pointwise predictive density. HIGHER IS BETTER.
    This script scores held-out squared error instead. LOWER IS BETTER.

Why height needs its own machinery. Everywhere else in this repo, gym effects
are measured with paired differences: for a climber logging at two gyms,
subtracting their two levels cancels the climber completely. That identity is
BLIND to height by construction -- within a climber, height is constant, so it
subtracts away along with everything else about them. Height is identified only
BETWEEN climbers, so it needs a different route:

  1. Two-way least squares on the send levels, m[u,g] = ability[u] + corr[g],
     with the gym corrections constrained to sum to zero. This is the same
     decomposition the Bayesian model performs, minus the priors, the partial
     pooling and the ex-Gaussian tail. `corr` is pinned by the 4,201 climbers
     who log at two or more gyms; `ability` is then every climber's level with
     their own gyms' grading removed.

  2. Regress that ability on height under each candidate form, scored
     out-of-sample by 5-fold cross-validation over CLIMBERS -- the same unit
     the marginalized fits are scored on.

Two disciplines this script insists on, both learned the hard way here:

  PAIRED errors. Two forms score the SAME held-out climbers, so
  climber-to-climber variation cancels and only the disagreement between the
  forms survives. Quoting the raw spread of either form's own errors instead
  inflates the error bar by orders of magnitude and hides every real result.

  A measured noise floor. The fold assignment is random. A verdict that moves
  when only the fold seed changes is a property of the shuffle, not the data --
  exactly how the v7 sweep failed, where the gap between models was smaller
  than the gap between seeds of the same model. Every comparison is therefore
  repeated over N_SEEDS independent fold assignments and reported as a range.

Result as of 2026-08-07: `quadratic_x_gender` wins on 25 of 25 seeds. Height
plainly belongs (+3.6 sigma over dropping it). The gender interaction is real
only once there is a bend for it to act on -- +2.2 sigma on the quadratic,
+0.2 sigma on the linear -- because the curvature has OPPOSITE SIGN by gender
and therefore nearly cancels in any form that makes the two share it.

    python scripts/probe_height_forms.py

Run from the repo root: src/kaya/secrets.py shadows the stdlib module numpy's
bit generator imports, so running from inside src/kaya breaks numpy.
"""
import collections
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.linalg import lsqr

from kaya.grading_model_v2 import make_dataset

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'

NETWORK = 'net50'
NAME_FILTER = 'confident'
N_FOLDS = 5
N_SEEDS = 25          # fold assignments, for the noise floor
BASE_SEED = 1000

# The five specifications _design_columns builds, named identically. `zero` is
# load-bearing: without a no-height baseline every comparison is between two
# models that already assume height belongs.
FORMS = ['zero', 'linear', 'linear_x_gender', 'quadratic', 'quadratic_x_gender']

PAIRS = [
    ('zero', 'linear', 'does height belong at all'),
    ('linear', 'quadratic', 'does height CURVE'),
    ('linear', 'linear_x_gender', 'gender x slope'),
    ('quadratic', 'quadratic_x_gender', 'gender x CURVE'),
    ('zero', 'quadratic_x_gender', 'full height model vs none'),
]


def load():
    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets[NETWORK], name_filter=NAME_FILTER, label='probe')
    return ds.observations, ds.users


def two_way(obs):
    """Split m into a per-climber and a per-gym part by least squares.

    Sum-to-zero on the corrections is imposed as an extra heavily weighted row
    rather than by dropping one gym. Dropping a gym makes every correction a
    contrast against whichever gym happened to be dropped -- the same fit, but
    a parameterisation whose numbers mean something different, and one that
    would not line up with the model's ZeroSumNormal.
    """
    users = obs.user_id.astype('category')
    gyms = obs.gym_id.astype('category')
    nu, ng = len(users.cat.categories), len(gyms.cat.categories)
    n = len(obs)

    rows = np.repeat(np.arange(n), 2)
    cols = np.column_stack([users.cat.codes.values,
                            nu + gyms.cat.codes.values]).ravel()
    A = sparse.csr_matrix((np.ones(2 * n), (rows, cols)), shape=(n, nu + ng))
    con = sparse.csr_matrix((np.full(ng, 100.0), (np.zeros(ng), nu + np.arange(ng))),
                            shape=(1, nu + ng))
    A = sparse.vstack([A, con]).tocsr()
    b = np.concatenate([obs.m.values.astype(float), [0.0]])

    sol = lsqr(A, b, atol=1e-10, btol=1e-10, iter_lim=20000)[0]
    return (pd.Series(sol[:nu], index=users.cat.categories),
            pd.Series(sol[nu:], index=gyms.cat.categories))


def design(form, h, a, g, a_missing):
    """Covariate matrix for one height form, mirroring _design_columns.

    Ape index enters linearly in every form and carries a missing-data flag, so
    a climber who reported height but not reach still contributes to the height
    question instead of being dropped over an unrelated variable.
    """
    cols = [np.ones_like(h), g]
    if form == 'linear':
        cols += [h]
    elif form == 'linear_x_gender':
        cols += [h, g * h]
    elif form == 'quadratic':
        cols += [h, h ** 2]
    elif form == 'quadratic_x_gender':
        cols += [h, h ** 2, g * h, g * h ** 2]
    cols += [a, a_missing]
    return np.column_stack(cols)


def cv_errors(X_by_form, y, w, fold):
    """Held-out squared error per climber for each form. Lower is better."""
    out = {}
    for f, X in X_by_form.items():
        e = np.full(len(y), np.nan)
        for k in range(N_FOLDS):
            tr, te = fold != k, fold == k
            sw = np.sqrt(w[tr])
            beta = np.linalg.lstsq(X[tr] * sw[:, None], y[tr] * sw, rcond=None)[0]
            e[te] = (y[te] - X[te] @ beta) ** 2
        out[f] = e
    return out


def sigmas(errors, lo, hi, n):
    """Paired advantage of `hi` over `lo`, in standard errors. Positive favours hi."""
    d = errors[lo] - errors[hi]
    se = d.std(ddof=1) / np.sqrt(n)
    return (d.mean() / se) if se > 0 else 0.0


def main():
    obs, users = load()
    ability, corr = two_way(obs)

    u = users.loc[ability.index].copy()
    u['ability'] = ability.values
    u['n_obs'] = obs.groupby('user_id').size().reindex(ability.index).values

    d = u[u.height.notna() & u.w_female.notna()].copy()
    a_missing = d.ape_index.isna().values.astype(float)
    ape = d.ape_index.fillna(d.ape_index.median()).values

    h = d.height.values - d.height.median()      # inches from the median
    a = ape - np.median(ape)
    g = d.w_female.values                        # a probability, not a flag
    y = d.ability.values
    w = d.n_obs.values.astype(float)             # ability precision scales with n
    n = len(y)

    n_multi = int((obs.groupby('user_id').gym_id.nunique() >= 2).sum())
    print(f'network {NETWORK}, names {NAME_FILTER}')
    print(f'{len(u):,} climbers; {n:,} reported a height and enter the regression')
    print(f'gym corrections from {n_multi:,} multi-gym climbers across '
          f'{len(corr)} gyms, spread {corr.std():.3f} grades')
    print(f'{int(a_missing.sum()):,} of those {n:,} lacked an ape index and are '
          'carried with a missing flag\n')

    X_by_form = {f: design(f, h, a, g, a_missing) for f in FORMS}

    # ---- headline table on one reference seed
    ref = cv_errors(X_by_form, y, w, np.random.default_rng(BASE_SEED)
                    .permutation(n) % N_FOLDS)
    order = sorted(FORMS, key=lambda f: ref[f].mean())
    best = order[0]
    print(f'Held-out squared error per climber, {N_FOLDS}-fold CV. LOWER IS BETTER.')
    print('Differences are paired against the best form.\n')
    print(f'  {"form":22s} {"MSE":>9s} {"vs best":>10s} {"sigma":>7s}')
    for f in order:
        if f == best:
            print(f'  {f:22s} {ref[f].mean():9.4f} {"best":>10s}')
        else:
            dv = ref[f] - ref[best]
            se = dv.std(ddof=1) / np.sqrt(n)
            print(f'  {f:22s} {ref[f].mean():9.4f} {dv.mean():+10.4f} '
                  f'{abs(dv.mean()) / se:7.2f}')

    # ---- the noise floor: does any of that survive a reshuffle?
    sig = {(lo, hi): [] for lo, hi, _ in PAIRS}
    winners = []
    for s in range(N_SEEDS):
        fold = np.random.default_rng(BASE_SEED + s).permutation(n) % N_FOLDS
        e = cv_errors(X_by_form, y, w, fold)
        winners.append(min(FORMS, key=lambda f: e[f].mean()))
        for lo, hi, _ in PAIRS:
            sig[(lo, hi)].append(sigmas(e, lo, hi, n))

    print(f'\nEvery comparison repeated over {N_SEEDS} independent fold seeds.')
    print('Positive sigma favours the SECOND form. Two sigma is the bar.\n')
    print(f'  {"question":32s} {"median":>8s} {"min":>7s} {"max":>7s} {"clears 2":>10s}')
    for lo, hi, label in PAIRS:
        v = np.array(sig[(lo, hi)])
        print(f'  {label:32s} {np.median(v):+8.2f} {v.min():+7.2f} {v.max():+7.2f} '
              f'{int((v > 2).sum()):>7d}/{N_SEEDS}')

    print('\n  best form by seed:')
    for f, c in collections.Counter(winners).most_common():
        print(f'    {f:22s} {c:2d}/{N_SEEDS}')

    # ---- what the winning curve actually claims, in grades
    X = X_by_form['quadratic_x_gender']
    sw = np.sqrt(w)
    beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
    g1, g2, g1x, g2x = beta[2], beta[3], beta[4], beta[5]
    print(f'\nThe fitted curve, height in inches from the median '
          f'({d.height.median():.0f}in)\n')
    grid = np.linspace(-8, 8, 33)
    for label, gv in [('male-coded', 0.0), ('female-coded', 1.0)]:
        s1, s2 = g1 + gv * g1x, g2 + gv * g2x
        span = (s1 * grid + s2 * grid ** 2).ptp()
        shape = 'concave (an optimum)' if s2 < 0 else 'convex (a trough)'
        print(f'  {label:13s} slope {s1:+.4f}/in   curvature {s2:+.5f}/in^2'
              f'   {shape}')
        print(f'  {"":13s} spans {span:.2f} grades across +/-8in of height')
    print('\n  The two curvatures have opposite signs, which is the whole '
          'finding: any\n  form that makes the genders share one bend averages '
          'them toward zero and\n  measures nothing, and a form with no bend at '
          'all leaves the interaction\n  nothing to act on.')


if __name__ == '__main__':
    main()
