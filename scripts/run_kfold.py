"""Grouped k-fold: hold out whole CLIMBERS, refit, predict all their rows.

Leave-one-out cross-validation asks "given everything else about this climber,
how well is one more of their sends predicted?" For a model whose entire job is
separating a climber's ability from a gym's grading standard, that is the easy
question -- and for the 59% of climbers with a single send it is barely a
question at all, since the row being predicted is the only thing pinning that
climber's ability down.

Holding out whole climbers asks the honest version: **can this model predict
somebody it has never seen?** A held-out row must be scored from the
population-level parameters and the gym corrections alone, because that
climber's own offset does not exist -- which is exactly the model's situation
whenever it makes a claim about a gym.

Three things have to be right or the answer is quietly flattering:

* **Split climbers, not rows.** Splitting rows would leave a climber's other
  sends in the training set, identifying the very offset being withheld.
* **Integrate the unknown offset out**, using the same closed form the
  marginalized model uses for its single-send climbers. Substituting zero would
  score a climber of exactly average ability instead of an unknown one.
* **Scale held-out climbers with the TRAINING set's constants.** Heights and
  visit counts are centred and scaled by medians and standard deviations; if
  the held-out climbers set their own, the test set has leaked into the fit
  through the back door with nothing raising an error.

Cost: one refit per fold per model -- five folds across seven height forms is
35 fits, which is why this goes through run_batch.py rather than by hand.

Writes runs/results/kfold_<name>_fold<i>.json, one per fit, carrying the
per-row scores for that fold's held-out climbers.
"""
import argparse
import dataclasses
import json
import pickle
import time
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SIGMA_LINK_FIXED = 0.5


def fold_of_each_row(user_ids_per_row, n_folds, seed=0):
    """Assign each CLIMBER to a fold, then carry that down to their rows."""
    users = np.unique(user_ids_per_row)
    rng = np.random.default_rng(seed)
    fold = rng.permutation(len(users)) % n_folds
    lookup = dict(zip(users, fold))
    return np.array([lookup[u] for u in user_ids_per_row])


def subset_to_users(ds, keep_users):
    """A DatasetV2 restricted to a set of climbers, rows and all."""
    keep = set(keep_users)
    obs = ds.observations[ds.observations['user_id'].isin(keep)].reset_index(drop=True)
    users = ds.users.loc[ds.users.index.isin(keep)]
    climbs = ds.climbs[ds.climbs['climb_id'].isin(obs['climb_id'].unique())]
    return dataclasses.replace(ds, observations=obs, users=users,
                               climbs=climbs.reset_index(drop=True))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--name', required=True, help='label for this model')
    p.add_argument('--height-form', default='linear')
    p.add_argument('--fold', type=int, required=True)
    p.add_argument('--n-folds', type=int, default=5)
    p.add_argument('--seed', type=int, default=0)
    p.add_argument('--network', default='net50')
    p.add_argument('--name-filter', default='confident')
    p.add_argument('--draws', type=int, default=500)
    p.add_argument('--tune', type=int, default=600)
    p.add_argument('--chains', type=int, default=4)
    p.add_argument('--target-accept', type=float, default=0.9)
    p.add_argument('--data-dir', default=None)
    p.add_argument('--out-dir', default=None)
    args = p.parse_args()

    import arviz as az
    import pymc as pm
    from kaya.grading_model_v2 import make_dataset, build_model_v2
    from kaya.marginal_v2 import prepare_design

    data_dir = Path(args.data_dir) if args.data_dir else ROOT / 'runs'
    out_dir = Path(args.out_dir) if args.out_dir else ROOT / 'runs'
    (out_dir / 'results').mkdir(parents=True, exist_ok=True)

    with open(data_dir / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((data_dir / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets[args.network], name_filter=args.name_filter,
                      label=f'{args.network}/{args.name_filter}')

    uid = ds.observations['user_id'].to_numpy()
    folds = fold_of_each_row(uid, args.n_folds, args.seed)
    test_rows = folds == args.fold
    test_users = np.unique(uid[test_rows])
    train_users = np.setdiff1d(np.unique(uid), test_users)
    ds_train = subset_to_users(ds, train_users)
    print(f'[{args.name} f{args.fold}] train {len(ds_train.observations):,} rows / '
          f'{len(train_users):,} climbers | held out {test_rows.sum():,} rows / '
          f'{len(test_users):,} climbers', flush=True)

    model = build_model_v2(ds_train, height_form=args.height_form,
                           estimate_sigma_link=False, marginalize_singles=True)
    t0 = time.time()
    with model:
        idata = pm.sample(draws=args.draws, tune=args.tune, chains=args.chains,
                          target_accept=args.target_accept, progressbar=False,
                          idata_kwargs={'log_likelihood': False})
    elapsed = time.time() - t0
    summ = az.summary(idata, hdi_prob=0.89)
    max_rhat = float(summ['r_hat'].max())
    print(f'[{args.name} f{args.fold}] sampled in {elapsed/60:.1f} min, '
          f'max R-hat {max_rhat:.3f}', flush=True)

    scores, rows, dropped = score_heldout(
        idata, ds, ds_train, test_rows, args.height_form, prepare_design)

    res = {
        'name': args.name, 'fold': args.fold, 'n_folds': args.n_folds,
        'seed': args.seed, 'height_form': args.height_form,
        'n_test_rows': int(test_rows.sum()), 'n_test_users': int(len(test_users)),
        'n_scored': int(len(scores)), 'n_dropped_unseen_gym': int(dropped),
        'elapsed_min': elapsed / 60, 'max_rhat': max_rhat,
        'min_ess': float(summ['ess_bulk'].min()),
        'divergences': int(idata.sample_stats['diverging'].sum()),
        'elpd': float(scores.sum()),
        'row_index': [int(i) for i in rows],
        'elpd_i': [round(float(v), 6) for v in scores],
    }
    out = out_dir / 'results' / f'kfold_{args.name}_fold{args.fold}.json'
    out.write_text(json.dumps(res, default=float))
    print(f'[{args.name} f{args.fold}] elpd {scores.sum():.1f} over '
          f'{len(scores):,} rows{f" ({dropped} dropped)" if dropped else ""} '
          f'-> {out.name}', flush=True)


def score_heldout(idata, ds_full, ds_train, test_rows, height_form, prepare_design):
    """Log predictive density for held-out rows, unknown offsets integrated out.

    Averaged over posterior draws in the probability domain (logsumexp), not
    the log domain: the quantity wanted is log E[p], and E[log p] is a
    different and systematically smaller number.
    """
    from scipy.special import logsumexp
    from kaya.marginal_v2 import exgaussian_logpdf

    # The training fit's own scaling constants, reused for the held-out rows.
    tr = prepare_design(ds_train, height_form=height_form)
    full = prepare_design(ds_full, height_form=height_form,
                          consts=tr['consts'])

    # Gyms are indexed by the TRAINING gym list, because that is the order the
    # fitted corrections are in. A held-out row at a gym no training climber
    # visited has no correction to apply and is unscoreable -- dropped and
    # counted, not silently given the average.
    gym_pos = {g: i for i, g in enumerate(tr['gym_ids'])}
    gym_full = ds_full.observations['gym_id'].to_numpy()
    rows = np.flatnonzero(test_rows)
    keep = np.array([gym_full[i] in gym_pos for i in rows])
    dropped = int((~keep).sum())
    rows = rows[keep]
    gym_i = np.array([gym_pos[gym_full[i]] for i in rows])

    post = idata.posterior.stack(s=('chain', 'draw'))
    n_draws = post.sizes['s']
    val = lambda n: np.asarray(post[n].values)

    # Covariate row per observation, in the training design's column order.
    Xc_obs = full['Xc'][full['obs_u'][rows]]
    coef = np.stack([val(n) for n in tr['Xnames']])        # (n_cov, draws)
    beta0 = val('beta0')
    sigma_user = val('sigma_user')
    gymc = np.asarray(post['gym_correction_c'].values)     # (n_gyms, draws)
    log_lambda0, kappa, rho = val('log_lambda0'), val('kappa'), val('rho')
    m = full['m'][rows]
    nv = full['n_visits'][rows]
    r = full['r_obs'][rows]

    lp = np.empty((len(rows), n_draws))
    for d in range(n_draws):
        ceiling = beta0[d] + Xc_obs @ coef[:, d] + gymc[gym_i, d]
        nu = np.exp(-(log_lambda0[d] + kappa[d] * nv + rho[d] * r))
        sd = np.sqrt(SIGMA_LINK_FIXED ** 2 + sigma_user[d] ** 2)
        lp[:, d] = exgaussian_logpdf(-m, -ceiling, sd, nu)
    return logsumexp(lp, axis=1) - np.log(n_draws), rows, dropped


if __name__ == '__main__':
    main()
