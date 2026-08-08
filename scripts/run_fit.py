"""Run one grading-model-v2 fit and save summary + trace.

Reads base_bouldering.pkl and networks.json from --data-dir, and writes
idata_<name>.nc plus result_<name>.json to --out-dir. Both default to runs/,
so this works unchanged on a laptop and on a rented box.
"""
import argparse
import json
import pickle
import sys
import time
import warnings
from pathlib import Path

warnings.filterwarnings('ignore', category=FutureWarning)
import arviz as az
import numpy as np
import pymc as pm

from kaya.advancement import describe as advancement_describe
from kaya.convergence import assess_result, frozen_chains
from kaya.grading_model_v2 import build_model_v2, make_dataset

ROOT = Path(__file__).resolve().parents[1]

KEY_PARAMS = ['beta0', 'beta_gender', 'log_lambda0', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x',
              'sat_amp', 'sat_h0', 'sat_scale', 'delta1', 'delta2',
              'sigma_user', 'sigma_gym', 'sigma_link', 'lambda0', 'kappa', 'rho',
              'delta1_x', 'delta2_x', 'beta_h_missing', 'beta_a_missing', 'psi']


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--name', required=True)
    p.add_argument('--data-dir', default=None,
                   help='where base_bouldering.pkl and networks.json live')
    p.add_argument('--out-dir', default=None,
                   help='where the trace and result json are written')
    p.add_argument('--network', default='la6')
    p.add_argument('--name-filter', default='all')
    p.add_argument('--height-form', default='quadratic_x_gender')
    p.add_argument('--gender-mode', default='point')
    p.add_argument('--climb-quant', action='store_true')
    p.add_argument('--ape-x-gender', action='store_true')
    p.add_argument('--ape-linear', action='store_true',
                   help='drop the ape-index quadratic term (delta2)')
    p.add_argument('--fixed-sigma-link', action='store_true')
    p.add_argument('--zero-sum-users', action='store_true')
    p.add_argument('--marginalize-singles', action='store_true',
                   help='integrate out the offsets of climbers with one row')
    p.add_argument('--marginalize-all', action='store_true',
                   help='integrate out EVERY climber offset -- singles in '
                        'closed form, the rest by adaptive Gauss-Hermite '
                        'quadrature. Samples 40 parameters instead of 4,241 '
                        'and removes the sigma_user/epsilon funnel outright')
    p.add_argument('--n-quad', type=int, default=31,
                   help='quadrature nodes per climber for --marginalize-all')
    p.add_argument('--center-user-offsets', action='store_true',
                   help='sample epsilon ~ Normal(0, sigma_user) directly '
                        'instead of sigma_user * Normal(0,1). The data '
                        'dominates the prior 21-64x per climber, which is the '
                        'regime where centered samples better')
    p.add_argument('--n-at-max', action='store_true',
                   help='let the shortfall shrink with how many times the '
                        'climber repeated their hardest grade at that gym. '
                        'Direct evidence the ceiling is near it, and stronger '
                        'than raw visit count -- 100 visits with one send at '
                        'the top says something different from 100 visits '
                        'with twelve')
    p.add_argument('--advancement', action='store_true',
                   help='shift each ceiling by the grades that climber had '
                        'gained by the day of that send, relative to their own '
                        'other sends. Without it a 2022 send at one gym and a '
                        '2025 send at another are compared as if simultaneous, '
                        'which hands the newer gym the climber\'s own progress '
                        'as a softness correction. FIXED at the rate measured '
                        'within gyms -- fitted, it absorbs 3.4x its true value')
    p.add_argument('--orthogonal-design', action='store_true',
                   help='sample the covariate block on a Gram-Schmidt '
                        'orthogonalised basis; raw-basis coefficients are '
                        'still reported under their usual names')
    p.add_argument('--draws', type=int, default=1000)
    p.add_argument('--tune', type=int, default=1000)
    p.add_argument('--chains', type=int, default=4)
    p.add_argument('--target-accept', type=float, default=0.9)
    p.add_argument('--max-treedepth', type=int, default=10,
                   help='NUTS doubles its trajectory up to 2**this steps. The '
                        'v7 sweep saturated the default on 100%% of iterations, '
                        'so raising it is the direct fix for truncation -- at '
                        'up to 2x cost per level')
    p.add_argument('--seed', type=int, default=None,
                   help='PyMC random_seed. Left unset every fit draws a fresh '
                        'one, which makes runs irreproducible AND makes a '
                        'top-up indistinguishable from a repeat. Set it, '
                        'record it (it lands in the result JSON), and give a '
                        'top-up run a DIFFERENT one -- chains merged from two '
                        'runs that shared a seed are the same chains twice, '
                        'and R-hat computed over them would be a fiction.')
    args = p.parse_args()

    data_dir = Path(args.data_dir) if args.data_dir else ROOT / 'runs'
    out_dir = Path(args.out_dir) if args.out_dir else ROOT / 'runs'
    (out_dir / 'traces').mkdir(parents=True, exist_ok=True)
    (out_dir / 'results').mkdir(parents=True, exist_ok=True)

    with open(data_dir / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((data_dir / 'networks.json').read_text())['networks']

    ds = make_dataset(base, nets[args.network], name_filter=args.name_filter,
                      label=f'{args.network}/{args.name_filter}')
    print(f'[{args.name}] dataset: {ds.summary()}', flush=True)

    model = build_model_v2(
        ds,
        height_form=args.height_form,
        gender_mode=args.gender_mode,
        climb_quantization=args.climb_quant,
        ape_quadratic=not args.ape_linear,
        ape_x_gender=args.ape_x_gender,
        estimate_sigma_link=not args.fixed_sigma_link,
        zero_sum_users=args.zero_sum_users,
        marginalize_singles=args.marginalize_singles,
        center_user_offsets=args.center_user_offsets,
        marginalize_all=args.marginalize_all,
        n_quad=args.n_quad,
        orthogonal_design=args.orthogonal_design,
        advancement=args.advancement,
        use_n_at_max=args.n_at_max,
    )
    if args.advancement:
        print(f'[{args.name}] advancement offset: '
              f'{advancement_describe(ds.observations)}', flush=True)

    t0 = time.time()
    with model:
        # Keep the tuning draws. PyMC discards them by default, which is right
        # for reporting but throws away the most diagnostic part of the run --
        # you cannot see chains starting dispersed and pulling together, and it
        # is unrecoverable once the trace is written.
        idata = pm.sample(draws=args.draws, tune=args.tune, chains=args.chains,
                          target_accept=args.target_accept,
                          max_treedepth=args.max_treedepth,
                          progressbar=False,
                          discard_tuned_samples=False, random_seed=args.seed,
                          idata_kwargs={'log_likelihood': args.gender_mode == 'point'})
    # Stamp the seed into the trace itself. The result JSON carries it too, but
    # a trace outlives its result file in practice, and merge_chains.py has to
    # be able to refuse a merge of two runs that shared one.
    idata.posterior.attrs['kaya_seed'] = (
        'unset' if args.seed is None else str(args.seed))
    idata.posterior.attrs['kaya_fit_args'] = json.dumps(
        {k: v for k, v in vars(args).items()
         if k not in ('name', 'out_dir', 'data_dir', 'seed')}, sort_keys=True)
    elapsed = time.time() - t0

    # Save the trace FIRST. Summarising can fail (it has, on an arviz/numba
    # mismatch) and sampling is far too expensive to lose to a reporting bug.
    idata.to_netcdf(str(out_dir / 'traces' / f'idata_{args.name}.nc'))
    print(f'[{args.name}] trace saved ({elapsed/60:.1f} min sampling)', flush=True)

    present = [v for v in KEY_PARAMS if v in idata.posterior]
    summ = az.summary(idata, var_names=present, hdi_prob=0.89)
    ndiv = int(idata.sample_stats['diverging'].sum())
    print(f'\n[{args.name}] sampled in {elapsed/60:.1f} min | divergences {ndiv}'
          f'/{args.chains*args.draws}', flush=True)
    print(summ.to_string(), flush=True)

    gc = az.summary(idata, var_names=['gym_correction_c'], hdi_prob=0.89)
    print(f'\n[{args.name}] gym corrections (mean-centered):', flush=True)
    print(gc.to_string(), flush=True)

    # Recorded before anything is judged. R-hat and ESS are BETWEEN-chain
    # statistics, so a single chain that never moved makes a healthy model
    # report as a catastrophic failure -- and identically so for models that
    # share no parameters, which is how this was caught.
    frozen = frozen_chains(idata)
    if frozen:
        print(f'\n[{args.name}] WARNING: chain(s) {frozen} never moved '
              f'(step size adapted to zero). The R-hat and ESS below describe '
              f'that dead chain, not the model. Re-run at a different seed.',
              flush=True)

    res = {'name': args.name, 'args': vars(args), 'dataset': ds.summary(),
           'elapsed_min': elapsed / 60, 'divergences': ndiv,
           'frozen_chains': frozen,
           'max_rhat': float(summ['r_hat'].max()), 'min_ess': float(summ['ess_bulk'].min()),
           'params': summ.to_dict('index')}

    # How often NUTS ran out of trajectory instead of finishing one. The v7
    # sweep hit the ceiling on 100% of iterations, which is what made every
    # draw correlated with the last -- and it is invisible in R-hat and ESS,
    # so it has to be recorded separately or the next run rediscovers it.
    if 'tree_depth' in idata.sample_stats:
        td = np.asarray(idata.sample_stats['tree_depth'].values)
        ss = np.asarray(idata.sample_stats['step_size'].values)
        res['tree_depth'] = {
            'mean': float(td.mean()), 'max': int(td.max()),
            'frac_at_limit': float((td >= args.max_treedepth).mean()),
            'limit': args.max_treedepth,
            'step_size': float(ss.mean()),
        }
        print(f"[{args.name}] tree depth {td.mean():.2f} mean, "
              f"{100 * (td >= args.max_treedepth).mean():.0f}% at the limit of "
              f"{args.max_treedepth}, step size {ss.mean():.5f}", flush=True)

    # Under --orthogonal-design the names above are Deterministic transforms of
    # what the sampler actually moved, which keeps max_rhat comparable to every
    # raw-basis fit on disk. Record the sampled basis alongside it: the two
    # together are the whole result, since the point of the change is that the
    # orthogonal directions mix even where their raw-basis combination does not.
    if args.orthogonal_design:
        oh = [f'{v}_orth' for v in KEY_PARAMS if f'{v}_orth' in idata.posterior]
        osum = az.summary(idata, var_names=oh, hdi_prob=0.89)
        res['orth'] = {'max_rhat': float(osum['r_hat'].max()),
                       'min_ess': float(osum['ess_bulk'].min()),
                       'params': osum.to_dict('index')}
        print(f'\n[{args.name}] sampled (orthogonal) basis:', flush=True)
        print(osum.to_string(), flush=True)

    # Convergence is a gate, not a report. The trace is still written above --
    # sampling is far too expensive to throw away, and a failed fit is itself a
    # finding worth showing -- but the verdict travels with the result so no
    # downstream builder has to re-derive it, and so nothing can quote these
    # numbers as a measurement without seeing that they are not one.
    verdict = assess_result(res)
    res['convergence'] = verdict.as_dict()
    if not verdict.converged:
        print(f'\n[{args.name}] *** {verdict.describe()} ***', file=sys.stderr, flush=True)
        print(f'[{args.name}] {verdict.describe()}', flush=True)
    if args.gender_mode == 'point':
        try:
            if args.marginalize_all:
                # Under --marginalize-all the multi-climber term is a
                # pm.Potential, and PyMC emits no log_likelihood group for a
                # Potential -- so arviz sees only m_single and az.loo either
                # fails or, worse, silently scores a sixth of the dataset.
                #
                # The terms are not lost: build_model_v2 records them as the
                # Deterministic `log_lik_multi`, one entry per multi-climber.
                # Reassembling them gives a complete pointwise log-likelihood.
                #
                # The UNIT changes, and that is not a detail. Integrating a
                # climber's offset out makes their rows conditionally dependent,
                # so leave-one-OBSERVATION-out no longer exists in closed form;
                # the natural unit becomes leave-one-CLIMBER-out. Every climber
                # contributes exactly one term -- singles through m_single,
                # multis through log_lik_multi -- so the elpd here is over
                # climbers, NOT over observations.
                #
                # Consequence: this elpd is comparable ACROSS the marginalized
                # sweep and NOT comparable to any v7 number, which was per
                # observation over a larger, differently-sized set of units.
                import xarray as xr
                single = idata.log_likelihood['m_single']
                sdim = [d for d in single.dims if d not in ('chain', 'draw')][0]
                multi = idata.posterior['log_lik_multi']
                mdim = [d for d in multi.dims if d not in ('chain', 'draw')][0]
                merged = xr.concat(
                    [single.rename({sdim: 'climber'}),
                     multi.rename({mdim: 'climber'})], dim='climber')
                merged = merged.assign_coords(climber=np.arange(merged.sizes['climber']))
                idata.log_likelihood = xr.Dataset({'climber_obs': merged})
                res['loo_unit'] = 'climber'
                print(f'[{args.name}] LOO unit is the climber '
                      f'({single.sizes[sdim]} single + {multi.sizes[mdim]} multi '
                      f'= {merged.sizes["climber"]}); not comparable to a v7 elpd',
                      flush=True)
            elif args.marginalize_singles:
                # The marginalized model has TWO observed variables --
                # m_single (offset integrated out) and m_multi (offset still
                # sampled) -- so arviz sees two log-likelihood groups and has
                # no way to know they are one dataset. Stitch them back into a
                # single 'obs' dimension, in the original row order, so the
                # resulting elpd is directly comparable to the unmarginalized
                # model's.
                import xarray as xr
                ll = idata.log_likelihood
                parts, idx = [], []
                for nm, dim in (('m_single', 'obs_single'), ('m_multi', 'obs_multi')):
                    da = ll[nm]
                    idx.append(da[dim].values)
                    parts.append(da.rename({dim: 'obs'}).assign_coords(
                        obs=np.arange(da.sizes[dim])))
                order = np.argsort(np.concatenate(idx))
                merged = xr.concat(parts, dim='obs')
                merged = merged.assign_coords(obs=np.arange(merged.sizes['obs']))
                merged = merged.isel(obs=order)
                idata.log_likelihood = xr.Dataset({'m_obs': merged})
            loo = az.loo(idata, pointwise=False)
            res['loo'] = {'elpd_loo': float(loo.elpd_loo), 'se': float(loo.se), 'p_loo': float(loo.p_loo)}
            print(f"\n[{args.name}] LOO elpd = {loo.elpd_loo:.1f} +/- {loo.se:.1f}", flush=True)
        except Exception as e:
            print(f'[{args.name}] LOO failed: {e}', flush=True)

    (out_dir / 'results' / f'result_{args.name}.json').write_text(
        json.dumps(res, indent=2, default=float))
    print(f'[{args.name}] done.', flush=True)


if __name__ == '__main__':
    main()
