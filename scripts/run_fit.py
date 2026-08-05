"""Run one grading-model-v2 fit and save summary + trace.

Reads base_bouldering.pkl and networks.json from --data-dir, and writes
idata_<name>.nc plus result_<name>.json to --out-dir. Both default to runs/,
so this works unchanged on a laptop and on a rented box.
"""
import argparse, json, pickle, time, warnings
from pathlib import Path

warnings.filterwarnings('ignore', category=FutureWarning)
import numpy as np
import arviz as az
import pymc as pm

from kaya.grading_model_v2 import make_dataset, build_model_v2

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
    p.add_argument('--draws', type=int, default=1000)
    p.add_argument('--tune', type=int, default=1000)
    p.add_argument('--chains', type=int, default=4)
    p.add_argument('--target-accept', type=float, default=0.9)
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
    )

    t0 = time.time()
    with model:
        # Keep the tuning draws. PyMC discards them by default, which is right
        # for reporting but throws away the most diagnostic part of the run --
        # you cannot see chains starting dispersed and pulling together, and it
        # is unrecoverable once the trace is written.
        idata = pm.sample(draws=args.draws, tune=args.tune, chains=args.chains,
                          target_accept=args.target_accept, progressbar=False,
                          discard_tuned_samples=False,
                          idata_kwargs={'log_likelihood': args.gender_mode == 'point'})
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

    res = {'name': args.name, 'args': vars(args), 'dataset': ds.summary(),
           'elapsed_min': elapsed / 60, 'divergences': ndiv,
           'max_rhat': float(summ['r_hat'].max()), 'min_ess': float(summ['ess_bulk'].min()),
           'params': summ.to_dict('index')}
    if args.gender_mode == 'point':
        try:
            if args.marginalize_singles:
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
