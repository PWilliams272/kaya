"""Extract posterior (and prior) draws from every grading-model fit on disk.

Writes one JSON the viewer uses for: per-parameter prior-vs-posterior, chain
traces, posteriors overlaid across model versions, and corner plots.

Draws are thinned with the SAME step for every parameter within a fit, so
draw i of one parameter still corresponds to draw i of another -- that joint
structure is what makes the corner plots meaningful. Do not thin per-parameter.
"""
import argparse
import json
import os
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

from kaya.convergence import assess_result

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
# The job scratch directory is deleted when the job is; runs/ is the durable
# archive. Each input resolves to runs/ when it is there and falls back to
# scratch otherwise, so a batch still mid-copy keeps working.
_SCRATCH = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')


def _pick(*candidates):
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def data_file(fname):
    """base_bouldering.pkl, networks.json, csv inputs."""
    return _pick(RUNS / fname, _SCRATCH / fname)


def trace_file(name):
    return _pick(RUNS / 'traces' / f'idata_{name}.nc',
                 _SCRATCH / f'idata_{name}.nc')


def result_file(name):
    return _pick(RUNS / 'results' / f'result_{name}.json',
                 _SCRATCH / f'result_{name}.json')


def trace_names(*globs):
    """Fit names that have BOTH a trace and a result, from either location."""
    import fnmatch
    names = set()
    for d in (RUNS / 'traces', _SCRATCH):
        if not d.is_dir():
            continue
        for p in d.glob('idata_*.nc'):
            n = p.stem[len('idata_'):]
            if any(fnmatch.fnmatch(n, g) for g in globs):
                names.add(n)
    return sorted(n for n in names if result_file(n).exists())
OUT = Path('/Users/peterwilliams/projects/kaya/src/kaya/viewer_static/v2_posterior.json')
THIN_TO = 150

SCALARS = ['beta0', 'beta_gender', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x',
           'delta1', 'delta2', 'delta1_x', 'delta2_x',
           'sigma_user', 'sigma_gym', 'log_lambda0',
           'kappa', 'rho', 'beta_h_missing', 'beta_a_missing',
           'sat_amp', 'sat_h0', 'sat_scale', 'vq_curv', 'vq_peak']


def thin(arr, to=THIN_TO):
    step = max(1, arr.shape[1] // to)
    return arr[:, ::step][:, :to]


def prior_draws(fit_args, n=1500):
    """Sample this fit's prior. Needs the model rebuilt with its own settings,
    since e.g. the saturating arm has parameters the quadratic arm does not.

    `marginalize_singles` is deliberately NOT passed through. Integrating out
    the per-climber offsets changes the likelihood, not the priors: every
    parameter shown here has exactly the same prior in both versions. Building
    the cheaper unmarginalized model gives the identical answer.
    """
    import pymc as pm

    from kaya.grading_model_v2 import build_model_v2, make_dataset
    base = pickle.load(open(data_file('base_bouldering.pkl'), 'rb'))
    nets = json.loads(data_file('networks.json').read_text())['networks']
    ds = make_dataset(base, nets[fit_args['network']],
                      name_filter=fit_args['name_filter'],
                      label=f"{fit_args['network']}/{fit_args['name_filter']}")
    model = build_model_v2(ds, height_form=fit_args['height_form'],
                           gender_mode=fit_args.get('gender_mode', 'point'),
                           ape_x_gender=fit_args.get('ape_x_gender', False),
                           estimate_sigma_link=not fit_args.get('fixed_sigma_link', True),
                           zero_sum_users=fit_args.get('zero_sum_users', False))
    want = [v for v in SCALARS if v in [rv.name for rv in model.free_RVs]]
    with model:
        pr = pm.sample_prior_predictive(draws=n, var_names=want, random_seed=11)
    out = {}
    for name in want:
        v = np.asarray(pr.prior[name].values).reshape(-1)
        step = max(1, v.size // THIN_TO)
        out[name] = [round(float(x), 5) for x in v[::step][:THIN_TO]]
    return out


def dataset_scales(fit_args):
    """The centring/scaling the model applies to height and ape index.

    The viewer needs these to put its x-axes back in inches. They are a
    property of the fitted dataset, not constants -- net50/confident has
    h_sd 3.92 in, and an earlier hard-coded 3.4 in the page was simply wrong.
    """
    from kaya.grading_model_v2 import make_dataset
    base = pickle.load(open(data_file('base_bouldering.pkl'), 'rb'))
    nets = json.loads(data_file('networks.json').read_text())['networks']
    ds = make_dataset(base, nets[fit_args['network']],
                      name_filter=fit_args['name_filter'],
                      label=f"{fit_args['network']}/{fit_args['name_filter']}")
    u = ds.users
    h = u['height'].to_numpy(float)
    a = u['ape_index'].to_numpy(float)
    # p_gf is the probability the climber is female; the model's own split.
    g = u['p_gf'].to_numpy(float)

    def stats(v):
        v = v[~np.isnan(v)]
        if not v.size:
            return None
        return {'n': int(v.size),
                'median': round(float(np.median(v)), 2),
                'sd': round(float(np.std(v)), 3),
                'p1': round(float(np.percentile(v, 1)), 2),
                'p99': round(float(np.percentile(v, 99)), 2)}

    out = {
        'network': fit_args['network'], 'name_filter': fit_args['name_filter'],
        'h_median': round(float(np.nanmedian(h)), 3),
        'h_sd': round(float(np.nanstd(h)), 3),
        'a_median': round(float(np.nanmedian(a)), 3),
        'a_sd': round(float(np.nanstd(a)), 3),
        # 1st/99th percentiles, so the viewer draws curves over the range the
        # data actually covers instead of extrapolating into empty space.
        'h_lo': round(float(np.nanpercentile(h, 1)), 2),
        'h_hi': round(float(np.nanpercentile(h, 99)), 2),
        'a_lo': round(float(np.nanpercentile(a, 1)), 2),
        'a_hi': round(float(np.nanpercentile(a, 99)), 2),
        # The ape axis is drawn symmetric about zero, so it needs the widest
        # side, not each side separately.
        'a_abs': round(float(np.nanpercentile(np.abs(a), 99.5)), 2),
    }
    # Per-gender, for the "where each group actually sits" bands. Height and
    # ape both get them: the ape *model* is gender-blind in every arm except
    # ape_x_gender, but the ape *distribution* is not.
    for lab, mask in [('male', g < 0.5), ('female', g >= 0.5)]:
        out[f'h_{lab}'] = stats(h[mask])
        out[f'a_{lab}'] = stats(a[mask])
    return out


# What each fit is FOR, so the page can show seven models instead of
# twenty-two by default.
#
#   'form'    a distinct height form on the standard dataset -- the comparison
#             the page is actually about
#   'refit'   the same settings fitted again with a different seed. These exist
#             to measure the noise floor, and overlaying them says nothing
#             except that the sampler is stochastic
#   'variant' a side experiment that changes something other than the height
#             form (a different name filter, zero-sum user offsets, extra ape
#             terms), so its posterior is not comparable to the rest
#
# The refit names match the replicate table in build_v2_reliability.py; if a
# refit is added there it belongs here too.
REFITS = {'v4_lin_b', 'v4_lin_c', 'v4_lin_d', 'v4_lin_e'}
VARIANTS = {'v3_apex', 'v4_lin_apex', 'v4_lin_apelin'}


def fit_role(base, name_filter, zero_sum_users):
    """Which of the three kinds of fit this is. See REFITS / VARIANTS above."""
    if base in REFITS:
        return 'refit'
    # Derived rather than listed where the payload already knows: a different
    # climber subset or a different offset parameterisation is a variant
    # whatever it is called.
    if base in VARIANTS or name_filter != 'confident' or zero_sum_users:
        return 'variant'
    return 'form'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--skip-priors', action='store_true')
    ap.add_argument('--primary', default='v3_conf',
                    help='fit whose dataset defines the exported scales')
    args = ap.parse_args()

    payload = {'thin_to': THIN_TO, 'fits': {}}
    for name in trace_names('v3_*', 'v4_*'):
        if name.startswith('smoke'):
            continue
        res = json.loads(result_file(name).read_text())
        idata = az.from_netcdf(trace_file(name))
        post = idata.posterior
        warm = idata.warmup_posterior if 'warmup_posterior' in idata.groups() else None
        present = [v for v in SCALARS if v in post]
        summ = az.summary(idata, var_names=present, hdi_prob=0.89)

        # Which version of the model this fit came from. The two arms are the
        # same model except for whether the per-climber offsets are estimated
        # or integrated out, so every figure that shows a parameter has to say
        # which one it is showing -- the draws are not interchangeable.
        marg = bool(res['args'].get('marginalize_singles', False))
        fit = {'n_chains': int(post.sizes['chain']), 'n_draws': int(post.sizes['draw']),
               'n_warmup': int(warm.sizes['draw']) if warm is not None else 0,
               'arm': 'marginalized' if marg else 'unmarginalized',
               'base': name[:-5] if name.endswith('_marg') else name,
               'height_form': res['args']['height_form'],
               'name_filter': res['args']['name_filter'],
               'zero_sum_users': bool(res['args'].get('zero_sum_users', False)),
               'role': fit_role(name[:-5] if name.endswith('_marg') else name,
                                res['args']['name_filter'],
                                bool(res['args'].get('zero_sum_users', False))),
               'max_rhat': round(res['max_rhat'], 3),
               'minutes': round(res['elapsed_min']),
               # The page shows failed fits deliberately -- an unreliable refit
               # is one of its findings -- so this flags rather than drops them.
               # What it prevents is a reader taking a broken chain's posterior
               # for a measurement because nothing on the figure said otherwise.
               'convergence': assess_result(res).as_dict(),
               'params': {}}
        for p in present:
            r = summ.loc[p]
            entry = {
                'chains': [[round(float(v), 5) for v in ch] for ch in thin(post[p].values)],
                'mean': round(float(r['mean']), 4), 'sd': round(float(r['sd']), 4),
                'lo': round(float(r['hdi_5.5%']), 4), 'hi': round(float(r['hdi_94.5%']), 4),
                'rhat': round(float(r['r_hat']), 3),
                'ess_bulk': int(r['ess_bulk']), 'ess_tail': int(r['ess_tail']),
            }
            if warm is not None and p in warm:
                entry['warmup'] = [[round(float(v), 5) for v in ch]
                                   for ch in thin(warm[p].values)]
            fit['params'][p] = entry

        st = idata.sample_stats
        diag = {}
        for k, label in [('diverging', 'divergences'), ('tree_depth', 'tree_depth'),
                         ('step_size', 'step_size'), ('n_steps', 'n_steps'),
                         ('acceptance_rate', 'accept')]:
            if k in st:
                v = st[k].values
                diag[label] = {'overall_mean': round(float(v.mean()), 4),
                               'max': round(float(v.max()), 4)}
        if 'diverging' in st:
            diag['divergences']['total'] = int(st['diverging'].values.sum())
        fit['sample_stats'] = diag

        if not args.skip_priors:
            try:
                fit['prior'] = prior_draws(res['args'])
            except Exception as e:
                print(f'  {name}: prior sampling failed ({e})')
        payload['fits'][name] = fit
        # Scales must come from the primary fit's dataset, not from whichever
        # fit happens to sort first -- v3_all uses every name, not the
        # confident-name subset, and its SDs differ.
        if 'scales' not in payload or name == args.primary:
            payload['scales'] = dataset_scales(res['args'])
        print(f"  {name}: {len(fit['params'])} params, warmup="
              f"{fit['n_warmup'] or 'no'}, prior={'yes' if 'prior' in fit else 'no'}")
        del idata

    OUT.write_text(json.dumps(payload, separators=(',', ':')))
    print(f'wrote {OUT}  {os.path.getsize(OUT)/1024:.0f} KB  '
          f'fits={list(payload["fits"])}')


if __name__ == '__main__':
    main()
