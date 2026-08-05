"""Regenerate the Grading Model v2 tab's data file from the fits on disk.

Reads every result_v3_*.json produced by run_grading_fit.py, plus the gym
id -> name/brand mapping, and writes one JSON the viewer renders. Re-run this
whenever a fit lands; nothing on the page should be hand-typed.
"""
import argparse
import csv
import json
import os
from pathlib import Path

import arviz as az

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
OUT = Path('/Users/peterwilliams/projects/kaya/src/kaya/viewer_static/v2_results.json')

# What each height form claims, for the comparison table.
FORM_LABEL = {
    'zero': 'Zero', 'linear': 'Linear', 'quadratic': 'Quadratic',
    'linear_x_gender': 'Linear × gender',
    'quadratic_x_gender': 'Quadratic × gender', 'saturating': 'Saturating',
    'vertex_quadratic': 'Vertex quadratic',
}
FORM_NPARAM = {'zero': 0, 'linear': 1, 'linear_x_gender': 2, 'quadratic': 2,
               'quadratic_x_gender': 4, 'saturating': 3, 'vertex_quadratic': 2}
# Fits that are not entries in the height-form comparison.
NOT_A_FORM_ARM = {'v3_all', 'v3_apex', 'v3_zsu'}

KEY = ['sigma_gym', 'beta_gender', 'kappa', 'rho', 'sigma_user', 'delta1',
       'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x', 'beta0',
       'beta_h_missing', 'beta_a_missing', 'sat_amp', 'sat_h0', 'sat_scale',
       'vq_curv', 'vq_peak']


def gym_lookup():
    """gym_id -> (name, brand), from the annotated correction table."""
    src = data_file('gymcorr_net50_conf.csv')
    if not src.exists():
        return {}
    with open(src) as f:
        return {r['gym_id']: (r['gym'], r['brand']) for r in csv.DictReader(f)}


def pct_single_obs(prim):
    """Fraction of climbers with exactly one observation, from the fit's own
    dataset summary if present, else recomputed from the source data."""
    d = prim.get('dataset') or {}
    if 'pct_single_obs' in d:
        return float(d['pct_single_obs'])
    import pickle

    from kaya.grading_model_v2 import make_dataset
    base = pickle.load(open(data_file('base_bouldering.pkl'), 'rb'))
    nets = json.loads(data_file('networks.json').read_text())['networks']
    a = prim['args']
    ds = make_dataset(base, nets[a['network']], name_filter=a['name_filter'],
                      label='')
    n = ds.observations.groupby('user_id').size()
    return float((n == 1).mean())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--arm', default='unmarginalized',
                    choices=['unmarginalized', 'marginalized'],
                    help='which model version the height-form table covers')
    ap.add_argument('--primary', default='v3_conf',
                    help='fit whose gym corrections the page reports')
    args = ap.parse_args()

    names = gym_lookup()
    fits = {}
    # One arm at a time. Broadening the search to runs/ made the marginalized
    # fits visible here for the first time, and they must NOT be mixed into the
    # height-form table: the two arms predict different things, so their scores
    # are not comparable and a combined ranking would be meaningless.
    arm_fits = [n for n in trace_names('v3_*')
                if n.endswith('_marg') == (args.arm == 'marginalized')]
    for path in [str(result_file(n)) for n in arm_fits]:
        d = json.loads(Path(path).read_text())
        fits[d['name']] = d

    if args.primary not in fits:
        raise SystemExit(f'primary fit {args.primary} not found; have {sorted(fits)}')
    prim = fits[args.primary]

    # --- gym corrections, read from the trace ------------------------------
    # run_grading_fit.py only summarises KEY_PARAMS into its result JSON; the
    # 29 gym corrections are printed but not stored, so read them from the
    # netcdf instead of re-deriving them by hand.
    trace = trace_file(args.primary)
    if not trace.exists():
        raise SystemExit(f'trace not found: {trace}')
    idata = az.from_netcdf(str(trace))
    gs = az.summary(idata, var_names=['gym_correction_c'], hdi_prob=0.89)
    gyms = []
    for k, v in gs.iterrows():
        gid = k[k.index('[') + 1:-1]
        name, brand = names.get(gid, (f'gym {gid}', 'Unknown'))
        gyms.append({
            'i': gid, 'g': name, 'b': brand,
            'm': round(float(v['mean']), 3), 'lo': round(float(v['hdi_5.5%']), 3),
            'hi': round(float(v['hdi_94.5%']), 3),
            's': bool(v['hdi_5.5%'] > 0 or v['hdi_94.5%'] < 0),
            'rhat': round(float(v['r_hat']), 3),
        })
    gyms.sort(key=lambda r: r['m'])

    # --- height-form comparison ------------------------------------------
    forms, best = [], None
    for name, d in fits.items():
        if name in NOT_A_FORM_ARM or not d.get('loo'):
            continue
        hf = d['args']['height_form']
        forms.append({
            'fit': name, 'form': hf, 'label': FORM_LABEL.get(hf, hf),
            'k': FORM_NPARAM.get(hf), 'elpd': round(d['loo']['elpd_loo'], 1),
            'se': round(d['loo']['se'], 1), 'p_loo': round(d['loo']['p_loo'], 1),
            'rhat': round(d['max_rhat'], 2), 'ess': int(d['min_ess']),
            'div': d['divergences'], 'min': round(d['elapsed_min']),
        })
    forms.sort(key=lambda r: -r['elpd'])
    if forms:
        best = forms[0]['elpd']
        for f in forms:
            f['d_elpd'] = round(f['elpd'] - best, 1)
        # The standard error of the *difference*, from paired pointwise LOO.
        # Differencing two totals and adding their SEs in quadrature would be
        # far too pessimistic -- the models are scored on the same
        # observations. Without this column the ranking oversells itself:
        # every gap here turns out to be inside one dse.
        try:
            idatas = {f['fit']: az.from_netcdf(trace_file(f['fit']))
                      for f in forms}
            cmp = az.compare(idatas, ic='loo', scale='log')
            for f in forms:
                if f['fit'] in cmp.index:
                    f['dse'] = round(float(cmp.loc[f['fit'], 'dse']), 1)
            del idatas
        except Exception as e:
            print(f'  paired LOO comparison failed ({e}); dse omitted')

    # --- key parameters across every fit, to show what replicates ---------
    replication = {}
    for p in KEY:
        row = {}
        for name, d in fits.items():
            v = d['params'].get(p)
            if v:
                row[name] = {'m': round(v['mean'], 3), 'lo': round(v['hdi_5.5%'], 3),
                             'hi': round(v['hdi_94.5%'], 3), 'rhat': round(v['r_hat'], 2)}
        if row:
            replication[p] = row

    spread = (gyms[-1]['m'] - gyms[0]['m']) if gyms else 0
    payload = {
        'primary': args.primary,
        'arm': args.arm,
        'dataset': prim['dataset'],
        'generated_from': sorted(fits),
        'pending': [n for n in ('v3_zero', 'v3_apex', 'v3_quad', 'v3_vtx', 'v3_zsu')
                    if n not in fits],
        'gyms': gyms,
        'n_gyms': len(gyms),
        'n_sig': sum(1 for g in gyms if g['s']),
        'spread': round(spread, 3),
        # Share of climbers contributing exactly one (climber, gym) row. The
        # write-up quotes this in two places and they had drifted apart, so it
        # is exported rather than retyped.
        'pct_single_obs': round(pct_single_obs(prim), 4),
        'sigma_gym': prim['params']['sigma_gym'],
        'forms': forms,
        'replication': replication,
        'sampling': {'max_rhat': round(prim['max_rhat'], 2),
                     'min_ess': int(prim['min_ess']),
                     'divergences': prim['divergences'],
                     'minutes': round(prim['elapsed_min'])},
    }
    OUT.write_text(json.dumps(payload, separators=(',', ':'), default=float))
    print(f'wrote {OUT}  {os.path.getsize(OUT)/1024:.1f} KB')
    print(f'  primary={args.primary}  gyms={len(gyms)} ({payload["n_sig"]} credible)'
          f'  spread={spread:.3f}')
    print('  forms ranked: ' + ', '.join(f"{f['label']} {f['d_elpd']:+.1f}" for f in forms))
    print(f'  still pending: {payload["pending"] or "none"}')


if __name__ == '__main__':
    main()
