"""Does integrating out the climber offsets change the ANSWERS?

The two model versions' predictive scores are not comparable -- they predict
different things -- but their parameters are the same parameters. A gym
correction means "how much stiffer this gym grades than the network average"
in both. So the question that actually matters can be asked directly:

    does the headline move, or was this only about fixing the comparison?

Three things are compared, per height form:

  * **gym corrections**, gym by gym, with their uncertainty. The published
    result. If these hold, the marginalization was a methodological repair
    rather than a change of finding.
  * **height and ape coefficients**, which is what the whole form comparison
    is about.
  * **sigma_user and sigma_gym**. sigma_user is the one to watch: with 10,357
    free offsets the original could let per-climber parameters absorb
    variation that belongs to the population spread, so it is the parameter
    most likely to move, and a large move is informative rather than alarming.

Writes src/kaya/viewer_static/v2_arm_params.json.

Run from the repo root -- running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
TRACES = ROOT / 'runs' / 'traces'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_arm_params.json'

# height form label -> unmarginalized trace name. The marginalized partner is
# the same name with _marg appended.
PAIRS = {
    'straight line': 'v3_lin',
    'curve, differs by gender': 'v3_conf',
    'no height term': 'v3_zero',
    'plain curve': 'v3_quad',
    'saturating curve': 'v3_sat',
    'straight line, differs by gender': 'v4_linxg',
}
SCALARS = ['beta0', 'beta_gender', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x',
           'delta1', 'delta2', 'beta_h_missing', 'beta_a_missing',
           'sigma_user', 'sigma_gym', 'log_lambda0', 'kappa', 'rho']


def summarise(name):
    f = TRACES / f'idata_{name}.nc'
    if not f.exists():
        return None
    post = az.from_netcdf(str(f)).posterior
    out = {'scalars': {}, 'gyms': {}}
    for v in SCALARS:
        if v in post:
            x = np.asarray(post[v].values).ravel()
            lo, hi = np.percentile(x, [5.5, 94.5])
            out['scalars'][v] = {'mean': float(x.mean()), 'sd': float(x.std(ddof=1)),
                                 'lo': float(lo), 'hi': float(hi)}
    if 'gym_correction_c' in post:
        da = post['gym_correction_c']
        gym_dim = [d for d in da.dims if d not in ('chain', 'draw')][0]
        ids = [str(g) for g in da[gym_dim].values]
        arr = da.values.reshape(-1, len(ids))
        lo, hi = np.percentile(arr, [5.5, 94.5], axis=0)
        for i, g in enumerate(ids):
            out['gyms'][g] = {'mean': float(arr[:, i].mean()),
                              'sd': float(arr[:, i].std(ddof=1)),
                              'lo': float(lo[i]), 'hi': float(hi[i])}
    return out


def main():
    # Gym display names, so the chart is readable.
    names = {}
    rf = ROOT / 'src/kaya/viewer_static/v2_results.json'
    if rf.exists():
        for r in json.loads(rf.read_text()).get('gyms', []):
            names[str(r['i'])] = {'g': r.get('g'), 'b': r.get('b')}

    forms = {}
    for label, base in PAIRS.items():
        old, new = summarise(base), summarise(base + '_marg')
        if not old or not new:
            missing = 'unmarginalized' if not old else 'marginalized'
            print(f'-- {label}: {missing} fit not found, skipping')
            continue
        shared = sorted(set(old['gyms']) & set(new['gyms']))
        gyms = [{'id': g, **names.get(g, {}),
                 'old': old['gyms'][g], 'new': new['gyms'][g]} for g in shared]
        if gyms:
            o = np.array([x['old']['mean'] for x in gyms])
            n = np.array([x['new']['mean'] for x in gyms])
            stats = {
                'n_gyms': len(gyms),
                'corr': round(float(np.corrcoef(o, n)[0, 1]), 4),
                'max_shift': round(float(np.abs(o - n).max()), 4),
                'mean_abs_shift': round(float(np.abs(o - n).mean()), 4),
                'spread_old': round(float(o.max() - o.min()), 4),
                'spread_new': round(float(n.max() - n.min()), 4),
                # Rank changes are what a reader of the league table notices.
                'rank_changes': int(np.sum(
                    np.argsort(np.argsort(-o)) != np.argsort(np.argsort(-n)))),
                # How big is the shift next to the uncertainty on it?
                'max_shift_in_sd': round(float(np.max(
                    np.abs(o - n) / np.maximum(
                        [x['old']['sd'] for x in gyms], 1e-9))), 2),
            }
        else:
            stats = {}
        forms[label] = {'base': base, 'gyms': gyms, 'stats': stats,
                        'scalars': {k: {'old': old['scalars'].get(k),
                                        'new': new['scalars'].get(k)}
                                    for k in SCALARS
                                    if k in old['scalars'] or k in new['scalars']}}
        print(f'\n=== {label} ({base} vs {base}_marg) ===')
        if stats:
            print(f'  gym corrections: correlation {stats["corr"]:.4f}, '
                  f'largest shift {stats["max_shift"]:.3f} grades '
                  f'({stats["max_shift_in_sd"]:.1f} sd), '
                  f'{stats["rank_changes"]} of {stats["n_gyms"]} gyms change rank')
            print(f'  spread across gyms: {stats["spread_old"]:.3f} -> '
                  f'{stats["spread_new"]:.3f} grades')
        print(f'  {"parameter":>16} {"original":>20} {"marginalized":>20} {"shift/sd":>9}')
        for k, v in forms[label]['scalars'].items():
            a, b = v['old'], v['new']
            if not a or not b:
                continue
            z = abs(a['mean'] - b['mean']) / max(a['sd'], 1e-9)
            print(f'  {k:>16} {a["mean"]:>12.3f} +/-{a["sd"]:<6.3f} '
                  f'{b["mean"]:>12.3f} +/-{b["sd"]:<6.3f} {z:>9.1f}')

    if not forms:
        print('\nno matched pairs yet -- the marginalized fits are still running')
    OUT.write_text(json.dumps({'forms': forms}, separators=(',', ':')))
    print(f'\nwrote {OUT}  {OUT.stat().st_size/1024:.0f} KB')


if __name__ == '__main__':
    main()
