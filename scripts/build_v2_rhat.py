"""What R-hat actually measures, from this project's own chains.

R-hat is quoted all over the page as a pass/fail at 1.01, which invites two
wrong readings: that 1.00 is a floor, and that 1.02 is a rounding error. Both
are wrong, and the traces already on disk can show why rather than assert it.

For m chains of n draws, with theta_bar_j the mean of chain j:

    B = n/(m-1) * sum_j (theta_bar_j - theta_bar)^2      between-chain
    W = (1/m) * sum_j s_j^2                              within-chain
    var_plus = (n-1)/n * W + B/n
    R_hat = sqrt(var_plus / W)

Rearranged, R_hat stops looking like a number near 1 and starts being
readable:

    B/W = n * (R_hat^2 - 1) + 1

which is roughly the autocorrelation factor -- how many draws it takes this
sampler to produce one draw's worth of new information. At 500 draws per
chain, R_hat = 1.01 means B/W = 11.

This script exports:
  * classic vs arviz's rank-normalized split R-hat, per parameter
  * how far below 1.0 R-hat actually goes across every trace
  * the same chains scored at several lengths, since R_hat - 1 shrinks with n
    at fixed mixing quality
  * R-hat and effective sample size per height form, both arms, which is the
    direct measurement of whether integrating the offsets out samples better

Writes src/kaya/viewer_static/v2_rhat.json. Run from the repo root.
"""
import argparse
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

from kaya.viewer_paths import result_file, trace_file, trace_names

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_rhat.json'

PRIMARY = 'v3_lin_marg'
# Parameters worth showing individually: the model's own quantities, not the
# deterministic transforms of them (lambda0 is exp(log_lambda0), so the pair
# correlates at exactly 1.000 and says nothing).
SHOW = ['beta0', 'beta_gender', 'gamma1', 'sigma_user', 'sigma_gym',
        'log_lambda0', 'kappa', 'rho', 'beta_h_missing', 'beta_a_missing']
LENGTHS = [125, 250, 500]


def classic_rhat(x):
    """Gelman-Rubin from the definition. x is (chains, draws). Returns (R, B/W)."""
    m, n = x.shape
    means = x.mean(axis=1)
    b = n / (m - 1) * ((means - means.mean()) ** 2).sum()
    w = float(x.var(axis=1, ddof=1).mean())
    if w <= 0:
        return 1.0, 1.0
    return float(np.sqrt(((n - 1) / n * w + b / n) / w)), float(b / w)


def scalar_params(idata):
    return [p for p in idata.posterior.data_vars
            if idata.posterior[p].values.ndim == 2]


def _cond(X):
    """Condition number of a design block's correlation matrix.

    1 is perfectly round; large means one direction is far narrower than
    another, which is the shape a diagonal mass matrix cannot represent.
    """
    ev = np.linalg.eigvalsh(np.corrcoef(X.T))
    return float(ev[-1] / max(ev[0], 1e-12))


def design_block():
    """What orthogonalising the design does, measured on the design itself.

    Nothing here needs a trace: it is a property of the covariate matrix, so
    it can be reported before the fit that tests it has finished. That is the
    point -- the page can say what the change does and what it costs without
    waiting on, or pre-judging, the run that decides whether it helps.
    """
    import pickle

    from kaya.grading_model_v2 import PRIOR_SD, make_dataset
    from kaya.marginal_v2 import prepare_design

    root = Path(__file__).resolve().parents[1]
    with open(root / 'runs' / 'base_bouldering.pkl', 'rb') as fh:
        base = pickle.load(fh)
    nets = json.loads((root / 'runs' / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')

    forms = [('zero', 'no height term'), ('linear', 'linear'),
             ('quadratic', 'quadratic'), ('linear_x_gender', 'linear × gender'),
             ('quadratic_x_gender', 'quadratic × gender'),
             ('saturating', 'saturating'),
             ('vertex_quadratic', 'vertex quadratic')]
    rows = []
    for form, label in forms:
        d0 = prepare_design(ds, height_form=form)
        d1 = prepare_design(ds, height_form=form, orthogonal_design=True)
        X0, names = d0['Xc'], d0['Xnames']
        c = np.corrcoef(X0.T)
        iu = np.triu_indices(len(names), 1)
        k = int(np.argmax(np.abs(c[iu])))
        i, j = iu[0][k], iu[1][k]
        rows.append({
            'form': form, 'label': label, 'n_cols': len(names),
            'a': names[i], 'b': names[j], 'r': round(float(c[i, j]), 3),
            'cond_raw': round(_cond(X0), 1),
            'cond_orth': round(_cond(d1['Xc']), 4),
            # saturating and vertex_quadratic are nonlinear IN THE PARAMETERS,
            # so their height terms never enter the design matrix and cannot be
            # rotated. Flagged rather than quietly listed beside forms the
            # change actually acts on.
            'rotatable': form in {'zero', 'linear', 'quadratic',
                                  'linear_x_gender', 'quadratic_x_gender'},
        })

    # The prior trade-off, on the page's primary form. Rescaling each
    # orthogonalised column back to its raw norm is what keeps the prior on the
    # fitted CURVE intact; the priors on individual coefficients still move,
    # and that movement is the whole statistical content of the change.
    d0 = prepare_design(ds, height_form='quadratic_x_gender')
    d1 = prepare_design(ds, height_form='quadratic_x_gender',
                        orthogonal_design=True)
    names = d0['Xnames']
    T = np.asarray(d1['consts']['X_orth_T'])
    sd = np.array([PRIOR_SD.get(n, 1.0) for n in names])
    S = T @ np.diag(sd ** 2) @ T.T
    curve0 = float(np.sqrt((d0['Xc'] ** 2 * sd ** 2).sum(axis=1)).mean())
    curve1 = float(np.sqrt((d1['Xc'] ** 2 * sd ** 2).sum(axis=1)).mean())

    # The orthogonal basis functions, written out. T is upper triangular, so
    # column j is raw column j plus a combination of the ones before it;
    # dividing by T[j,j] normalises the leading coefficient to 1 and makes the
    # result readable as "h^2 minus its projection onto h".
    basis = []
    for j, nm in enumerate(names):
        terms = [{'on': names[k], 'c': round(float(T[k, j] / T[j, j]), 3)}
                 for k in range(j) if abs(T[k, j] / T[j, j]) >= 5e-4]
        if terms:
            basis.append({'name': nm, 'terms': terms})

    return {
        'forms': rows,
        'primary_form': 'quadratic_x_gender',
        'basis': basis,
        'prior': {
            'curve_raw': round(curve0, 3), 'curve_orth': round(curve1, 3),
            'coefs': [{'name': n, 'sd': float(sd[i]),
                       'implied': round(float(np.sqrt(S[i, i])), 3),
                       'ratio': round(float(np.sqrt(S[i, i]) / sd[i]), 2)}
                      for i, n in enumerate(names)],
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--primary', default=PRIMARY)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()
    out = Path(args.out) if args.out else OUT

    idata = az.from_netcdf(trace_file(args.primary))
    n_chains, n_draws = idata.posterior[scalar_params(idata)[0]].values.shape

    # --- classic vs split, per parameter ---
    params = []
    for p in SHOW:
        if p not in idata.posterior:
            continue
        x = idata.posterior[p].values
        r, bw = classic_rhat(x)
        params.append({
            'name': p,
            'classic': round(r, 4),
            'bw': round(bw, 2),
            'split': round(float(az.rhat(idata, var_names=[p])[p].values), 4),
            'ess': int(az.ess(idata, var_names=[p])[p].values),
        })

    # --- the same chains at several lengths ---
    worst = max(params, key=lambda d: d['split'])['name']
    x = idata.posterior[worst].values
    lengths = []
    for n in LENGTHS:
        if n > n_draws:
            continue
        r, bw = classic_rhat(x[:, :n])
        lengths.append({'n': n, 'rhat': round(r, 4), 'bw': round(bw, 1)})

    # --- how far below 1.0 does it go, across everything on disk ---
    below, total, lowest = 0, 0, []
    for name in trace_names('v3_*', 'v4_*'):
        if name.startswith('smoke'):
            continue
        t = az.from_netcdf(trace_file(name))
        for p in scalar_params(t):
            r, _ = classic_rhat(t.posterior[p].values)
            total += 1
            if r < 1:
                below += 1
            lowest.append((r, p, name))
    lowest.sort()

    # --- both arms, per height form: does marginalizing sample better? ---
    arms = {}
    for name in trace_names('v3_*', 'v4_*'):
        if name.startswith('smoke'):
            continue
        res = json.loads(result_file(name).read_text())
        marg = bool(res['args'].get('marginalize_singles', False))
        base = name[:-5] if name.endswith('_marg') else name
        rec = arms.setdefault(base, {})
        rec['marginalized' if marg else 'original'] = {
            'rhat': round(res['max_rhat'], 3), 'ess': int(res['min_ess']),
            'minutes': round(res['elapsed_min']),
        }
    paired = [{'base': b, **v} for b, v in arms.items()
              if 'original' in v and 'marginalized' in v]
    better = sum(1 for p in paired if p['marginalized']['ess'] > p['original']['ess'])

    # Is there a ridge worth rotating? A dense mass matrix, a PCA rotation and
    # emcee's affine invariance all buy the same thing: immunity to LINEAR
    # correlation. Whether that is worth having is a property of each fit, not
    # of "the model" -- the linear form has nothing above 0.6 while the height
    # forms with two terms per gender have pairs near 0.8.
    ridges = []
    for name in trace_names('v3_*', 'v4_*'):
        if name.startswith('smoke'):
            continue
        t = az.from_netcdf(trace_file(name))
        # lambda0 is exp(log_lambda0); the pair correlates at exactly 1.000 and
        # says nothing about geometry.
        sc = [p for p in scalar_params(t) if p != 'lambda0']
        if len(sc) < 2:
            continue
        x = np.column_stack([t.posterior[p].values.ravel() for p in sc])
        c = np.corrcoef(x.T)
        iu = np.triu_indices(len(sc), 1)
        k = int(np.argmax(np.abs(c[iu])))
        i, j = iu[0][k], iu[1][k]
        ev = np.linalg.eigvalsh(c)
        ridges.append({
            'fit': name, 'a': sc[i], 'b': sc[j], 'r': round(float(c[i, j]), 3),
            'n_params': len(sc),
            # Ratio of largest to smallest eigenvalue of the correlation
            # matrix: 1 is perfectly round, large means one direction is far
            # narrower than another and a rotation would pay.
            'condition': round(float(ev[-1] / max(ev[0], 1e-12)), 1),
        })
    ridges.sort(key=lambda d: -abs(d['r']))

    payload = {
        'primary': args.primary, 'n_chains': int(n_chains), 'n_draws': int(n_draws),
        'ridges': ridges,
        'design': design_block(),
        'params': params,
        'worst_param': worst, 'lengths': lengths,
        'below_one': below, 'n_scalars': total,
        'lowest': [{'rhat': round(r, 5), 'param': p, 'fit': f}
                   for r, p, f in lowest[:3]],
        'paired': paired,
        'n_paired_better': better,
        'gate': 1.01,
    }
    out.write_text(json.dumps(payload, separators=(',', ':')))

    print(f'{args.primary}: {n_chains} chains x {n_draws} draws\n')
    print(f"{'param':16s}{'classic':>9}{'B/W':>8}{'split':>9}{'ESS':>7}")
    for p in params:
        print(f"{p['name']:16s}{p['classic']:9.4f}{p['bw']:8.1f}"
              f"{p['split']:9.4f}{p['ess']:7d}")
    print(f'\n{below} of {total} scalars have classic R-hat below 1.0 '
          f'(lowest {lowest[0][0]:.5f})')
    print(f'\nmarginalizing improved min ESS in {better} of {len(paired)} height forms')
    for p in sorted(paired, key=lambda d: d['base']):
        o, m = p['original'], p['marginalized']
        print(f"   {p['base']:14s} {o['rhat']:.3f}/{o['ess']:<4d} -> "
              f"{m['rhat']:.3f}/{m['ess']:<4d}")
    print(f'\nwrote {out}')


if __name__ == '__main__':
    main()
