"""Do four different samplers agree on the same posterior?

PyMC/NUTS, emcee, and dynesty are three different algorithms running two
different implementations of the same likelihood (PyTensor and NumPy). They
have no reason to agree unless everything is right, which is what makes the
comparison worth running: it is the one check that catches a subtly wrong
quadrature, a mis-specified prior, or a transform applied in the wrong
direction. A converged sampler proves it converged. Three samplers agreeing
proves the target was the same.

Reports, per parameter:
  * posterior mean and sd from each sampler
  * the gap between samplers in units of the Monte Carlo error, which is the
    only scale on which "do they agree" has an answer

Run from the repo root. Writes src/kaya/viewer_static/v2_samplers.json.
"""
import argparse
import json
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_samplers.json'

# PyMC names its parameters differently from the flat vector the NumPy model
# uses: sigmas are on the natural scale there and logged here, and the gym
# corrections are a vector rather than 28 scalars. Only the shared scalars are
# compared -- they are the ones any conclusion rests on.
SHARED = ['beta0', 'beta_gender', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x',
          'delta1', 'delta2', 'beta_h_missing', 'beta_a_missing',
          'log_lambda0', 'kappa', 'rho']
DERIVED = {'sigma_user': 'log_sigma_user', 'sigma_gym': 'log_sigma_gym'}


def from_pymc(name):
    """Posterior mean/sd/MCSE per parameter from a PyMC trace."""
    import arviz as az
    f = RUNS / 'traces' / f'idata_{name}.nc'
    if not f.exists():
        return None
    idata = az.from_netcdf(str(f))
    post = idata.posterior
    out = {}
    for v in SHARED:
        if v in post:
            x = post[v].values.ravel()
            out[v] = _summ(x, _ess(idata, v))
    # PyMC samples sigma on the natural scale; the NumPy model samples its log.
    # Compare on the log scale, which is where both are actually exploring.
    for nat, log_name in DERIVED.items():
        if nat in post:
            x = np.log(post[nat].values.ravel())
            out[log_name] = _summ(x, _ess(idata, nat))
    return out


def _ess(idata, v):
    import arviz as az
    try:
        return float(np.asarray(az.ess(idata, var_names=[v])[v].values).ravel()[0])
    except Exception:
        return np.nan


def _summ(x, ess=np.nan):
    sd = float(np.std(x, ddof=1))
    n = ess if np.isfinite(ess) and ess > 0 else len(x)
    return {'mean': float(np.mean(x)), 'sd': sd,
            'mcse': sd / np.sqrt(n), 'ess': float(n)}


def from_emcee(name):
    f = RUNS / 'results' / f'emcee_{name}.npz'
    if not f.exists():
        return None
    d = np.load(f, allow_pickle=True)
    names = [str(s) for s in d['param_names']]
    chain = d['chain']                       # (steps, walkers, ndim)
    tau = np.asarray(d['tau'], float)
    flat = chain.reshape(-1, chain.shape[-1])
    out = {}
    for j, nm in enumerate(names):
        if nm not in SHARED and nm not in DERIVED.values():
            continue
        # Effective draws, not raw draws: emcee's walkers are correlated both
        # along the chain and with each other, and quoting raw n would make
        # the Monte Carlo error look ~50x smaller than it is.
        n_eff = len(flat) / max(tau[j], 1.0)
        out[nm] = _summ(flat[:, j], n_eff)
    return out


def from_nested(tag):
    f = RUNS / 'results' / f'nested_{tag}.json'
    if not f.exists():
        return None
    d = json.loads(f.read_text())
    out = {}
    for nm, mu, sd in zip(d['param_names'], d['mean'], d['sd']):
        if nm in SHARED or nm in DERIVED.values():
            # Nested sampling's samples are weighted; an effective count is
            # not directly available here, so MCSE is left unknown rather than
            # invented.
            out[nm] = {'mean': mu, 'sd': sd, 'mcse': np.nan, 'ess': np.nan}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pymc', default='v3_lin_marg')
    ap.add_argument('--emcee', default='lin')
    ap.add_argument('--nested', default='lin_s1')
    args = ap.parse_args()

    arms = {}
    for label, val in (('PyMC / NUTS', from_pymc(args.pymc)),
                       ('emcee', from_emcee(args.emcee)),
                       ('nested sampling', from_nested(args.nested))):
        if val:
            arms[label] = val
        else:
            print(f'-- {label}: not found, skipping')
    if len(arms) < 2:
        raise SystemExit('need at least two samplers to compare')

    params = [p for p in SHARED + list(DERIVED.values())
              if all(p in a for a in arms.values())]
    labels = list(arms)
    print(f'comparing {len(labels)} samplers on {len(params)} shared parameters')
    print(f'reference: {labels[0]}\n')

    hdr = f'{"parameter":>18}'
    for l in labels:
        hdr += f'{l[:14]+" mean":>21}{"sd":>9}'
    print(hdr)
    rows = []
    for p in params:
        line = f'{p:>18}'
        for l in labels:
            a = arms[l][p]
            line += f'{a["mean"]:>21.4f}{a["sd"]:>9.4f}'
        print(line)
        rows.append({'param': p,
                     **{l: arms[l][p] for l in labels}})

    print('\ndisagreement between samplers, in units of Monte Carlo error')
    print('(under 2 is agreement; a real discrepancy is tens or hundreds)\n')
    ref = labels[0]
    print(f'{"parameter":>18}' + ''.join(f'{"vs "+l[:12]:>18}' for l in labels[1:])
          + f'{"  worst / posterior sd":>24}')
    worst_overall = 0.0
    for p in params:
        a = arms[ref][p]
        line = f'{p:>18}'
        worst_z, worst_frac = 0.0, 0.0
        for l in labels[1:]:
            b = arms[l][p]
            se = np.sqrt(np.nansum([a['mcse'] ** 2, b['mcse'] ** 2]))
            d = abs(a['mean'] - b['mean'])
            z = d / se if se > 0 else np.nan
            frac = d / max(a['sd'], 1e-12)
            worst_z = max(worst_z, 0 if np.isnan(z) else z)
            worst_frac = max(worst_frac, frac)
            line += f'{z:>18.1f}' if np.isfinite(z) else f'{"n/a":>18}'
        line += f'{worst_frac:>24.3f}'
        print(line)
        worst_overall = max(worst_overall, worst_frac)
    print(f'\nlargest disagreement, as a fraction of the posterior sd: '
          f'{worst_overall:.3f}')
    print('A fraction well under 1 means the samplers agree on the answer even '
          'where\nMonte Carlo error is small enough to make them "differ" '
          'statistically.')

    OUT.write_text(json.dumps({'samplers': labels, 'params': rows,
                               'worst_frac_sd': worst_overall},
                              separators=(',', ':'), default=float))
    print(f'\nwrote {OUT}')


if __name__ == '__main__':
    main()
