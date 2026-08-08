"""Recover climber-level PSIS-LOO for marginalized fits whose result JSON has none.

Why this exists: fits launched before run_fit.py learned to assemble the
marginalized log-likelihood recorded `loo: null`. The traces are fine -- the
per-climber terms were always saved as the Deterministic `log_lik_multi` -- so
the elpd is recoverable without refitting. At ~3.5 hours per fit that matters.

PSIS-LOO = Pareto-smoothed importance sampling leave-one-out cross-validation:
an estimate of how well the model predicts a held-out unit, computed from the
draws it already has instead of by refitting N times. **Higher elpd is better.**
Pareto k is its own reliability diagnostic -- **lower is better**, and above 0.7
the estimate for that unit is not trustworthy.

THE UNIT IS THE CLIMBER, NOT THE OBSERVATION. Integrating out a climber's
ability offset makes their rows conditionally dependent, so leave-one-row-out no
longer exists in closed form. Every climber contributes exactly one term. This
elpd is therefore comparable across marginalized fits and NOT comparable to any
v7 number, which was per observation.

Usage:
    python scripts/recover_marg_loo.py                 # every v10_*_marg fit
    python scripts/recover_marg_loo.py v10_lin_marg    # named fits only
"""
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np
import xarray as xr

ROOT = Path(__file__).resolve().parents[1]
TRACES = ROOT / 'runs' / 'traces'
RESULTS = ROOT / 'runs' / 'results'


def climber_log_likelihood(idata):
    """Assemble the complete pointwise log-likelihood, one term per climber."""
    single = idata.log_likelihood['m_single']
    sdim = [d for d in single.dims if d not in ('chain', 'draw')][0]
    multi = idata.posterior['log_lik_multi']
    mdim = [d for d in multi.dims if d not in ('chain', 'draw')][0]
    merged = xr.concat(
        [single.rename({sdim: 'climber'}), multi.rename({mdim: 'climber'})],
        dim='climber')
    return merged.assign_coords(climber=np.arange(merged.sizes['climber'])), \
        int(single.sizes[sdim]), int(multi.sizes[mdim])


def recover(name):
    trace = TRACES / f'idata_{name}.nc'
    result = RESULTS / f'result_{name}.json'
    if not trace.exists():
        return f'{name}: no trace'
    idata = az.from_netcdf(str(trace))
    if 'log_lik_multi' not in idata.posterior:
        return f'{name}: not a --marginalize-all fit (no log_lik_multi)'

    merged, n_single, n_multi = climber_log_likelihood(idata)
    idata.log_likelihood = xr.Dataset({'climber_obs': merged})
    loo = az.loo(idata, pointwise=True)

    k = np.asarray(loo.pareto_k.values)
    bad = int((k > 0.7).sum())
    out = {'elpd_loo': float(loo.elpd_loo), 'se': float(loo.se),
           'p_loo': float(loo.p_loo), 'unit': 'climber',
           'n_units': int(merged.sizes['climber']),
           'n_single': n_single, 'n_multi': n_multi,
           'pareto_k_max': float(k.max()), 'pareto_k_over_0p7': bad,
           'recovered_by': 'scripts/recover_marg_loo.py'}

    if result.exists():
        res = json.loads(result.read_text())
        res['loo'] = out
        res['loo_unit'] = 'climber'
        result.write_text(json.dumps(res, indent=2, default=float))

    flag = '' if bad == 0 else f'  ** {bad} climbers with Pareto k > 0.7 **'
    return (f'{name}: elpd {out["elpd_loo"]:+.1f} +/- {out["se"]:.1f} '
            f'over {out["n_units"]:,} climbers '
            f'({n_single:,} single + {n_multi:,} multi), '
            f'max k {out["pareto_k_max"]:.2f}{flag}')


def main():
    names = sys.argv[1:]
    if not names:
        names = sorted(p.stem[len('idata_'):] for p in TRACES.glob('idata_v10_*_marg.nc'))
    if not names:
        print('no marginalized traces found')
        return
    print('PSIS-LOO, leave-one-CLIMBER-out. Higher elpd is better.')
    print('Not comparable to a v7 elpd -- different unit.\n')
    for n in names:
        print(' ', recover(n), flush=True)


if __name__ == '__main__':
    main()
