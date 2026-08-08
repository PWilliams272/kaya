"""Do the two implementations agree on the PRIOR, parameter by parameter?

The likelihood has been cross-checked since the marginalized model was written
(`check_pymc_marginal.py`, agreement to 1e-9). The prior never was, and it is
the half where this project has already shipped a real bug: the first zero-sum
implementation sampled n-1 gym offsets and set the last to minus their sum,
which gives the last gym a prior 28 times wider than every other. The
likelihood never noticed, because both schemes hand it a valid zero-sum vector.
Only the prior differed -- so the two implementations agreed to 1e-9 on the log
LIKELIHOOD while quietly fitting different models.

That is the failure this script exists to catch, generalised: for every
parameter, vary it alone across a grid and compare the two log-priors. They
must differ by a CONSTANT. A constant is harmless -- it is the epsilon_raw
prior PyMC carries and NumPy integrates out, plus normalising terms. Any
variation across the grid is a different prior shape, which is a different
model, and it will not show up anywhere else.

The gym block needs care. PyMC's ZeroSumNormal stores n-1 coordinates in its
OWN basis, which is not the first n-1 elements of the zero-sum vector and not
marginal_v2's basis either. Slicing one into the other -- which
`check_pymc_marginal.py` does -- silently evaluates the two models at different
gym configurations. Here the transform is recovered numerically and inverted,
so both sides sit on the same 29 corrections.

Run from the repo root. Exits non-zero if any parameter's prior shape differs.
"""
import argparse
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

from kaya.grading_model_v2 import (
    build_model_v2,
    make_dataset,
    zerosum_basis_matrix,
    zerosum_coords,
)
from kaya.marginal_v2 import MarginalModel
from kaya.viewer_paths import data_file

ROOT = Path(__file__).resolve().parents[1]
TOL = 1e-8


def dataset():
    with open(data_file('base_bouldering.pkl'), 'rb') as f:
        base = pickle.load(f)
    nets = json.loads(data_file('networks.json').read_text())['networks']
    return make_dataset(base, nets['net50'], name_filter='confident',
                        label='net50/confident')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--height-form', default='quadratic_x_gender',
                    help='the page primary; exercises the most coefficients')
    ap.add_argument('--points', type=int, default=7)
    args = ap.parse_args()

    ds = dataset()
    mm = MarginalModel.from_dataset(ds, height_form=args.height_form,
                                    sigma_link_fixed=0.5, n_quad=31)
    model = build_model_v2(ds, height_form=args.height_form,
                           gender_mode='point', estimate_sigma_link=False,
                           sigma_link_fixed=0.5, marginalize_singles=True)
    ip = model.initial_point()
    zs_key = next(k for k in ip if k.startswith('gym_correction_raw'))
    n_free = int(np.asarray(ip[zs_key]).shape[0])
    M = zerosum_basis_matrix(model, zs_key, n_free)
    prior_fn = model.compile_fn(model.logp(vars=model.free_RVs, sum=True),
                                inputs=model.value_vars,
                                on_unused_input='ignore')

    def pymc_point(theta):
        p = mm.unpack(theta)
        # PyMC's coordinates that reproduce NumPy's 29 corrections, rather
        # than slicing one basis into the other.
        z = zerosum_coords(M, p['gym_raw'])
        pt = {}
        for k in ip:
            b = k.split('_log__')[0].split('_zerosum__')[0]
            if b == 'beta0':
                pt[k] = np.array(p['beta0'])
            elif b == 'sigma_user':
                pt[k] = np.array(p['log_sigma_user'])
            elif b == 'sigma_gym':
                pt[k] = np.array(p['log_sigma_gym'])
            elif b == 'epsilon_raw':
                pt[k] = np.zeros(np.asarray(ip[k]).shape)
            elif b == 'gym_correction_raw':
                pt[k] = z
            elif b == 'log_lambda0':
                pt[k] = np.array(p['log_lambda0'])
            elif b == 'kappa':
                pt[k] = np.array(p['kappa'])
            elif b == 'rho':
                pt[k] = np.array(p['rho'])
            elif b in mm.Xnames:
                pt[k] = np.array(p['beta'][mm.Xnames.index(b)])
            else:
                raise SystemExit(f'unmapped variable {k!r}')
        return pt

    rng = np.random.default_rng(7)
    base = mm.initial_point(rng)
    # Every scalar the NumPy model samples, plus one gym coordinate as a probe
    # for the block the old bug lived in.
    probes = [(nm, j) for j, nm in enumerate(mm.param_names)
              if not nm.startswith('gym_raw[')]
    probes += [(nm, j) for j, nm in enumerate(mm.param_names)
               if nm in ('gym_raw[0]', 'gym_raw[14]', 'gym_raw[27]')]

    print(f'height form: {args.height_form}   '
          f'{len(mm.param_names)} sampled parameters   '
          f'{args.points} points each\n')
    print(f'{"parameter":>18} {"grid":>22} {"spread of the difference":>26}  verdict')
    bad = []
    for nm, j in probes:
        lo, hi = base[j] - 1.0, base[j] + 1.0
        diffs = []
        for val in np.linspace(lo, hi, args.points):
            th = base.copy()
            th[j] = val
            a = mm.log_prior(th)
            b = float(prior_fn(pymc_point(th)))
            diffs.append(a - b)
        spread = float(np.ptp(diffs))
        ok = spread < TOL
        if not ok:
            bad.append((nm, spread))
        print(f'{nm:>18} [{lo:>8.3f},{hi:>8.3f}] {spread:>26.3e}  '
              f'{"same shape" if ok else "*** DIFFERS ***"}')

    print()
    if bad:
        print('PRIORS DISAGREE on: ' + ', '.join(f'{n} ({s:.2e})' for n, s in bad))
        print('A difference that varies across the grid is a different prior, '
              'which is a\ndifferent model -- and nothing else in the test '
              'suite would catch it.')
        return 1
    print(f'PASS -- all {len(probes)} probed directions agree to better than '
          f'{TOL:.0e}.')
    print('The two implementations describe the same prior, so the posteriors '
          'they\nsample are the same target.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
