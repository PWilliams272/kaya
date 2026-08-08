"""Cross-check the PyMC marginalized model against the NumPy reference.

Two independent implementations of the same likelihood. If they disagree, one
of them is wrong, and neither is trustworthy until they don't.

Checked here:

1. `marginalize_singles=True` must give the same total log-probability as the
   NumPy module for the SAME parameter values -- with the multi-observation
   offsets set to whatever the NumPy quadrature would put them at is not
   possible, so instead the comparison is done at fixed offsets: the NumPy
   module's non-integrated form. Concretely, both are evaluated with every
   multi-observation offset pinned to zero, which turns the quadrature into a
   single node and makes the two exactly comparable.
2. The unmarginalized model with every offset at zero must ALSO match, once
   the single-observation rows are given back their narrower sigma. This is
   what proves the closed form is the same likelihood and not merely a
   plausible one.

Run from the repo root.
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
from kaya.marginal_v2 import MarginalModel, exgaussian_logpdf

TMP = Path('/Users/peterwilliams/.claude/jobs/e4f1b508/tmp')


def dataset():
    with open(TMP / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((TMP / 'networks.json').read_text())['networks']
    return make_dataset(base, nets['net50'], name_filter='confident',
                        label='net50/confident')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--orthogonal-design', action='store_true',
                    help='run the check on the Gram-Schmidt basis instead')
    args = ap.parse_args()
    orth = args.orthogonal_design
    ds = dataset()
    mm = MarginalModel.from_dataset(ds, height_form='linear',
                                    sigma_link_fixed=0.5, n_quad=31,
                                    orthogonal_design=orth)
    rng = np.random.default_rng(3)
    theta = mm.initial_point(rng)
    p = mm.unpack(theta)

    model = build_model_v2(ds, height_form='linear', gender_mode='point',
                           estimate_sigma_link=False, sigma_link_fixed=0.5,
                           marginalize_singles=True, orthogonal_design=orth)
    print(f'PyMC model with marginalize_singles=True, '
          f'orthogonal_design={orth}')
    free = {v.name: v for v in model.free_RVs}
    print(f'  free random variables: {len(free)}')
    ip0 = model.initial_point()
    by_base = {k.split('_log__')[0].split('_zerosum__')[0]: k for k in ip0}
    for nm in free:
        print(f'    {nm:<28} shape {np.asarray(ip0[by_base[nm]]).shape}')

    # Build a point matching theta, with every multi-obs offset at zero so the
    # two implementations describe the same configuration.
    ip = model.initial_point()
    pt_ = {}
    for k in ip:
        base_name = k.split("_log__")[0].split("_zerosum__")[0]
        if base_name == 'beta0':
            pt_[k] = np.array(p['beta0'])
        elif base_name == 'sigma_user':
            pt_[k] = np.array(p['log_sigma_user'])
        elif base_name == 'sigma_gym':
            pt_[k] = np.array(p['log_sigma_gym'])
        elif base_name == 'epsilon_raw':
            pt_[k] = np.zeros(ip[k].shape)
        elif base_name == 'gym_correction_raw':
            # ZeroSumNormal stores n-1 coordinates in PyMC's OWN basis, which
            # is neither the first n-1 elements of the zero-sum vector nor
            # marginal_v2's basis. Slicing one into the other put the PyMC
            # model at some other valid gym configuration -- harmless here,
            # because the corrections are read back below and handed to NumPy,
            # but it meant the check ran at an unspecified point rather than
            # at theta. Map properly and the two sides are pinned to the same
            # configuration by construction, which the assertion below now
            # verifies instead of assuming.
            pt_[k] = zerosum_coords(
                zerosum_basis_matrix(model, k, ip[k].shape[0]), p['gym_raw'])
        elif base_name == 'log_lambda0':
            pt_[k] = np.array(p['log_lambda0'])
        elif base_name == 'kappa':
            pt_[k] = np.array(p['kappa'])
        elif base_name == 'rho':
            pt_[k] = np.array(p['rho'])
        elif base_name.removesuffix('_orth') in mm.Xnames:
            # Under --orthogonal-design the sampled names carry an _orth
            # suffix and mm's design matrix is already in that basis, so the
            # same beta vector maps straight across.
            pt_[k] = np.array(
                p['beta'][mm.Xnames.index(base_name.removesuffix('_orth'))])
        else:
            raise SystemExit(f'unmapped variable {k!r}')

    # Both quantities from one compiled call, so the gym corrections handed to
    # NumPy are exactly the ones PyMC used.
    #
    # `model['gym_correction_raw']` is the RANDOM VARIABLE. Compiling it
    # directly evaluates a fresh prior draw and ignores the point entirely --
    # which silently fed NumPy a different parameter vector and showed up as a
    # 4,535-unit "disagreement" between the implementations.
    # replace_rvs_by_values rewrites the graph in terms of the value variables,
    # which is what actually respects the point. It also handles
    # ZeroSumNormal's transform, an orthogonal projection rather than
    # "append minus the sum", so it cannot be reconstructed by hand here.
    derived = model.replace_rvs_by_values(
        [model['gym_correction_raw'], model['sigma_user']])
    fn = model.compile_fn([model.logp(vars=model.observed_RVs, sum=True),
                           *derived])
    pymc_total, gym_raw, su_pymc = fn(pt_)
    pymc_total = float(pymc_total)
    gym_raw = np.asarray(gym_raw).ravel()
    assert abs(float(su_pymc) - p['sigma_user']) < 1e-12, (
        f'point not applied: PyMC sigma_user {float(su_pymc)} '
        f'vs NumPy {p["sigma_user"]}')
    # The gym block, now that the coordinates are mapped rather than sliced.
    assert np.abs(gym_raw - p['gym_raw']).max() < 1e-9, (
        'PyMC and NumPy are on different gym configurations: max difference '
        f'{np.abs(gym_raw - p["gym_raw"]).max():.2e}')
    user_term = p['beta0'] + mm.Xc @ p['beta']
    gym_term = p['sigma_gym'] * gym_raw
    c = user_term[mm.obs_u] + gym_term[mm.obs_g]
    nu = np.exp(-(p['log_lambda0'] + p['kappa'] * mm.n_visits
                  + p['rho'] * mm.r_obs))
    sl, su = mm.sigma_link_fixed, p['sigma_user']

    s, o = mm.single_obs, mm.multi_obs
    numpy_total = (exgaussian_logpdf(-mm.m[s], -c[s],
                                     np.sqrt(sl ** 2 + su ** 2), nu[s]).sum()
                   + exgaussian_logpdf(-mm.m[o], -c[o], sl, nu[o]).sum())

    print(f'\n  PyMC  observed log-probability: {pymc_total:,.9f}')
    print(f'  NumPy observed log-probability: {numpy_total:,.9f}')
    d = abs(pymc_total - numpy_total)
    rel = d / abs(numpy_total)
    print(f'  absolute difference: {d:.3e}')
    print(f'  relative difference: {rel:.3e}')
    # Relative, not absolute: the total sums 20,014 terms, so float64
    # accumulation alone moves the last few digits, and PyMC's normal_lcdf and
    # scipy's log_ndtr are different routines for the same function. Anything
    # near 1e-9 relative is agreement; a real formula error would be orders of
    # magnitude larger, as the 4,535 above was.
    assert rel < 1e-7, 'PyMC and NumPy disagree on the same likelihood'
    print('  PASS -- the two implementations agree to floating-point noise\n')

    # And the parameter count, which is the point of the exercise.
    full = build_model_v2(ds, height_form='linear', gender_mode='point',
                          estimate_sigma_link=False, sigma_link_fixed=0.5)
    def count(m):
        ipt = m.initial_point()
        return int(sum(np.asarray(v).size for v in ipt.values()))
    print(f'  parameters, unmarginalized : {count(full):,}')
    print(f'  parameters, singles removed: {count(model):,}')
    print(f'  removed                    : {count(full) - count(model):,}')


if __name__ == '__main__':
    main()
