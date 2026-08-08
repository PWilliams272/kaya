"""The PyTensor quadrature must equal the NumPy reference it was ported from.

`marginal_pt` integrates out every climber offset so PyMC can sample 40
parameters instead of 4,241. That is only worth anything if it computes the
same integral as `marginal_v2`, which is the reference emcee and the nested
samplers already use -- otherwise the two halves of this project quietly fit
different models, which has happened here before (the zero-sum prior bug).

The port is checked at two levels:

  * the per-climber integral on its own, against `multi_log_integral`, and
  * the assembled model's whole log-likelihood, against `log_likelihood`.

The second is the one that matters and the first is what localises a failure.

One subtlety worth stating, because it cost a debugging round: `hermegauss`
weights carry exp(-z^2/2) and sum to sqrt(2*pi), not to 1. The density being
integrated against is the normalised Gaussian, so the 1/sqrt(2*pi) has to come
out of the weights. Omitting it shifts every climber by a constant 0.5*log(2pi)
-- invisible within one fit, fatal to any model comparison.
"""
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

pytest.importorskip('pymc')
import pytensor  # noqa: E402
import pytensor.tensor as pt  # noqa: E402

from kaya.grading_model_v2 import (  # noqa: E402
    build_model_v2,
    make_dataset,
    zerosum_basis_matrix,
    zerosum_coords,
)
from kaya.marginal_pt import build_layout, multi_log_integral_pt  # noqa: E402
from kaya.marginal_v2 import MarginalModel  # noqa: E402
from kaya.viewer_paths import data_file  # noqa: E402

DATA = ROOT / 'runs' / 'base_bouldering.pkl'
pytestmark = pytest.mark.skipif(
    not DATA.exists(),
    reason='runs/base_bouldering.pkl is gitignored; run the pull to enable')

N_QUAD = 31
SEEDS = [3, 11, 42]


@pytest.fixture(scope='module')
def ds():
    with open(data_file('base_bouldering.pkl'), 'rb') as f:
        base = pickle.load(f)
    nets = json.loads(data_file('networks.json').read_text())['networks']
    return make_dataset(base, nets['net50'], name_filter='confident', label='t')


@pytest.fixture(scope='module')
def linear(ds):
    return MarginalModel.from_dataset(ds, height_form='linear',
                                      sigma_link_fixed=0.5, n_quad=N_QUAD)


def test_the_layout_matches_the_reference_grouping(linear):
    """Different row ordering would compare two different integrals."""
    lay = build_layout(linear.obs_u, linear.multi_obs, n_quad=N_QUAD)
    assert np.array_equal(lay.obs, linear.multi_obs)
    assert np.array_equal(lay.seg, linear.multi_seg)
    assert lay.n_users == linear.n_multi_users


def test_the_quadrature_weights_are_normalised(linear):
    """exp(gh_logw) must sum to 1, not to sqrt(2*pi).

    This is the bug the module docstring describes, pinned directly: it is a
    constant per climber, so it passes every within-fit check and corrupts
    every between-model one.
    """
    lay = build_layout(linear.obs_u, linear.multi_obs, n_quad=N_QUAD)
    assert abs(np.exp(lay.gh_logw).sum() - 1.0) < 1e-12


@pytest.mark.parametrize('seed', SEEDS)
def test_the_per_climber_integral_matches_numpy(linear, seed):
    lay = build_layout(linear.obs_u, linear.multi_obs, n_quad=N_QUAD)
    m_, c_, nu_ = pt.dvector('m'), pt.dvector('c'), pt.dvector('nu')
    sl_, su_ = pt.dscalar('sl'), pt.dscalar('su')
    f = pytensor.function([m_, c_, nu_, sl_, su_],
                          multi_log_integral_pt(m_, c_, nu_, sl_, su_, lay))
    theta = linear.initial_point(np.random.default_rng(seed))
    c, nu, sl, su = linear.pieces(theta)
    ref = linear.multi_log_integral(c, nu, sl, su)
    o = lay.obs
    got = f(linear.m[o], c[o], nu[o], sl, su)
    assert np.abs(got - ref).max() < 1e-6


def _pymc_point(mm, ip, M):
    """Map a NumPy parameter vector onto PyMC's value variables."""
    def convert(theta):
        p = mm.unpack(theta)
        z = zerosum_coords(M, p['gym_raw'])
        out = {}
        for k in ip:
            b = k.split('_log__')[0].split('_zerosum__')[0]
            if b == 'beta0':
                v = p['beta0']
            elif b == 'sigma_user':
                v = p['log_sigma_user']
            elif b == 'sigma_gym':
                v = p['log_sigma_gym']
            elif b == 'log_lambda0':
                v = p['log_lambda0']
            elif b == 'kappa':
                v = p['kappa']
            elif b == 'rho':
                v = p['rho']
            elif b == 'gym_correction_raw':
                v = z
            elif b in mm.Xnames:
                v = p['beta'][mm.Xnames.index(b)]
            else:
                raise AssertionError(f'unmapped variable {k!r}')
            out[k] = np.array(v)
        return out
    return convert


@pytest.mark.parametrize('height_form', ['linear', 'quadratic_x_gender'])
def test_the_whole_model_likelihood_matches_numpy(ds, height_form):
    """The check that matters: assembled PyMC model vs the reference."""
    mm = MarginalModel.from_dataset(ds, height_form=height_form,
                                    sigma_link_fixed=0.5, n_quad=N_QUAD)
    model = build_model_v2(ds, height_form=height_form, gender_mode='point',
                           estimate_sigma_link=False, sigma_link_fixed=0.5,
                           marginalize_all=True, n_quad=N_QUAD)
    ip = model.initial_point()
    zk = next(k for k in ip if k.startswith('gym_correction_raw'))
    M = zerosum_basis_matrix(model, zk, int(np.asarray(ip[zk]).shape[0]))
    lik = model.compile_fn(
        model.logp(vars=model.observed_RVs + model.potentials, sum=True),
        inputs=model.value_vars, on_unused_input='ignore')
    convert = _pymc_point(mm, ip, M)
    for seed in SEEDS:
        theta = mm.initial_point(np.random.default_rng(seed))
        assert abs(mm.log_likelihood(theta) - float(lik(convert(theta)))) < 1e-3


def test_no_climber_offset_is_sampled(ds):
    """The point of the exercise: 4,241 parameters become 40."""
    model = build_model_v2(ds, height_form='linear', gender_mode='point',
                           estimate_sigma_link=False, sigma_link_fixed=0.5,
                           marginalize_all=True, n_quad=N_QUAD)
    names = {v.name for v in model.free_RVs}
    assert 'epsilon' not in names and 'epsilon_raw' not in names
    n = sum(np.asarray(v).size for v in model.initial_point().values())
    assert n == 40, f'expected 40 sampled elements, got {n}'


def test_sigma_user_is_still_estimated(ds):
    """Integrating the offsets out must not remove the scale they shared.

    sigma_user is what the whole model is for -- it is the spread of climber
    ability. It survives because it is still in the integrand.
    """
    model = build_model_v2(ds, height_form='linear', gender_mode='point',
                           estimate_sigma_link=False, sigma_link_fixed=0.5,
                           marginalize_all=True, n_quad=N_QUAD)
    assert 'sigma_user' in {v.name for v in model.free_RVs}
