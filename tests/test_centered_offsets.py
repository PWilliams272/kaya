"""Centered and non-centered climber offsets must be the same model.

Which one samples better is an empirical question about the data -- the
likelihood:prior information ratio is 21-64x per climber here, which is the
regime where centered wins. But whichever is chosen, the posterior must be
identical, and that is what these tests pin.

The check is a change of variables. With eps = sigma * z,

    log p_centered(eps) = log p_noncentered(z) - n * log(sigma)

so the two log-densities differ by exactly the Jacobian and by nothing else. A
difference that is not the Jacobian means the reparameterization changed the
model, which would make every fit before and after it incomparable.
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

from kaya.grading_model_v2 import build_model_v2, make_dataset  # noqa: E402
from kaya.viewer_paths import data_file  # noqa: E402

DATA = ROOT / 'runs' / 'base_bouldering.pkl'
pytestmark = pytest.mark.skipif(
    not DATA.exists(),
    reason='runs/base_bouldering.pkl is gitignored; run the pull to enable')

KW = dict(height_form='linear', gender_mode='point', estimate_sigma_link=False,
          sigma_link_fixed=0.5, marginalize_singles=True)


@pytest.fixture(scope='module')
def models():
    with open(data_file('base_bouldering.pkl'), 'rb') as f:
        base = pickle.load(f)
    nets = json.loads(data_file('networks.json').read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident', label='t')
    return (build_model_v2(ds, center_user_offsets=True, **KW),
            build_model_v2(ds, center_user_offsets=False, **KW))


def _matched_points(mc, mn, scale=0.3, seed=0):
    """One point in each parameterization describing the same configuration."""
    rng = np.random.default_rng(seed)
    ipn = mn.initial_point()
    kz = next(k for k in ipn if k.startswith('epsilon_raw'))
    z = rng.standard_normal(np.asarray(ipn[kz]).shape) * scale
    ipn[kz] = z
    sigma = float(np.exp(ipn[next(k for k in ipn if k.startswith('sigma_user'))]))
    ipc = mc.initial_point()
    for k in ipc:
        if not k.startswith('epsilon'):
            src = next(kk for kk in ipn
                       if kk.split('_log__')[0] == k.split('_log__')[0])
            ipc[k] = ipn[src]
    ipc[next(k for k in ipc if k.startswith('epsilon'))] = sigma * z
    return ipc, ipn, sigma, z.size


def test_the_centered_model_samples_epsilon_not_epsilon_raw(models):
    mc, mn = models
    assert 'epsilon' in {v.name for v in mc.free_RVs}
    assert 'epsilon_raw' not in {v.name for v in mc.free_RVs}
    assert 'epsilon_raw' in {v.name for v in mn.free_RVs}


def test_both_parameterizations_sample_the_same_number_of_things(models):
    """A reparameterization moves coordinates around; it does not add any."""
    mc, mn = models
    assert len(mc.free_RVs) == len(mn.free_RVs)
    assert ({v.name for v in mc.free_RVs} ^ {v.name for v in mn.free_RVs}
            == {'epsilon', 'epsilon_raw'})


@pytest.mark.parametrize('seed', [0, 1, 2])
def test_the_log_densities_differ_by_exactly_the_jacobian(models, seed):
    """The whole contract. Any residual here is a changed model."""
    mc, mn = models
    ipc, ipn, sigma, n = _matched_points(mc, mn, seed=seed)
    lc = float(mc.compile_logp()(ipc))
    ln = float(mn.compile_logp()(ipn))
    expected = -n * np.log(sigma)
    assert abs((lc - ln) - expected) < 1e-6, (
        f'difference {lc - ln:.6f} is not the Jacobian {expected:.6f}')


def test_the_jacobian_actually_depends_on_sigma(models):
    """Guards the degenerate pass: if sigma were 1 the Jacobian would vanish.

    Then the test above would hold for a broken implementation too, so confirm
    the correction is really being exercised.
    """
    mc, mn = models
    _, _, sigma, n = _matched_points(mc, mn)
    assert abs(sigma - 1.0) > 0.1, 'sigma too close to 1 to test anything'
    assert abs(-n * np.log(sigma)) > 100
