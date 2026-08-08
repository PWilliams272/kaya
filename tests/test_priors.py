"""The prior, which is the half of the model that has already been wrong once.

`check_pymc_marginal.py` compares `model.logp(vars=observed_RVs)` -- the
LIKELIHOOD. That is why the original zero-sum bug survived: sampling n-1 gym
offsets and setting the last to minus their sum gives the last gym a prior 28
times wider than every other, but it still hands the likelihood a valid
zero-sum vector. The two implementations agreed to 1e-9 on the likelihood while
fitting different models, and nothing in the suite would have noticed.

Covered here:

  * the zero-sum basis helpers, which are what let anything compare the two
    implementations on the gym block at all, and
  * the real prior comparison, parameter by parameter, when the fit data is on
    disk (it is gitignored, so a fresh clone skips it).
"""
import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

pm = pytest.importorskip('pymc')

from kaya.grading_model_v2 import zerosum_basis_matrix, zerosum_coords  # noqa: E402

N_CATEGORIES = 6


@pytest.fixture(scope='module')
def zs():
    """A bare ZeroSumNormal, and the recovered matrix for its basis."""
    with pm.Model() as model:
        pm.ZeroSumNormal('gym_correction_raw', sigma=1, shape=N_CATEGORIES)
    key = next(k for k in model.initial_point()
               if k.startswith('gym_correction_raw'))
    n_free = int(np.asarray(model.initial_point()[key]).shape[0])
    return model, key, n_free, zerosum_basis_matrix(model, key, n_free)


def test_zerosum_stores_one_fewer_coordinate_than_categories(zs):
    _, _, n_free, m = zs
    assert n_free == N_CATEGORIES - 1
    assert m.shape == (N_CATEGORIES, N_CATEGORIES - 1)


def test_every_basis_vector_sums_to_zero(zs):
    """If this fails the recovered matrix is not the transform we think it is."""
    _, _, _, m = zs
    assert np.abs(m.sum(axis=0)).max() < 1e-9


def test_coords_round_trip(zs):
    """coords(basis, basis @ z) == z, which is the whole contract."""
    _, _, n_free, m = zs
    rng = np.random.default_rng(0)
    for _ in range(5):
        z = rng.standard_normal(n_free)
        assert np.allclose(zerosum_coords(m, m @ z), z, atol=1e-9)


def test_a_non_zero_sum_target_is_rejected(zs):
    """Silently projecting it would hand the model a different configuration.

    This is the failure the helper exists to make loud: least squares happily
    returns the nearest point in the span, so without the residual check a
    caller would get plausible coordinates for the wrong gym vector.
    """
    _, _, _, m = zs
    target = np.ones(N_CATEGORIES)          # sums to 6, not 0
    with pytest.raises(ValueError, match='not in the zero-sum span'):
        zerosum_coords(m, target)


def test_slicing_the_vector_is_not_the_same_as_mapping_it(zs):
    """Guards the specific shortcut that used to be in check_pymc_marginal.py.

    Taking the first n-1 elements of a zero-sum vector as if they were the
    ZeroSumNormal's coordinates gives a DIFFERENT configuration -- valid, but
    not the one asked for. If this ever starts passing, the two bases have
    coincidentally aligned and the test is no longer testing anything.
    """
    _, _, n_free, m = zs
    rng = np.random.default_rng(1)
    target = m @ rng.standard_normal(n_free)
    sliced = np.asarray(target)[:n_free]
    assert not np.allclose(m @ sliced, target, atol=1e-6)


# --- the real comparison, against the fitted dataset ------------------------

DATA = ROOT / 'runs' / 'base_bouldering.pkl'


@pytest.mark.skipif(not DATA.exists(),
                    reason='runs/base_bouldering.pkl is gitignored; '
                           'run the pull to enable the full prior check')
@pytest.mark.parametrize('height_form', ['linear', 'quadratic_x_gender'])
def test_the_two_implementations_agree_on_the_prior(height_form):
    """Vary each parameter alone; the two log-priors must differ by a CONSTANT.

    A constant is the epsilon_raw prior PyMC carries and NumPy integrates out.
    Variation across the grid is a different prior shape, i.e. a different
    model, and it would show up nowhere else.
    """
    check_priors = pytest.importorskip('check_priors')

    import contextlib
    import io
    argv = sys.argv
    sys.argv = ['check_priors', '--height-form', height_form, '--points', '5']
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            status = check_priors.main()
    finally:
        sys.argv = argv
    assert status == 0, buf.getvalue()
