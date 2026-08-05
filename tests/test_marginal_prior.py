"""The NumPy reference model's priors must match the PyTensor model's.

`check_pymc_marginal.py` compares the two implementations' *likelihoods* and
they agree to 1e-9. That check passed for weeks while the two were fitting
different models, because the difference was entirely in the prior: the
reference implementation built its zero-sum gym corrections by sampling n-1
values and setting the last to minus their sum, which gives that last gym a
prior variance of n-1 instead of (n-1)/n. On 29 gyms, whichever gym sorted
last carried a prior 28 times wider than every other one.

A likelihood check cannot see this -- both schemes hand the likelihood a valid
zero-sum vector. These tests look at the prior directly.
"""
import numpy as np
import pytest

from kaya.marginal_v2 import zero_sum_basis

N_GYMS = 29


def test_basis_is_orthonormal():
    q = zero_sum_basis(N_GYMS)
    assert q.shape == (N_GYMS, N_GYMS - 1)
    np.testing.assert_allclose(q.T @ q, np.eye(N_GYMS - 1), atol=1e-12)


def test_basis_spans_the_zero_sum_hyperplane():
    q = zero_sum_basis(N_GYMS)
    # Every column sums to zero, so any combination of them does too.
    np.testing.assert_allclose(q.sum(axis=0), 0.0, atol=1e-12)
    z = np.random.default_rng(0).standard_normal((500, N_GYMS - 1))
    np.testing.assert_allclose((z @ q.T).sum(axis=1), 0.0, atol=1e-10)


def test_every_category_gets_the_same_prior_variance():
    """The bug this file exists for: one category 28x wider than the rest."""
    q = zero_sum_basis(N_GYMS)
    z = np.random.default_rng(1).standard_normal((400_000, N_GYMS - 1))
    var = (z @ q.T).var(axis=0)
    expected = (N_GYMS - 1) / N_GYMS          # ZeroSumNormal(sigma=1)
    np.testing.assert_allclose(var, expected, rtol=0.02)
    # Exchangeability is the property that actually broke; state it directly.
    assert var.max() / var.min() < 1.1, (
        f'gym prior variances are not exchangeable: {var.min():.3f} to '
        f'{var.max():.3f}. Sampling n-1 values and appending minus their sum '
        f'produces exactly this, and it is not ZeroSumNormal.')


def test_append_minus_sum_is_rejected_by_the_same_check():
    """Guard the guard: the broken scheme must fail the test above."""
    z = np.random.default_rng(2).standard_normal((200_000, N_GYMS - 1))
    broken = np.concatenate([z, -z.sum(axis=1, keepdims=True)], axis=1)
    var = broken.var(axis=0)
    assert var.max() / var.min() > 20, 'the broken scheme should be caught'


@pytest.mark.parametrize('n', [3, 7, 29, 50])
def test_matches_pymc_zerosumnormal(n):
    """Same marginal variance as PyMC's own implementation, at several sizes."""
    pm = pytest.importorskip('pymc')
    q = zero_sum_basis(n)
    z = np.random.default_rng(3).standard_normal((60_000, n - 1))
    ours = (z @ q.T).var(axis=0).mean()
    with pm.Model():
        theirs = pm.draw(pm.ZeroSumNormal('x', sigma=1, shape=n),
                         draws=60_000, random_seed=4).var(axis=0).mean()
    assert abs(ours - theirs) < 0.02, f'{ours:.4f} vs PyMC {theirs:.4f}'
