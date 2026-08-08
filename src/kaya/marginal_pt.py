"""Integrating out EVERY climber offset, in PyTensor.

`build_model_v2(marginalize_singles=True)` integrates out the offsets of the
6,156 climbers with a single observation, in closed form, and samples the
remaining 4,201. `marginal_v2` -- the NumPy reference that emcee and the nested
samplers call -- integrates out all 10,357, the multi-observation ones by
adaptive Gauss-Hermite quadrature. This module is the PyTensor port of that
quadrature, so PyMC can do the same thing and differentiate through it.

Why bother, when the half-marginalized model already fits:

  * **It is the funnel.** Measured on the v7 sweep, corr(log sigma_user, spread
    of epsilon_raw) = -0.847. The likelihood:prior information ratio is 21-64x
    per climber, so the data pins each epsilon tightly and the non-centered
    coordinate epsilon/sigma has to slide against sigma to compensate. NUTS
    responded by shrinking its step size to 0.003, and then 100% of iterations
    ran out of tree depth mid-trajectory. Removing the 4,201 offsets removes
    the geometry that causes it, rather than re-coordinatising around it.
  * **4,241 parameters become 40.** Traces go from ~1 GB to ~30 MB, and the
    35-fit Phase 4 cross-validation becomes a different size of job.

The algorithm, following `MarginalModel.multi_log_integral`:

    p(m_u) = INT prod_i ExGauss(m_i | eps) * phi(eps; 0, sigma_user) d eps

Plain Gauss-Hermite spreads nodes over the PRIOR, which is the wrong scale --
a climber with k observations has an integrand about 1/sqrt(k) as wide, so most
nodes land where the integrand is negligible. So each climber's peak is located
first by Newton's method on a provably concave function, and the nodes are
placed around it at the width the Laplace approximation implies. That is
standard adaptive Gauss-Hermite, the same thing lme4 does for `nAGQ > 1`, and
it converges at ~15 nodes instead of ~161.

Newton runs a fixed number of unrolled iterations with no safeguard, exactly as
the NumPy version does, and gradients propagate through the node placement --
which is correct, because NUTS needs the gradient of the approximation it is
actually evaluating, not of the exact integral.

Correctness is not asserted here, it is checked: `tests/test_marginal_pt.py`
compares this against `marginal_v2` on the real dataset.
"""
from __future__ import annotations

from typing import NamedTuple

import numpy as np
import pytensor.tensor as pt
from numpy.polynomial.hermite_e import hermegauss
from pymc.distributions.dist_math import normal_lcdf

# Below this ratio the ExGaussian's three middle terms are individually
# infinite and cancel exactly, giving inf - inf. The exponential gap has
# vanished by then, so the density is simply the Normal's. Same constant as
# marginal_v2.exgaussian_logpdf -- they must switch at the same place or the
# two implementations disagree in the tail.
NORMAL_LIMIT = 0.05

_LOG_2PI = float(np.log(2.0 * np.pi))


def _log_ndtr(z):
    """log Phi(z), stable in the left tail.

    PyMC's own ExGaussian uses `normal_lcdf`, so using it here keeps this
    quadrature numerically consistent with the `m_single` node built alongside
    it. Plain log(Phi(z)) underflows to -inf deep in the left tail and silently
    kills parameter regions the sampler has to walk through.
    """
    return normal_lcdf(0.0, 1.0, z)


def exgaussian_logpdf_pt(x, mu, sigma, nu):
    """PyTensor twin of marginal_v2.exgaussian_logpdf."""
    z = (x - mu) / sigma - sigma / nu
    ex = -pt.log(nu) + (mu - x) / nu + 0.5 * (sigma / nu) ** 2 + _log_ndtr(z)
    normal = -0.5 * ((x - mu) / sigma) ** 2 - pt.log(sigma) - 0.5 * _LOG_2PI
    return pt.switch(pt.gt(nu, NORMAL_LIMIT * sigma), ex, normal)


class MultiLayout(NamedTuple):
    """The fixed, data-only bookkeeping the quadrature needs.

    Built once in NumPy. `obs` is ordered so each climber's rows are
    contiguous, which is not required by the scatter below but keeps the
    memory access pattern sane and matches the NumPy reference's layout.
    """
    obs: np.ndarray          # (n_multi,) observation indices, grouped by user
    seg: np.ndarray          # (n_multi,) 0..n_users-1, which climber each row is
    n_users: int
    gh_z: np.ndarray         # (Q,) Gauss-Hermite nodes
    gh_logw: np.ndarray      # (Q,) log of the Gauss-Hermite weights


def build_layout(obs_u: np.ndarray, multi_obs: np.ndarray,
                 n_quad: int = 31) -> MultiLayout:
    """Group multi-observation rows by climber and build the quadrature rule."""
    order = np.argsort(obs_u[multi_obs], kind='stable')
    obs = np.asarray(multi_obs)[order]
    users = obs_u[obs]
    uniq, seg = np.unique(users, return_inverse=True)
    # hermegauss weights carry exp(-z^2/2) and sum to sqrt(2pi), not to 1. The
    # density being integrated against is the NORMALISED Gaussian, so the
    # 1/sqrt(2pi) has to come out of the weights -- exactly as marginal_v2
    # does. Omitting it costs a constant 0.5*log(2pi) per climber, which is
    # invisible in any single fit and shifts every model comparison.
    z, w = hermegauss(n_quad)
    gh_logw = np.log(w) - 0.5 * _LOG_2PI
    return MultiLayout(obs=obs, seg=seg.astype('int64'), n_users=int(uniq.size),
                       gh_z=z, gh_logw=gh_logw)


def _laplace(m, c, nu, sigma_link, sigma_user, seg, n_users, iters=4):
    """Peak and width of each climber's integrand, by Newton's method.

        h(eps) = sum_i log ExGauss(m_i | eps) - eps^2 / (2 sigma_user^2)

    With z_i = (c_i + eps - m_i)/sigma_link - sigma_link/nu_i and the inverse
    Mills ratio lam(z) = phi(z)/Phi(z):

        h'  = sum_i [ lam(z_i)/sigma_link - 1/nu_i ] - eps/sigma_user^2
        h'' = sum_i [ -lam(z_i)(z_i + lam(z_i)) ] / sigma_link^2 - 1/sigma_user^2

    h'' is strictly negative -- lam(z)(z + lam(z)) is the variance of a
    truncated normal up to sign -- so h is concave and Newton converges from
    eps = 0 in a handful of steps. No safeguard loop, matching the reference:
    if that concavity ever failed the right answer is to find out loudly.
    """
    sl, su = sigma_link, sigma_user
    eps = pt.zeros((n_users,))
    small = pt.le(nu, NORMAL_LIMIT * sl)
    g2 = None
    for _ in range(iters):
        e_obs = eps[seg]
        z = (c + e_obs - m) / sl - sl / nu
        lam = pt.exp(-0.5 * z ** 2 - 0.5 * _LOG_2PI - _log_ndtr(z))
        # The same Normal-limit switch as the density. Getting it wrong would
        # not corrupt the integral -- the density switches independently -- but
        # it would put the nodes in the wrong place, which comes to the same
        # thing.
        d1 = pt.switch(small, -(c + e_obs - m) / sl ** 2, lam / sl - 1.0 / nu)
        d2 = pt.switch(small, -1.0 / sl ** 2, -lam * (z + lam) / sl ** 2)
        g1 = pt.inc_subtensor(pt.zeros((n_users,))[seg], d1) - eps / su ** 2
        g2 = pt.inc_subtensor(pt.zeros((n_users,))[seg], d2) - 1.0 / su ** 2
        eps = eps - g1 / g2
    return eps, 1.0 / pt.sqrt(-g2)


def multi_log_integral_pt(m, c, nu, sigma_link, sigma_user, layout: MultiLayout,
                          newton_iters: int = 4):
    """log p(observations of climber u), one entry per multi-observation climber.

    `m`, `c` and `nu` must already be restricted to `layout.obs` and in its
    order. Returns a vector of length `layout.n_users`.
    """
    seg = pt.as_tensor_variable(layout.seg)
    gh_z = pt.as_tensor_variable(layout.gh_z)
    gh_logw = pt.as_tensor_variable(layout.gh_logw)
    n_users = layout.n_users
    sl, su = sigma_link, sigma_user

    mode, scale = _laplace(m, c, nu, sl, su, seg, n_users, iters=newton_iters)

    # eps_k = mode_u + scale_u * z_k, one column per climber: (Q, U)
    eps = mode[None, :] + scale[None, :] * gh_z[:, None]
    eps_obs = eps[:, seg]                                       # (Q, n_multi)

    ll = exgaussian_logpdf_pt(-m[None, :], -(c[None, :] + eps_obs), sl,
                              nu[None, :])
    # Sum within climber. inc_subtensor accumulates on repeated indices, which
    # is exactly the segment sum np.bincount does in the reference.
    per_user = pt.inc_subtensor(pt.zeros((gh_z.shape[0], n_users))[:, seg], ll)

    # Change of variables. The nodes carry the Hermite weight exp(-z^2/2),
    # which is not the density being integrated against, so divide it back out
    # (+z^2/2), multiply in the actual Gaussian prior on eps, and include the
    # Jacobian `scale` of eps = mode + scale * z.
    logint = (per_user
              - 0.5 * (eps / su) ** 2 - pt.log(su)
              + gh_logw[:, None] + 0.5 * gh_z[:, None] ** 2
              + pt.log(scale)[None, :])
    return pt.logsumexp(logint, axis=0)
