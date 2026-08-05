"""The v2 grading model with each climber's ability offset integrated out.

Why
---
`build_model_v2` gives every climber their own latent ability offset
`epsilon_u`. On net50/confident that is 10,357 parameters against 20,014
observations -- one unknown per two data points -- and 59% of climbers
contribute exactly one observation, so their offset can absorb that
observation completely. The consequences are measurable:

  * leave-one-out cross-validation reports 8,400 effective parameters and
    flags 17% of rows as unreliable,
  * two fits of the *identical* model, differing only in random seed, score
    31.1 elpd apart -- larger than every difference between the height forms
    the comparison is supposed to resolve,
  * and scoring only climbers with three or more observations collapses that
    same run-to-run gap to 0.4, which identifies the single-observation rows
    as the cause rather than merely correlating with it.

The offsets are not droppable: gym corrections are identified by the same
climber appearing at two gyms. But they can be *integrated out*, and for most
of the data that integral is exact.

The likelihood
--------------
For observation i belonging to climber u,

    m_i = ceiling_i - Exponential(mean = nu_i) + Normal(0, sigma_link)
    ceiling_i = c_i + epsilon_u

where c_i collects everything that does not depend on the climber's own
offset (intercept, body covariates, gym correction) and epsilon_u ~
Normal(0, sigma_user). Equivalently -m_i is ExGaussian(-ceiling_i,
sigma_link, nu_i), which is how `build_model_v2` writes it.

One observation -- exact, no approximation
    epsilon_u enters additively and is Gaussian, and the observation noise is
    Gaussian, so the two convolve into a single wider Gaussian:

        -m ~ ExGaussian(mu = -c, sigma = sqrt(sigma_link^2 + sigma_user^2), nu)

    Same distribution family, one changed argument. 6,156 parameters vanish
    with nothing given up.

Several observations -- one-dimensional adaptive quadrature
    The observations share a single scalar epsilon_u, so their joint marginal
    is a one-dimensional integral against a Gaussian weight:

        p(m_u) = INT prod_i ExGauss(m_i | eps) * phi(eps; 0, sigma_user) d eps

    Plain Gauss-Hermite quadrature spreads its nodes over the PRIOR, and that
    is the wrong scale: a climber with k observations has an integrand roughly
    1/sqrt(k) as wide, so at k = 21 the peak is five times narrower than the
    node spacing and most nodes land where the integrand is negligible.
    Measured, that needs ~161 nodes to converge (466 ms per likelihood
    evaluation), which is too slow to sample with.

    So locate each climber's peak first -- three Newton steps on a concave
    function, using the analytic first and second derivatives below -- and
    place the nodes around it at the width the Laplace approximation implies.
    Standard adaptive Gauss-Hermite quadrature, the same thing lme4 does for
    `nAGQ > 1`. It converges at ~15 nodes because the nodes now sit where the
    mass is.

Deliberately plain NumPy: this module is the reference implementation. The
PyTensor version used by PyMC must agree with it, emcee and the nested
samplers call it directly, and its single-observation branch can be checked
against a quadrature evaluation of the same quantity to machine precision.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import numpy as np
from numpy.polynomial.hermite_e import hermegauss
from scipy.special import log_ndtr, logsumexp

from kaya.grading_model_v2 import DatasetV2, _design_columns

__all__ = ['MarginalModel', 'exgaussian_logpdf']


def exgaussian_logpdf(x, mu, sigma, nu):
    """log density of Normal(mu, sigma) + Exponential(mean=nu).

    Two numerical hazards, both of which bite here:

    `log_ndtr` rather than log(ndtr(.)) -- the argument runs deep into the
    left tail, where ndtr underflows to 0 and the log becomes -inf, silently
    killing whole parameter regions a sampler needs to walk through.

    And a switch to the Normal limit when nu is small relative to sigma. The
    three middle terms are individually +/-inf there and cancel exactly, so
    evaluating them literally gives inf - inf = nan. Since the exponential
    gap vanishes as nu -> 0, the density is simply Normal(mu, sigma) in that
    regime. NUTS never wandered far enough to hit this, but nested sampling
    starts from the whole prior and does, immediately.
    """
    z = (x - mu) / sigma - sigma / nu
    ex = -np.log(nu) + (mu - x) / nu + 0.5 * (sigma / nu) ** 2 + log_ndtr(z)
    normal = -0.5 * ((x - mu) / sigma) ** 2 - np.log(sigma) - 0.5 * np.log(2 * np.pi)
    return np.where(nu > 0.05 * sigma, ex, normal)


def prepare_design(dataset: DatasetV2, *, height_form='linear',
                   ape_quadratic=True, ape_x_gender=False, consts=None):
    """Build the design matrix and per-observation vectors from a dataset.

    Every centring and scaling here is copied from `build_model_v2`
    deliberately rather than re-derived: the two must produce the same numbers
    for the cross-check between implementations to mean anything.

    `consts` is the point of the split. Passing None derives the scaling from
    this dataset and returns it. Passing a previously returned dict applies
    *those* constants instead, which is what grouped cross-validation needs:
    held-out climbers have to be mapped through the training set's medians and
    standard deviations. Letting them set their own scale would leak the test
    set into the fit through the back door, quietly and without any error.
    """
    obs, users = dataset.observations, dataset.users
    user_ids = users.index.tolist()
    uidx = {u: i for i, u in enumerate(user_ids)}
    gym_ids = sorted(obs['gym_id'].unique())
    gidx = {g: i for i, g in enumerate(gym_ids)}

    obs_u = obs['user_id'].map(uidx).to_numpy()
    obs_g = obs['gym_id'].map(gidx).to_numpy()
    m = obs['m'].to_numpy(float)

    height = users['height'].to_numpy(float)
    ape = users['ape_index'].to_numpy(float)
    nsps = users['n_sends_per_sesh'].to_numpy(float)
    nv_raw = obs['n_visits'].to_numpy(float)

    own = consts is None
    c = {} if own else dict(consts)
    if own:
        c['nv_scale'] = float(np.nanmedian(nv_raw)) or 1.0
        c['h_sd'] = float(np.nanstd(height)) or 1.0
        c['a_sd'] = float(np.nanstd(ape)) or 1.0
        c['h_med'] = float(np.nanmedian(height))
        c['a_med'] = float(np.nanmedian(ape))
        c['r_scale'] = float(np.nanmedian(nsps))

    n_visits = nv_raw / c['nv_scale'] - 1.0
    h_c = np.nan_to_num((height - c['h_med']) / c['h_sd'], nan=0.0)
    a_c = np.nan_to_num((ape - c['a_med']) / c['a_sd'], nan=0.0)
    h_miss = np.isnan(height).astype(float)
    a_miss = np.isnan(ape).astype(float)
    w_female = users['w_female'].fillna(0.5).to_numpy(float)

    Xcols, Xnames = _design_columns(height_form, h_c, a_c, w_female,
                                    ape_quadratic, ape_x_gender)
    Xcols = np.column_stack([Xcols, h_miss, a_miss])
    Xnames = list(Xnames) + ['beta_h_missing', 'beta_a_missing']
    if own:
        c['X_mean'] = Xcols.mean(axis=0)
    Xc = Xcols - np.asarray(c['X_mean'])

    r_user = ((nsps / c['r_scale'] - 1.0) if c['r_scale']
              else np.zeros_like(nsps))
    r_obs = np.nan_to_num(r_user[obs_u], nan=0.0)

    return {'Xc': Xc, 'Xnames': Xnames, 'obs_u': obs_u, 'obs_g': obs_g,
            'm': m, 'n_visits': n_visits, 'r_obs': r_obs,
            'user_ids': user_ids, 'gym_ids': gym_ids,
            'w_female': w_female, 'consts': c}


@dataclass
class MarginalModel:
    """Everything the marginal likelihood needs, precomputed from a dataset.

    Construct with `MarginalModel.from_dataset(...)`. `log_likelihood(theta)`
    then takes a flat parameter vector -- the ordering is `param_names` -- so
    it can be handed to emcee, dynesty, or a bridge-sampling routine without
    any of them knowing about the model.
    """
    # design
    Xc: np.ndarray            # (n_users, n_coef) centred covariates
    Xnames: list[str]
    obs_u: np.ndarray         # (n_obs,) climber index per observation
    obs_g: np.ndarray         # (n_obs,) gym index per observation
    m: np.ndarray             # (n_obs,) hardest grade
    n_visits: np.ndarray      # (n_obs,) centred, scaled
    r_obs: np.ndarray         # (n_obs,) centred, scaled sends-per-session
    n_users: int
    n_gyms: int
    median_m: float
    sigma_link_fixed: float

    # climber grouping: singles get the closed form, the rest get quadrature
    single_obs: np.ndarray    # (n_single,) observation indices
    multi_obs: np.ndarray     # (n_multi,) observation indices, grouped by user
    multi_seg: np.ndarray     # (n_multi,) 0..n_multi_users-1 segment id
    n_multi_users: int

    # quadrature
    gh_z: np.ndarray
    gh_logw: np.ndarray

    param_names: list[str] = field(default_factory=list)
    prior_sd: dict = field(default_factory=dict)
    label: str = ''

    PRIOR_SD = {'beta_gender': 2.0, 'gamma1': 1.0, 'gamma2': 0.3,
                'gamma1_x': 0.5, 'gamma2_x': 0.15, 'delta1': 1.0,
                'delta2': 0.3, 'delta1_x': 0.5, 'delta2_x': 0.15,
                'beta_h_missing': 1.0, 'beta_a_missing': 1.0}

    # ---- construction -------------------------------------------------

    @classmethod
    def from_dataset(cls, dataset: DatasetV2, *, height_form='linear',
                     ape_quadratic=True, ape_x_gender=False,
                     sigma_link_fixed=0.5, n_quad=21):
        """Mirror build_model_v2's data preparation exactly.

        Every scaling and centring below is copied from `build_model_v2`
        deliberately, not re-derived: the two must produce the same numbers
        for the cross-check to mean anything.
        """
        d = prepare_design(dataset, height_form=height_form,
                           ape_quadratic=ape_quadratic,
                           ape_x_gender=ape_x_gender)
        Xc, Xnames = d['Xc'], d['Xnames']
        obs_u, obs_g, m = d['obs_u'], d['obs_g'], d['m']
        n_visits, r_obs = d['n_visits'], d['r_obs']
        user_ids, gym_ids = d['user_ids'], d['gym_ids']

        # Split climbers by how many observations they have. Sorting the
        # multi-observation rows by climber turns "group by user" into a
        # contiguous segment scan, which is what makes the quadrature a couple
        # of array operations instead of a Python loop over 4,201 climbers.
        counts = np.bincount(obs_u, minlength=len(user_ids))
        is_single = counts[obs_u] == 1
        single_obs = np.flatnonzero(is_single)
        multi_obs = np.flatnonzero(~is_single)
        order = np.argsort(obs_u[multi_obs], kind='stable')
        multi_obs = multi_obs[order]
        _, multi_seg = np.unique(obs_u[multi_obs], return_inverse=True)

        z, w = hermegauss(n_quad)   # weight exp(-z^2/2), weights sum to sqrt(2pi)
        gh_logw = np.log(w) - 0.5 * np.log(2 * np.pi)

        param_names = (['beta0', 'log_sigma_user'] + Xnames
                       + ['log_sigma_gym']
                       + [f'gym_raw[{i}]' for i in range(len(gym_ids) - 1)]
                       + ['log_lambda0', 'kappa', 'rho'])

        return cls(
            Xc=Xc, Xnames=Xnames, obs_u=obs_u, obs_g=obs_g, m=m,
            n_visits=n_visits, r_obs=r_obs,
            n_users=len(user_ids), n_gyms=len(gym_ids),
            median_m=float(np.nanmedian(m)), sigma_link_fixed=sigma_link_fixed,
            single_obs=single_obs, multi_obs=multi_obs, multi_seg=multi_seg,
            n_multi_users=int(multi_seg.max()) + 1 if len(multi_seg) else 0,
            gh_z=z, gh_logw=gh_logw,
            param_names=param_names,
            prior_sd={nm: cls.PRIOR_SD.get(nm, 1.0) for nm in Xnames},
            label=dataset.label,
        )

    @property
    def n_params(self):
        return len(self.param_names)

    # ---- parameter packing --------------------------------------------

    def unpack(self, theta):
        """Flat vector -> named pieces.

        sigma_user and sigma_gym are sampled on the log scale so the vector is
        unconstrained everywhere, which is what emcee and a Gaussian proposal
        want. The Jacobian is accounted for in `log_prior`.

        Gym corrections carry a zero-sum constraint (the model is identified
        only up to an additive shift between climber ability and gym
        correction), so n_gyms - 1 free values are sampled and the last is set
        to make the sum zero.
        """
        theta = np.asarray(theta, float)
        i = 0
        beta0 = theta[i]; i += 1
        log_sigma_user = theta[i]; i += 1
        nb = len(self.Xnames)
        beta = theta[i:i + nb]; i += nb
        log_sigma_gym = theta[i]; i += 1
        ng = self.n_gyms - 1
        gym_free = theta[i:i + ng]; i += ng
        log_lambda0 = theta[i]; i += 1
        kappa = theta[i]; i += 1
        rho = theta[i]; i += 1
        gym_raw = np.concatenate([gym_free, [-gym_free.sum()]])
        return dict(beta0=beta0, sigma_user=np.exp(log_sigma_user),
                    log_sigma_user=log_sigma_user, beta=beta,
                    sigma_gym=np.exp(log_sigma_gym), log_sigma_gym=log_sigma_gym,
                    gym_raw=gym_raw, log_lambda0=log_lambda0,
                    kappa=kappa, rho=rho)

    # ---- the likelihood -----------------------------------------------

    def log_likelihood(self, theta):
        p = self.unpack(theta)
        sl, su = self.sigma_link_fixed, p['sigma_user']

        # c_i: the ceiling with the climber's own offset left out.
        user_term = p['beta0'] + self.Xc @ p['beta']
        gym_term = p['sigma_gym'] * p['gym_raw']
        c = user_term[self.obs_u] + gym_term[self.obs_g]

        log_rate = (p['log_lambda0'] + p['kappa'] * self.n_visits
                    + p['rho'] * self.r_obs)
        nu = np.exp(-log_rate)

        total = 0.0

        # One observation: the offset's Gaussian and the noise Gaussian merge.
        if len(self.single_obs):
            s = self.single_obs
            total += exgaussian_logpdf(-self.m[s], -c[s],
                                       np.sqrt(sl ** 2 + su ** 2), nu[s]).sum()

        # Several observations: integrate the shared offset numerically,
        # with the nodes placed on each climber's own peak.
        if len(self.multi_obs):
            total += self.multi_log_integral(c, nu, sl, su).sum()

        return float(total)

    def multi_log_integral(self, c, nu, sl, su):
        """log p(observations of climber u), one entry per multi-obs climber.

        Split out so a test can compare it against dense numerical integration
        of the same integral without reimplementing the node placement -- a
        test that rebuilds the nodes itself only checks arithmetic, not
        whether the nodes are in the right place.
        """
        o = self.multi_obs
        seg, nseg = self.multi_seg, self.n_multi_users
        mo, co, nuo = self.m[o], c[o], nu[o]

        mode, scale = self._laplace(mo, co, nuo, sl, su, seg, nseg)
        # eps_k = mode_u + scale_u * z_k, one column per climber.
        eps = mode[None, :] + scale[None, :] * self.gh_z[:, None]  # (Q, U)
        eps_obs = eps[:, seg]                                      # (Q, n)

        ll = exgaussian_logpdf(-mo[None, :], -(co[None, :] + eps_obs),
                               sl, nuo[None, :])
        # Sum within climber. bincount is ~50x faster here than np.add.at.
        per_user = np.stack([np.bincount(seg, weights=row, minlength=nseg)
                             for row in ll])
        # Change of variables. The nodes carry the Hermite weight exp(-z^2/2),
        # which is not the density being integrated against, so divide it back
        # out (+z^2/2) and multiply in the actual Gaussian prior on eps and the
        # Jacobian `scale` of eps = mode + scale * z.
        logint = (per_user
                  - 0.5 * (eps / su) ** 2 - np.log(su)
                  + self.gh_logw[:, None] + 0.5 * self.gh_z[:, None] ** 2
                  + np.log(scale)[None, :])
        return logsumexp(logint, axis=0)

    def pieces(self, theta):
        """The intermediate quantities, for tests and diagnostics."""
        p = self.unpack(theta)
        user_term = p['beta0'] + self.Xc @ p['beta']
        gym_term = p['sigma_gym'] * p['gym_raw']
        c = user_term[self.obs_u] + gym_term[self.obs_g]
        nu = np.exp(-(p['log_lambda0'] + p['kappa'] * self.n_visits
                      + p['rho'] * self.r_obs))
        return c, nu, self.sigma_link_fixed, p['sigma_user']

    def _laplace(self, m, c, nu, sl, su, seg, nseg, iters=4):
        """Peak and width of each climber's integrand, by Newton's method.

        h(eps) = sum_i log ExGauss(m_i | eps) - eps^2 / (2 sigma_user^2)

        With z_i = (c_i + eps - m_i)/sigma_link - sigma_link/nu_i and the
        inverse Mills ratio lam(z) = phi(z)/Phi(z):

            h'  = sum_i [ lam(z_i)/sigma_link - 1/nu_i ] - eps/sigma_user^2
            h'' = sum_i [ -lam(z_i)(z_i + lam(z_i)) ] / sigma_link^2
                  - 1/sigma_user^2

        h'' is strictly negative (lam(z)(z + lam(z)) is positive for all z --
        it is the variance of a truncated normal, up to sign), so h is concave
        and Newton converges from eps = 0 in a handful of steps. No safeguard
        loop is needed, and none is included: if that concavity ever failed the
        right answer is to find out loudly, not to limp on with a bad centre.
        """
        eps = np.zeros(nseg)
        for _ in range(iters):
            e_obs = eps[seg]
            z = (c + e_obs - m) / sl - sl / nu
            lam = np.exp(_log_phi(z) - log_ndtr(z))
            # Same Normal-limit switch as the density: where nu is negligible
            # the ExGaussian derivatives are inf - inf, and the truth is just
            # the Normal's. Getting this wrong would not corrupt the integral
            # (the density switches independently) but would put the nodes in
            # the wrong place, which is the same thing in the end.
            small = nu <= 0.05 * sl
            d1 = np.where(small, -(c + e_obs - m) / sl ** 2, lam / sl - 1.0 / nu)
            d2 = np.where(small, -1.0 / sl ** 2, -lam * (z + lam) / sl ** 2)
            g1 = np.bincount(seg, weights=d1, minlength=nseg) - eps / su ** 2
            g2 = np.bincount(seg, weights=d2, minlength=nseg) - 1.0 / su ** 2
            eps = eps - g1 / g2
        scale = 1.0 / np.sqrt(-g2)
        return eps, scale

    # ---- priors, matching build_model_v2 -------------------------------

    def log_prior(self, theta):
        p = self.unpack(theta)
        lp = 0.0
        lp += _norm_lp(p['beta0'], self.median_m, 5.0)
        # HalfNormal on the natural scale, sampled as log -> + log|d sigma/d log sigma|
        lp += _halfnorm_lp(p['sigma_user'], 2.0) + p['log_sigma_user']
        for nm, b in zip(self.Xnames, p['beta']):
            lp += _norm_lp(b, 0.0, self.prior_sd[nm])
        lp += _halfnorm_lp(p['sigma_gym'], 1.5) + p['log_sigma_gym']
        # ZeroSumNormal(sigma=1) over the n_gyms-1 free coordinates.
        lp += -0.5 * np.sum(p['gym_raw'] ** 2)
        lp += _norm_lp(p['log_lambda0'], 0.0, 1.0)
        lp += _norm_lp(p['kappa'], 0.0, 0.5)
        lp += _norm_lp(p['rho'], 0.0, 0.5)
        return float(lp)

    def log_posterior(self, theta):
        lp = self.log_prior(theta)
        if not np.isfinite(lp):
            return -np.inf
        ll = self.log_likelihood(theta)
        return lp + ll if np.isfinite(ll) else -np.inf

    def initial_point(self, rng=None):
        rng = rng or np.random.default_rng(0)
        th = np.zeros(self.n_params)
        th[0] = self.median_m
        th[1] = np.log(1.0)        # sigma_user
        idx = 2 + len(self.Xnames)
        th[idx] = np.log(0.3)      # sigma_gym
        return th + 0.01 * rng.standard_normal(self.n_params)


def _log_phi(z):
    return -0.5 * z ** 2 - 0.5 * np.log(2 * np.pi)


def _norm_lp(x, mu, sd):
    return -0.5 * ((x - mu) / sd) ** 2 - np.log(sd) - 0.5 * np.log(2 * np.pi)


def _halfnorm_lp(x, sd):
    if np.any(x <= 0):
        return -np.inf
    return (-0.5 * (x / sd) ** 2 - np.log(sd) + 0.5 * np.log(2 / np.pi))
