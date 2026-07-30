"""Data prep and PyMC model for the Bayesian gym-grading model.

Design and rationale: see BAYESIAN_GRADING_MODEL.md at the repo root. Every
equation/prior referenced in docstrings here corresponds to a section in
that doc — read it first if the "why" behind a choice isn't obvious from
the code alone.

Fit independently per discipline (bouldering / routes) throughout — nothing
in here pools across disciplines.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import pymc as pm

from kaya.data_access import (
    BOULDER_GRADE_TO_NUM,
    KayaDataAccessor,
    route_grade_to_num,
)


@dataclass
class GradingModelDataset:
    """Everything the PyMC model needs for one discipline.

    observations: one row per (user_id, gym_id) with >=1 send in this
        discipline. Columns: user_id, gym_id, climb_id (behind the user's
        hardest send at that gym), m (that send's numeric grade), n_visits
        (distinct days at that gym, in this discipline).
    users: one row per user_id appearing in `observations`. Columns:
        height, ape_index (inches), gender (gender_guesser string),
        n_sends, n_sesh, n_sends_per_sesh (all discipline-scoped — see
        BAYESIAN_GRADING_MODEL.md section 7 on why these can't be reused
        from KayaDataAccessor.read_user_profiles(), which aggregates both
        disciplines together).
    gyms: sorted list of gym_id values appearing in `observations`, fixing
        index order for the model.
    climbs: sorted list of climb_id values appearing in `observations`,
        each tagged with the gym_id it belongs to (climb_correction's
        hierarchical prior needs to know which gym each climb is nested
        under — section 2).
    """

    discipline: str
    observations: pd.DataFrame
    users: pd.DataFrame
    gyms: pd.DataFrame
    climbs: pd.DataFrame


def _grade_num_for_discipline(sends_df: pd.DataFrame, discipline: str) -> pd.Series:
    if discipline == 'bouldering':
        return sends_df['grade'].map(BOULDER_GRADE_TO_NUM)
    return sends_df['grade'].apply(route_grade_to_num)


def _discipline_mask(climb_type_lower: pd.Series, discipline: str) -> pd.Series:
    if discipline == 'bouldering':
        return climb_type_lower.str.contains('boulder')
    return climb_type_lower.str.contains('route') | climb_type_lower.str.contains('rope')


def build_grading_model_dataset(
    discipline: str,
    accessor: Optional[KayaDataAccessor] = None,
) -> GradingModelDataset:
    """Build the full dataset for one discipline ('bouldering' or 'routes').

    Reads the raw sends table once and derives everything from it, rather
    than reusing ViewerPayloadBuilder._build_gym_comparison_base() (which
    computes a similar per-user/gym max-grade table but doesn't carry
    climb_id, and reuses KayaDataAccessor.read_user_profiles(), whose
    activity counts aren't discipline-scoped) or read_user_profiles()
    directly, for the same reason.
    """
    if discipline not in ('bouldering', 'routes'):
        raise ValueError(f"discipline must be 'bouldering' or 'routes', got {discipline!r}")

    accessor = accessor or KayaDataAccessor()
    sends_df = accessor.read_sends(
        source='local_db',
        columns=[
            'user_id', 'gym_id', 'date', 'grade', 'climb_type', 'climb_id',
            'height', 'ape_index', 'first_name',
        ],
        parse_dates=False,
        order_by=False,
    )
    if sends_df.empty:
        empty = pd.DataFrame()
        return GradingModelDataset(discipline, empty, empty, empty, empty)

    climb_type_lower = sends_df['climb_type'].fillna('').astype(str).str.lower()
    mask = _discipline_mask(climb_type_lower, discipline)
    sends_df = sends_df.loc[mask].copy()
    sends_df['grade_num'] = _grade_num_for_discipline(sends_df, discipline)

    sends_df = sends_df[
        sends_df['grade_num'].notna()
        & sends_df['user_id'].notna()
        & sends_df['gym_id'].notna()
        & sends_df['climb_id'].notna()
    ].copy()
    if sends_df.empty:
        empty = pd.DataFrame()
        return GradingModelDataset(discipline, empty, empty, empty, empty)

    sends_df['gym_id'] = sends_df['gym_id'].astype(str)
    sends_df['climb_id'] = sends_df['climb_id'].astype(str)
    sends_df['user_id'] = sends_df['user_id'].astype(str)

    # --- observations: per (user, gym), the hardest send and its climb ---
    group_cols = ['user_id', 'gym_id']
    n_visits = sends_df.groupby(group_cols, as_index=False).agg(n_visits=('date', 'nunique'))
    hardest = (
        sends_df.sort_values(group_cols + ['grade_num'])
        .groupby(group_cols, as_index=False)
        .tail(1)[group_cols + ['climb_id', 'grade_num']]
        .rename(columns={'grade_num': 'm'})
    )
    observations = hardest.merge(n_visits, on=group_cols, how='left')

    # --- climbs: which gym each climb belongs to (for climb_correction's
    # hierarchical prior mean) ---
    climbs = (
        sends_df[['climb_id', 'gym_id']]
        .drop_duplicates(subset='climb_id')
        .reset_index(drop=True)
    )

    # --- users: discipline-scoped activity + discipline-independent body/gender ---
    user_activity = sends_df.groupby('user_id', as_index=False).agg(
        n_sends=('grade_num', 'size'),
        n_sesh=('date', 'nunique'),
        height=('height', 'first'),
        ape_index=('ape_index', 'first'),
        first_name=('first_name', 'first'),
    )
    user_activity['n_sends_per_sesh'] = user_activity['n_sends'] / user_activity['n_sesh'].clip(lower=1)
    user_activity['height'] = user_activity['height'] / 2.54
    user_activity['ape_index'] = user_activity['ape_index'] / 2.54

    detector = accessor._gender_detector
    user_activity['gender'] = user_activity['first_name'].fillna('').apply(detector.get_gender)
    users = user_activity.drop(columns=['first_name']).set_index('user_id')

    gyms = pd.DataFrame({'gym_id': sorted(observations['gym_id'].unique())})

    return GradingModelDataset(
        discipline=discipline,
        observations=observations.reset_index(drop=True),
        users=users,
        gyms=gyms,
        climbs=climbs,
    )


def dataset_summary(dataset: GradingModelDataset) -> Dict[str, Any]:
    """Quick sanity-check numbers — not part of the model, just for eyeballing
    a freshly-built dataset before spending time fitting it."""
    if dataset.observations.empty:
        return {'discipline': dataset.discipline, 'empty': True}
    obs = dataset.observations
    return {
        'discipline': dataset.discipline,
        'n_observations': int(len(obs)),
        'n_users': int(obs['user_id'].nunique()),
        'n_gyms': int(dataset.gyms.shape[0]),
        'n_climbs': int(dataset.climbs.shape[0]),
        'n_users_multi_gym': int((obs.groupby('user_id')['gym_id'].nunique() >= 2).sum()),
        'median_n_visits': float(obs['n_visits'].median()),
        'm_range': (float(obs['m'].min()), float(obs['m'].max())),
        'height_missing_frac': float(dataset.users['height'].isna().mean()),
        'ape_index_missing_frac': float(dataset.users['ape_index'].isna().mean()),
    }


def build_pymc_model(
    dataset: GradingModelDataset,
    *,
    include_covariates: bool = True,
    height_quadratic: bool = True,
    ape_quadratic: bool = True,
    include_climb_correction: bool = True,
    climb_prior: str = 'studentt',
    climb_nu: float = 4.0,
    horseshoe_slab_scale: float = 2.0,
    use_gap_likelihood: bool = True,
    include_reliability: bool = True,
    sigma_link: float = 0.5,
    sigma_climb_sigma: float = 0.5,
) -> pm.Model:
    """Build the PyMC model for one discipline: BAYESIAN_GRADING_MODEL.md
    sections 1 (ability), 2 (gym/climb correction), 3 (observation model).

    Takes a GradingModelDataset from build_grading_model_dataset() and
    returns an unfit pm.Model -- call pm.sample() inside a `with model:`
    block (or pass model=model to pm.sample) to fit it.

    The flags below build the model up in stages (agreed in conversation
    after the full model showed poor mixing/identifiability issues on a
    real-data smoke test) -- default to the full design-doc model, but can
    be individually switched off to isolate which component is responsible
    for a given pathology before re-enabling it:

    - include_covariates: gender/height/ape terms in the ability sub-model
      (section 1). Off -> ability(u) = beta0 + epsilon(u) only.
    - height_quadratic / ape_quadratic: include the quadratic (gamma2/delta2)
      term for that covariate, vs. linear-only. For the linear-vs-quadratic
      functional-form check (design doc section 8) -- fit both, compare via
      az.loo()/az.compare() using the 'log_lik' Deterministic this function
      always adds, rather than eyeballing whether the credible interval on
      gamma2/delta2 excludes zero.
    - include_climb_correction: per-climb hierarchical correction (section
      2). Off -> ceiling routes through gym_correction directly, skipping
      climb_correction entirely (also removes the epsilon/climb_correction
      identifiability tension seen in the first real-data smoke test).
    - use_gap_likelihood: the one-sided Exponential-gap mechanism (section
      3). Off -> plain m ~ Normal(ceiling, sigma_obs), no ceiling/visits
      bias correction at all.
    - include_reliability: the r(u) reliability term in the gap rate
      (section 7). Only meaningful when use_gap_likelihood=True.
    - sigma_link: fixed (not estimated) scale for the soft link between the
      gap-implied ceiling and the structural ceiling. Deliberately a
      constant, not a free RV with its own prior -- it's a numerical device
      for making the hard identity m=ceiling-gap tractable for NUTS, and
      letting the data estimate it caused it to collapse toward 0 in
      testing, recreating the original stiff-boundary geometry it was
      introduced to avoid. It's not purely a numerical fudge, though: since
      grades are integer labels on a continuous difficulty scale, some
      nonzero disagreement between the two ceilings is real (quantization
      noise), not just a sampling convenience. Swept over {0.05, 0.1, 0.2,
      0.3, 0.5} on real data (climb correction off, section 2 disabled);
      0.5 was the only value with clean diagnostics (R-hat ~1.00, healthy
      ESS) -- larger than quantization alone would suggest (~0.25-0.3),
      plausibly because climb-level correction being off leaves
      sigma_link as the only place for genuine per-climb grading
      variation to go. Re-sweep once climb-level correction is
      reintroduced (see include_climb_correction) rather than assuming 0.5
      still holds.
    - sigma_climb_sigma: prior scale for sigma_climb. Tightened from the
      original design doc's implicit HalfNormal(1.0) to make climb-level
      correction more skeptical by default -- the real-data smoke test
      showed it absorbing per-user ability variation (sigma_user -> ~0)
      when left loose, given how few observations most individual climbs
      have in this dataset.
    - climb_prior / climb_nu / horseshoe_slab_scale: distribution family for
      climb_correction's hierarchical prior -- 'normal' (Stage 1's original,
      tightening sigma_climb_sigma alone wasn't enough to fix the collapse),
      'studentt' (heavier tails), or 'horseshoe' (regularized/"Finnish"
      horseshoe -- global scale shrinks everything by default, per-climb
      local scale lets a minority of well-supported climbs escape that
      shrinkage, slab_scale caps how far any one climb can escape).

      IMPORTANT -- climb_correction and include_covariates are structurally
      in tension, not just a prior-tuning problem: real data shows climbs
      cluster by user height well beyond chance (observed-vs-null variance
      ratio 1.73 on the 2-gym subset -- which climb becomes someone's
      "hardest send" is itself correlated with their height, plausibly
      reachy/tall-favoring climbs etc). That means climb identity and
      covariates are confounded in the data-generating process itself, not
      just hard to separate computationally. Confirmed by elimination: with
      both include_covariates=True and include_climb_correction=True on
      real data, StudentT AND horseshoe both collapsed sigma_user to ~0 and
      wiped out previously well-identified covariate effects (beta_gender
      etc.) -- horseshoe's global scale (climb_tau) inflated to an
      essentially unconstrained value instead of shrinking, because the
      confounding is pervasive across many climbs, not a sparse few
      outliers horseshoe's sparsity assumption expects. A synthetic
      recreation of this exact selection effect confirmed both the failure
      (Normal: R-hat 2.17) and that horseshoe meaningfully helps *when the
      only confound is per-user noise* (no covariates) -- so climb
      correction is likely viable on its own, just not jointly with
      covariates in one model. Practical implication: run these as two
      separate configurations rather than one unified model -- covariates
      on / climb correction off (this staged build's main line, Stage 0-4)
      for ability/gym-bias questions, or climb correction on / covariates
      off for "is this climb hard or soft" diagnostics. See
      BAYESIAN_GRADING_MODEL.md and the staged-build diagnostics artifact
      for the full investigation.
    """
    obs = dataset.observations
    users = dataset.users
    climbs = dataset.climbs

    user_ids = users.index.tolist()
    user_idx_map = {uid: i for i, uid in enumerate(user_ids)}
    gym_ids = dataset.gyms['gym_id'].tolist()
    gym_idx_map = {gid: i for i, gid in enumerate(gym_ids)}
    climb_ids = climbs['climb_id'].tolist()
    climb_idx_map = {cid: i for i, cid in enumerate(climb_ids)}

    climb_gym_idx = climbs['gym_id'].map(gym_idx_map).to_numpy()
    obs_user_idx = obs['user_id'].map(user_idx_map).to_numpy()
    obs_gym_idx = obs['gym_id'].map(gym_idx_map).to_numpy()
    obs_climb_idx = obs['climb_id'].map(climb_idx_map).to_numpy()
    m_data = obs['m'].to_numpy(dtype=float)
    n_visits = obs['n_visits'].to_numpy(dtype=float)

    # Height/ape (section 1): centered on population median; missing values
    # (~10%/~38% of users -- see dataset_summary) imputed at the median, i.e.
    # zero contribution from f_height/f_ape for those users. No principled
    # per-user substitute exists without more data, so this is a v1
    # simplification, not a claim those users have average height/ape.
    height = users['height'].to_numpy(dtype=float)
    ape = users['ape_index'].to_numpy(dtype=float)
    height_c = np.nan_to_num(height - np.nanmedian(height), nan=0.0)
    ape_c = np.nan_to_num(ape - np.nanmedian(ape), nan=0.0)

    # gender(u): gender_guesser returns male/mostly_male/female/mostly_female
    # /andy/unknown. Encoded as a continuous indicator rather than dropping
    # ambiguous users: 1.0 female-ish, 0.0 male-ish, 0.5 andy/unknown (no
    # directional assumption for the ambiguous cases).
    gender_map = {'female': 1.0, 'mostly_female': 1.0, 'male': 0.0, 'mostly_male': 0.0}
    gender_female = users['gender'].map(gender_map).fillna(0.5).to_numpy(dtype=float)

    # r(u) (section 7): reuses the Active/Inactive n_sends_per_sesh signal
    # directly as a continuous reliability proxy, scaled by its own median so
    # rho's magnitude doesn't depend on the discipline's raw units. Centered
    # at 0 for a typical user (r=0), not 1 -- rate = lambda0*(1+kappa*n)*
    # (1+rho*r) would otherwise make (1+rho*r) a near-constant multiplier for
    # any near-median user, making rho almost perfectly anti-correlated with
    # lambda0 (empirically corr = -0.91 on real data) since both are just
    # rescaling the same overall rate. Centering isolates rho to explaining
    # *deviations* from typical reliability, same fix as height/ape centering.
    n_sends_per_sesh = users['n_sends_per_sesh'].to_numpy(dtype=float)
    r_scale = np.nanmedian(n_sends_per_sesh)
    r_user = (n_sends_per_sesh / r_scale - 1.0) if r_scale else np.zeros_like(n_sends_per_sesh)
    r_obs = r_user[obs_user_idx]

    # beta0's prior mean is set to the population median grade rather than 0
    # -- purely a warm-start convenience (ability and grade share a scale by
    # construction), not load-bearing for correctness (see the soft-link
    # note below on why no boundary-matching init is required here).
    median_m = float(np.nanmedian(m_data))

    coords = {
        'user': user_ids,
        'gym': gym_ids,
        'climb': climb_ids,
        'obs': np.arange(len(obs)),
    }

    with pm.Model(coords=coords) as model:
        # --- section 1: ability sub-model ---
        beta0 = pm.Normal('beta0', mu=median_m, sigma=5)
        # Non-centered parameterization (epsilon_raw ~ N(0,1), scaled by
        # sigma_user) rather than epsilon ~ Normal(0, sigma_user) directly --
        # the centered form produces a "funnel" geometry NUTS struggles with
        # whenever sigma_user's posterior gets small, and empirically caused
        # near-100% divergences here.
        sigma_user = pm.HalfNormal('sigma_user', sigma=2)
        epsilon_raw = pm.Normal('epsilon_raw', mu=0, sigma=1, dims='user')
        epsilon = pm.Deterministic('epsilon', sigma_user * epsilon_raw, dims='user')

        if include_covariates:
            beta_gender = pm.Normal('beta_gender', mu=0, sigma=2)
            gender_data = pm.Data('gender_data', gender_female, dims='user')
            covariate_term = beta_gender * gender_data

            height_data = pm.Data('height_data', height_c, dims='user')
            gamma1 = pm.Normal('gamma1', mu=0, sigma=1)
            covariate_term = covariate_term + gamma1 * height_data
            if height_quadratic:
                gamma2 = pm.Normal('gamma2', mu=0, sigma=0.3)
                covariate_term = covariate_term + gamma2 * height_data ** 2

            ape_data = pm.Data('ape_data', ape_c, dims='user')
            delta1 = pm.Normal('delta1', mu=0, sigma=1)
            covariate_term = covariate_term + delta1 * ape_data
            if ape_quadratic:
                delta2 = pm.Normal('delta2', mu=0, sigma=0.3)
                covariate_term = covariate_term + delta2 * ape_data ** 2
        else:
            covariate_term = 0.0

        ability = pm.Deterministic('ability', beta0 + covariate_term + epsilon, dims='user')

        # --- section 2: gym correction (always present) ---
        sigma_gym = pm.HalfNormal('sigma_gym', sigma=1.5)
        gym_correction_raw = pm.Normal('gym_correction_raw', mu=0, sigma=1, dims='gym')
        gym_correction = pm.Deterministic(
            'gym_correction', sigma_gym * gym_correction_raw, dims='gym',
        )

        obs_user_idx_data = pm.Data('obs_user_idx', obs_user_idx, dims='obs')
        m_obs_data = pm.Data('m_obs_data', m_data, dims='obs')

        # --- climb-level correction (optional -- see include_climb_correction
        # docstring above) ---
        if include_climb_correction:
            climb_gym_idx_data = pm.Data('climb_gym_idx', climb_gym_idx, dims='climb')
            if climb_prior == 'horseshoe':
                # Regularized ("Finnish") horseshoe -- Piironen & Vehtari
                # (2017). Global scale tau shrinks everything by default;
                # per-climb local scale lambda_c lets a minority of
                # well-supported climbs escape that shrinkage; the slab
                # (horseshoe_slab_scale) caps how far any single climb can
                # escape, avoiding the raw horseshoe's unbounded tails.
                # Non-centered throughout to avoid the funnel geometry that
                # plain-Normal centered parameterizations hit in this model.
                tau = pm.HalfCauchy('climb_tau', beta=sigma_climb_sigma)
                lambda_raw = pm.HalfCauchy('climb_lambda_raw', beta=1.0, dims='climb')
                slab_scale = horseshoe_slab_scale
                lambda_tilde = (slab_scale * lambda_raw) / pm.math.sqrt(
                    slab_scale ** 2 + (tau ** 2) * (lambda_raw ** 2)
                )
                z = pm.Normal('climb_z', mu=0, sigma=1, dims='climb')
                climb_correction_raw = tau * lambda_tilde * z
                sigma_climb = pm.Deterministic('sigma_climb', tau)
            else:
                sigma_climb = pm.HalfNormal('sigma_climb', sigma=sigma_climb_sigma)
                if climb_prior == 'studentt':
                    climb_correction_raw = pm.StudentT(
                        'climb_correction_raw', nu=climb_nu, mu=0, sigma=1, dims='climb',
                    )
                else:
                    climb_correction_raw = pm.Normal('climb_correction_raw', mu=0, sigma=1, dims='climb')
                climb_correction_raw = sigma_climb * climb_correction_raw
            climb_correction = pm.Deterministic(
                'climb_correction',
                gym_correction[climb_gym_idx_data] + climb_correction_raw,
                dims='climb',
            )
            obs_climb_idx_data = pm.Data('obs_climb_idx', obs_climb_idx, dims='obs')
            correction_term = climb_correction[obs_climb_idx_data]
        else:
            obs_gym_idx_data = pm.Data('obs_gym_idx', obs_gym_idx, dims='obs')
            correction_term = gym_correction[obs_gym_idx_data]

        structural_ceiling = pm.Deterministic(
            'ceiling', ability[obs_user_idx_data] + correction_term, dims='obs',
        )

        # --- section 3: observation model ---
        if use_gap_likelihood:
            lambda0 = pm.HalfNormal('lambda0', sigma=2)
            if include_reliability:
                kappa = pm.HalfNormal('kappa', sigma=1)
                rho = pm.HalfNormal('rho', sigma=1)
                n_visits_data = pm.Data('n_visits_data', n_visits, dims='obs')
                r_obs_data = pm.Data('r_obs_data', r_obs, dims='obs')
                rate = lambda0 * (1 + kappa * n_visits_data) * (1 + rho * r_obs_data)
            else:
                rate = lambda0

            # gap ~ Exponential(rate) (section 3) is a genuine free latent
            # here (dims='obs'), not derived from ceiling -- PyMC transforms
            # Exponential RVs to log-space internally, so gap > 0 is
            # guaranteed by construction and NUTS's leapfrog steps can never
            # cross into an invalid region. m + gap is then the data's
            # "implied ceiling."
            #
            # The model's structural assumption (m = ceiling - gap, i.e.
            # implied ceiling == structural ceiling *exactly*) is enforced
            # as a soft link -- Normal(structural_ceiling, sigma_link) --
            # rather than a hard identity. A hard identity would tie a
            # positivity-constrained latent to an unconstrained sum of
            # latents via a rejection boundary, which is intractable for
            # NUTS at this dimensionality (verified empirically: even a
            # 30-observation, zero-hierarchy toy version of the
            # hard-identity formulation diverged on ~100% of steps).
            # sigma_link is a fixed constant (see docstring) rather than an
            # estimated RV, so nothing can push it back toward that
            # boundary.
            gap = pm.Exponential('gap', lam=rate, dims='obs')
            implied_ceiling = m_obs_data + gap
            link_logp = pm.logp(pm.Normal.dist(mu=structural_ceiling, sigma=sigma_link), implied_ceiling)
            pm.Potential('ceiling_link', link_logp)
            # Pointwise per-observation log-likelihood for LOO (az.loo()) --
            # PyMC can't auto-derive this the way it would for a plain
            # observed= RV, since the data enters through gap (a free latent)
            # and this Potential rather than a single observed likelihood
            # term. Sum of gap's own Exponential density (how likely is this
            # gap under the visits/reliability-implied rate) and the link's
            # Normal density (how well the gap-implied ceiling matches the
            # structural one) is the total contribution of observation i's
            # data to the joint log-density.
            gap_logp = pm.logp(pm.Exponential.dist(lam=rate), gap)
            pm.Deterministic('log_lik', gap_logp + link_logp, dims='obs')
        else:
            sigma_obs = pm.HalfNormal('sigma_obs', sigma=2)
            pm.Normal(
                'm_likelihood', mu=structural_ceiling, sigma=sigma_obs,
                observed=m_obs_data, dims='obs',
            )
            pm.Deterministic(
                'log_lik',
                pm.logp(pm.Normal.dist(mu=structural_ceiling, sigma=sigma_obs), m_obs_data),
                dims='obs',
            )

    return model
