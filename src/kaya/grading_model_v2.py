"""Grading model v2: probabilistic gender, EMG likelihood, flexible height forms.

Successor to grading_model.py. Keeps that model's structure (IRT-style
ability + hierarchical gym correction + one-sided gap likelihood) but
changes four things, each independently switchable so they can be tested
one at a time:

1. **Probabilistic gender.** grading_model.py coded gender as a hard
   0/0.5/1 from gender_guesser labels. That is both coarse (`mostly_female`
   -> 1.0 when its true P(female) is ~0.66; `unknown` -> 0.5 when its true
   P(female) is ~0.38) and, critically, *height-correlated*: a tall person
   with a female-ish name is more likely male (P(female)=0.46 at 70in,
   0.11 at 74in). Since gender has a ~1-grade main effect, that
   misclassification leaks sex signal into the height coefficient and can
   manufacture the "height helps female-coded climbers" finding. Handled
   here either by filtering to confident names, or by marginalizing over
   latent sex (see `gender_mode`).

2. **EMG likelihood.** m = ceiling - gap + noise with gap ~ Exponential is
   a Normal minus an Exponential, i.e. an Exponentially Modified Gaussian,
   which has a closed form. Marginalizing `gap` analytically removes one
   latent per observation (6.4k at 6 gyms, 33k at 29), gives a real
   `observed=` node so LOO/PPC work natively, and is what may finally let
   sigma_link be estimated rather than pinned at 0.5.

3. **Flexible height forms**, including a monotone saturating curve
   ("reach helps until you have enough") which the original investigation
   never tested -- the asymmetric bump it rejected (left width 1.59in vs
   right 12.87in) is what a saturating curve looks like when you force a
   symmetric bell onto it.

4. **Bounded per-climb quantization.** Grades are continuous but labels are
   integers: a climb that is truly 5.3 gets written down as 5, and every
   climber who sends it is understated by the same 0.3. That per-climb
   offset is physically bounded to +/-0.5, unlike the unbounded
   Normal/StudentT/horseshoe priors that previously collapsed sigma_user to
   zero -- a bounded offset *cannot* absorb per-user ability variance.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import pymc as pm
import pytensor.tensor as pt

from kaya.advancement import DATE_COL as ADVANCEMENT_DATE_COL
from kaya.advancement import advancement_offset
from kaya.data_access import BOULDER_GRADE_TO_NUM, KayaDataAccessor, route_grade_to_num
from kaya.marginal_pt import build_layout, multi_log_integral_pt

# Plausible adult ranges, in inches. Values outside these are treated as
# missing rather than believed -- see prepare_base_data.
HEIGHT_PLAUSIBLE = (48.0, 84.0)     # 4'0" - 7'0"
APE_PLAUSIBLE = (-12.0, 12.0)       # wingspan minus height


# --------------------------------------------------------------------------
# Data preparation
# --------------------------------------------------------------------------

def prepare_base_data(
    discipline: str = 'bouldering',
    accessor: Optional[KayaDataAccessor] = None,
    source: str = 'local_db',
) -> Dict[str, pd.DataFrame]:
    """Read sends once and build the full (all-gym) observation/user tables.

    Expensive (reads the whole sends table), so callers should cache the
    result and subset it with `make_dataset` rather than calling this per fit.

    `source` selects where the sends come from, and the two available answers
    are not equivalent:

      'local_db'  the sqlite mirror. Fast, and **stale** — as of 2026-08-07 it
                  was last written in June 2025 and covered about half the
                  (climber, gym) pairs the S3 history holds.
      's3_raw'    the authoritative pull history. Slower (thousands of gzipped
                  JSONL objects) but complete and current, and the only source
                  that includes gyms backfilled since the mirror went stale.

    Anything time-based must be built from 's3_raw'; see
    scripts/build_base_snapshot.py, which is the supported way to do it.
    """
    import nomquamgender as nqg

    accessor = accessor or KayaDataAccessor()
    sends = accessor.read_sends(
        source=source,
        columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type', 'climb_id',
                 'height', 'ape_index', 'first_name'],
        parse_dates=False,
        order_by=False,
    )
    ct = sends['climb_type'].fillna('').astype(str).str.lower()
    if discipline == 'bouldering':
        sends = sends[ct.str.contains('boulder')].copy()
        sends['m'] = sends['grade'].map(BOULDER_GRADE_TO_NUM)
    else:
        sends = sends[ct.str.contains('route') | ct.str.contains('rope')].copy()
        sends['m'] = sends['grade'].apply(route_grade_to_num)

    sends = sends[sends['m'].notna() & sends['user_id'].notna()
                  & sends['gym_id'].notna() & sends['climb_id'].notna()].copy()
    for c in ('user_id', 'gym_id', 'climb_id'):
        sends[c] = sends[c].astype(str)

    gcols = ['user_id', 'gym_id']
    # Dates survive the aggregation as three columns, not just as counts.
    #
    # They used to survive only as `n_visits`, which made the whole dataset
    # timeless: one number per (climber, gym) with no when. That is why the
    # model has no `t` in it, why climber advancement lands in a gym's
    # correction, and why gym drift has nowhere to live. Carrying the dates
    # costs three columns and unblocks all of it — see
    # docs/two-stage-and-grade-compression.md.
    #
    # `max_send_date` is the one that matters: it is the date of the very row
    # the model turns into an observation, so it is the timestamp an
    # advancement offset must be computed against. first/last bound the
    # climber's span at that gym, which is what a windowed formulation needs.
    per_pair = sends.groupby(gcols, as_index=False).agg(
        n_visits=('date', 'nunique'), n_sends_gym=('m', 'size'),
        first_send=('date', 'min'), last_send=('date', 'max'))
    # NOTE: ties at the max grade are broken arbitrarily by tail(1). Harmless
    # for `m` (tied climbs share the grade) but it does pin the observation to
    # one arbitrary climb_id, which matters for climb-level quantization.
    hardest = (sends.sort_values(gcols + ['m']).groupby(gcols, as_index=False)
               .tail(1)[gcols + ['climb_id', 'm', 'date']]
               .rename(columns={'date': 'max_send_date'}))
    obs = hardest.merge(per_pair, on=gcols, how='left')

    # n_at_max: how many times the climber actually sent their hardest grade
    # here. Far more direct evidence of "have they found their ceiling?" than
    # visit count -- 51.5% of pairs sent their max exactly once (could be a
    # soft climb or a lucky day) while 3.1% sent it 10+ times (clearly
    # plateaued there). Only ~2.8% of raw sends survive into the model at all,
    # and this recovers some of the discarded signal without restructuring the
    # likelihood. Note the *spacing* between top grades carries almost nothing
    # -- the 2nd-highest distinct grade is exactly 1 below the max 90.8% of
    # the time, because grades are integers -- so counts, not gaps, are where
    # the recoverable information is.
    mx = sends.merge(hardest[gcols + ['m']].rename(columns={'m': '_mx'}), on=gcols, how='left')
    n_at_max = (mx[mx['m'] == mx['_mx']].groupby(gcols, as_index=False)
                .size().rename(columns={'size': 'n_at_max'}))
    obs = obs.merge(n_at_max, on=gcols, how='left')
    obs['n_at_max'] = obs['n_at_max'].fillna(1).astype(float)

    climbs = sends[['climb_id', 'gym_id']].drop_duplicates('climb_id').reset_index(drop=True)

    users = sends.groupby('user_id', as_index=False).agg(
        n_sends=('m', 'size'), n_sesh=('date', 'nunique'),
        height=('height', 'first'), ape_index=('ape_index', 'first'),
        first_name=('first_name', 'first'),
    )
    users['n_sends_per_sesh'] = users['n_sends'] / users['n_sesh'].clip(lower=1)
    users['height'] = users['height'] / 2.54
    users['ape_index'] = users['ape_index'] / 2.54

    # Implausible body measurements -> NaN (i.e. treated as missing, not
    # dropped: the user's sends are still valid data). The raw field contains
    # obvious entry artifacts -- heights of 12in and a pile of exactly 96in
    # (a slider cap), ape indices of +/-30in. Only ~0.9% of heights and ~1.5%
    # of ape values, but they are catastrophic in a QUADRATIC term: the 50
    # most extreme users carry 10.3% of all sum-of-squares leverage in h_c^2.
    # The original height investigation (five functional forms, the bump, the
    # gender interaction) was run with these still in.
    users.loc[~users['height'].between(*HEIGHT_PLAUSIBLE), 'height'] = np.nan
    users.loc[~users['ape_index'].between(*APE_PLAUSIBLE), 'ape_index'] = np.nan

    # --- probabilistic gender from name, then sharpened by height ---
    nm = users['first_name'].fillna('').astype(str).str.strip().str.lower()
    ann = nqg.NBGC().annotate(nm.tolist(), as_df=True)
    users['p_gf'] = ann['p(gf)'].values
    users['name_counts'] = ann['counts'].fillna(0).values

    conf = users[(users['name_counts'] >= 100) & users['height'].notna()]
    hm = conf.loc[conf['p_gf'] <= 0.02, 'height']
    hf = conf.loc[conf['p_gf'] >= 0.98, 'height']
    height_dists = {'mu_m': hm.mean(), 'sd_m': hm.std(), 'mu_f': hf.mean(), 'sd_f': hf.std()}
    users['w_female'] = _p_female_given_name_height(
        users['p_gf'].values, users['height'].values, **height_dists)

    users = users.set_index('user_id')
    return {'observations': obs.reset_index(drop=True), 'users': users,
            'climbs': climbs, 'height_dists': height_dists}


def _p_female_given_name_height(p_gf, height, mu_m, sd_m, mu_f, sd_f):
    """P(female | name, height).

    Deliberately uses ONLY the name prior and the height likelihood -- never
    the ability data. Letting ability inform latent sex would let the model
    reassign gender to explain ability, making "tall people are abler" and
    "tall people are likelier male" mutually reinforcing. Cutting that
    feedback keeps the height->ability effect identified by variation in
    *names*, which is independent of height.
    """
    from scipy.stats import norm
    p = np.clip(np.asarray(p_gf, dtype=float), 1e-4, 1 - 1e-4)
    h = np.asarray(height, dtype=float)
    lf = norm.pdf(h, mu_f, sd_f) * p
    lm = norm.pdf(h, mu_m, sd_m) * (1 - p)
    with np.errstate(invalid='ignore'):
        joint = lf / (lf + lm)
    return np.where(np.isnan(h), p, joint)


def gym_network(base: Dict[str, pd.DataFrame], min_shared_users: int = 50) -> List[str]:
    """Largest set of gyms connected by >= min_shared_users shared climbers.

    Connectivity is what identifies gym corrections at all -- an isolated gym
    shares no climbers with the rest, so its correction is confounded with
    the abilities of its own users.
    """
    import collections
    obs = base['observations']
    per_user = obs.groupby('user_id')['gym_id'].nunique()
    multi = obs[obs['user_id'].isin(per_user[per_user >= 2].index)]
    pairs = multi.merge(multi, on='user_id')
    pairs = pairs[pairs['gym_id_x'] < pairs['gym_id_y']]
    edges = pairs.groupby(['gym_id_x', 'gym_id_y']).size().reset_index(name='shared')
    edges = edges[edges['shared'] >= min_shared_users]

    adj = collections.defaultdict(set)
    for a, b in zip(edges['gym_id_x'], edges['gym_id_y']):
        adj[a].add(b); adj[b].add(a)
    seen, comps = set(), []
    for node in adj:
        if node in seen:
            continue
        stack, comp = [node], []
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x); comp.append(x)
            stack.extend(adj[x] - seen)
        comps.append(comp)
    return sorted(max(comps, key=len)) if comps else []


# Prior standard deviations for the covariate coefficients, on the centred and
# scaled design. Module level because marginal_v2 imports it: the two
# implementations are cross-checked against each other, so a second copy that
# drifted would surface as an implementation disagreement and send someone
# hunting a bug that was really an edit in one file.
PRIOR_SD = {'beta_gender': 2.0, 'gamma1': 1.0, 'gamma2': 0.3,
            'gamma1_x': 0.5, 'gamma2_x': 0.15, 'delta1': 1.0,
            'delta2': 0.3, 'delta1_x': 0.5, 'delta2_x': 0.15,
            'beta_h_missing': 1.0, 'beta_a_missing': 1.0}


def zerosum_basis_matrix(model, key: str, n_free: int) -> np.ndarray:
    """PyMC's ZeroSumNormal basis, recovered by probing it with unit vectors.

    A ZeroSumNormal over n categories stores n-1 coordinates in a basis of
    PyMC's own choosing. Those coordinates are NOT the first n-1 elements of
    the zero-sum vector, and they are not `marginal_v2.zero_sum_basis`'s
    coordinates either -- both bases are valid and they are different.

    Any code that wants to evaluate the PyMC model at a gym configuration
    specified some other way therefore needs the map between them. The
    transform is linear, so evaluating it on e_0..e_{n_free-1} recovers its
    matrix exactly. Recovered rather than reimplemented on purpose: assuming
    the two bases agree is the mistake this exists to prevent.

    Returns an ``(n_categories, n_free)`` matrix ``M`` with ``M @ z`` the
    zero-sum vector for coordinates ``z``. Pair with `zerosum_coords` to go
    the other way.
    """
    fn = model.compile_fn(
        model.replace_rvs_by_values([model[key.split('_zerosum__')[0]]]),
        inputs=model.value_vars, on_unused_input='ignore')
    ip = model.initial_point()
    cols = []
    for i in range(n_free):
        pt = {k: np.zeros_like(np.asarray(v, dtype=float)) for k, v in ip.items()}
        z = np.zeros(n_free)
        z[i] = 1.0
        pt[key] = z
        cols.append(np.asarray(fn(pt)[0]).ravel())
    m = np.column_stack(cols)
    if np.abs(m.sum(axis=0)).max() > 1e-9:
        raise ValueError('recovered basis does not produce zero-sum vectors')
    return m


def zerosum_coords(basis: np.ndarray, target: np.ndarray) -> np.ndarray:
    """Coordinates in `basis` that reproduce the zero-sum vector `target`.

    Raises if `target` is not in the basis's span -- which means it was not
    zero-sum, and silently projecting it would hand the model a different
    configuration from the one asked for.
    """
    z, *_ = np.linalg.lstsq(basis, np.asarray(target, dtype=float), rcond=None)
    err = float(np.abs(basis @ z - target).max())
    if err > 1e-9:
        raise ValueError(
            f'target is not in the zero-sum span (residual {err:.2e}); '
            'it is probably not a zero-sum vector')
    return z


def orthogonal_transform(Xc: np.ndarray) -> np.ndarray:
    """Gram-Schmidt the *centred* design, returned as a change of basis.

    Returns an upper-triangular ``T`` such that ``Xorth = Xc @ T`` has
    orthogonal columns, each one carrying the SAME norm as the raw column it
    replaces. Because it is a change of basis and not a change of model, the
    sampled coefficients can be mapped straight back with ``beta = T @ theta``
    -- so every downstream consumer keeps working on raw columns and no
    fitted curve has to be re-derived.

    Two details are load-bearing:

    * **Centre first, orthogonalise second.** Gram-Schmidt on uncentred
      columns followed by centring destroys the orthogonality it just built,
      since subtracting a mean is itself a projection.
    * **Rescale to the original norms.** Plain QR returns unit columns.
      Orthogonalising shrinks a column (``gamma2_x``'s drops to 0.373x its
      raw norm), so a coefficient on the shrunken column has to grow by the
      inverse of that to describe the same curve -- while its PRIOR_SD entry
      sits unchanged. Left alone, that silently tightens the prior on the
      fitted *function* by a factor nobody wrote down. Restoring the norm
      keeps each prior meaning what it meant.

    Note this is not prior-preserving: independent priors on an orthogonal
    basis imply a correlated prior ``T diag(sd^2) T'`` on the raw
    coefficients. That is a deliberate modelling statement (independence
    asserted between directions the data can actually separate) rather than
    an accident, but it does mean the two parameterisations are not identical
    models -- see docs/inference-toolkit.md.
    """
    q, r = np.linalg.qr(np.asarray(Xc, dtype=float))
    # Sign convention: force diag(R) > 0, so orthogonal column j points the
    # same way as raw column j and its coefficient keeps the same sign.
    s = np.sign(np.diag(r))
    s[s == 0] = 1.0
    r = r * s[:, None]
    norms = np.linalg.norm(Xc, axis=0)
    return np.linalg.solve(r, np.diag(norms))


@dataclass
class DatasetV2:
    observations: pd.DataFrame
    users: pd.DataFrame
    climbs: pd.DataFrame
    label: str

    def summary(self) -> Dict[str, Any]:
        o = self.observations
        return {'label': self.label, 'n_obs': len(o), 'n_users': o['user_id'].nunique(),
                'n_gyms': o['gym_id'].nunique(), 'n_climbs': o['climb_id'].nunique(),
                'pct_multi_gym': float((o.groupby('user_id')['gym_id'].nunique() >= 2).mean()),
                'median_visits': float(o['n_visits'].median())}


def make_dataset(
    base: Dict[str, pd.DataFrame],
    gym_ids: Sequence[str],
    name_filter: str = 'all',
    confident_threshold: float = 0.05,
    min_name_counts: int = 100,
    label: str = '',
) -> DatasetV2:
    """Subset the base data to a gym network and (optionally) confident names.

    name_filter:
      'all'       -- every user, however ambiguous their name
      'confident' -- only p(gf) <= threshold or >= 1-threshold, with enough
                     name observations behind it. This is the decisive test
                     for whether the height finding survives once
                     gender-misclassification leakage is removed.
    """
    gym_ids = [str(g) for g in gym_ids]
    obs = base['observations']
    obs = obs[obs['gym_id'].isin(gym_ids)].copy()
    users = base['users']

    if name_filter == 'confident':
        p = users['p_gf']
        keep = ((p <= confident_threshold) | (p >= 1 - confident_threshold)) \
            & (users['name_counts'] >= min_name_counts)
        obs = obs[obs['user_id'].isin(users.index[keep])].copy()

    users = users.loc[users.index.isin(obs['user_id'].unique())].copy()
    climbs = base['climbs']
    climbs = climbs[climbs['climb_id'].isin(obs['climb_id'].unique())].reset_index(drop=True)
    return DatasetV2(obs.reset_index(drop=True), users, climbs, label or name_filter)


# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------

# Forms that are linear in their parameters, so the whole covariate block can
# be QR-reparameterized. 'saturating' cannot (h0 and s enter nonlinearly).
LINEAR_IN_PARAMS = {'zero', 'linear', 'linear_x_gender', 'quadratic',
                    'quadratic_x_gender'}


def _design_columns(height_form, h, a, gender, ape_quadratic, ape_x_gender=False):
    """Covariate design matrix and the coefficient name for each column.

    Height and ape index are near-independent in this data (r = +0.137), so
    their functional forms are specified separately rather than jointly.
    """
    cols, names = [gender], ['beta_gender']
    if height_form == 'linear':
        cols += [h]; names += ['gamma1']
    elif height_form == 'linear_x_gender':
        cols += [h, gender * h]; names += ['gamma1', 'gamma1_x']
    elif height_form == 'quadratic':
        cols += [h, h ** 2]; names += ['gamma1', 'gamma2']
    elif height_form == 'quadratic_x_gender':
        cols += [h, h ** 2, gender * h, gender * h ** 2]
        names += ['gamma1', 'gamma2', 'gamma1_x', 'gamma2_x']
    cols += [a]; names += ['delta1']
    if ape_quadratic:
        cols += [a ** 2]; names += ['delta2']
    if ape_x_gender:
        cols += [gender * a]; names += ['delta1_x']
        if ape_quadratic:
            cols += [gender * a ** 2]; names += ['delta2_x']
    return np.column_stack(cols), names


def _height_term(form, h, gender, prefix=''):
    """Return the height contribution to ability for a given functional form."""
    if form == 'zero':
        return 0.0
    if form == 'linear':
        g1 = pm.Normal(f'{prefix}gamma1', 0, 1)
        return g1 * h
    if form == 'linear_x_gender':
        # v1's claim ("height works differently for men and women") stated in
        # its cheapest form: one slope each, no curvature. Two parameters
        # against quadratic_x_gender's four, so it tests the interaction
        # without also paying for a bend the data may not support.
        g1 = pm.Normal(f'{prefix}gamma1', 0, 1)
        g1x = pm.Normal(f'{prefix}gamma1_x', 0, 0.5)
        return g1 * h + gender * (g1x * h)
    if form == 'quadratic':
        g1 = pm.Normal(f'{prefix}gamma1', 0, 1)
        g2 = pm.Normal(f'{prefix}gamma2', 0, 0.3)
        return g1 * h + g2 * h ** 2
    if form == 'quadratic_x_gender':
        # The published specification: separate quadratic for female-coded.
        g1 = pm.Normal(f'{prefix}gamma1', 0, 1)
        g2 = pm.Normal(f'{prefix}gamma2', 0, 0.3)
        g1x = pm.Normal(f'{prefix}gamma1_x', 0, 0.5)
        g2x = pm.Normal(f'{prefix}gamma2_x', 0, 0.15)
        return g1 * h + g2 * h ** 2 + gender * (g1x * h + g2x * h ** 2)
    if form == 'vertex_quadratic':
        # f = -kappa (h - p)^2, with p the peak height ITSELF rather than a
        # quantity derived from gamma1/gamma2. Mathematically the same family
        # as the plain quadratic -- centring never constrained the vertex,
        # which v1's own fit demonstrates by placing it ~9.9in below the
        # median -- but here the peak carries its own prior and its own
        # credible interval, so "where is the best height" is answered
        # directly instead of by propagating error through -gamma1/(2 gamma2).
        # When curvature is weak, kappa -> 0 and p becomes unidentified; that
        # shows up honestly as a very wide posterior on p rather than a
        # confident-looking number.
        #
        # vq_peak was N(0, 1.5) until 2026-08-06, and that number was quietly
        # doing the work this form exists to avoid. The posterior wanted the
        # optimum at -2.55 SD (~10 in below median height) -- 1.7 PRIOR SDs
        # out, with the prior only 3x wider than the posterior, contributing
        # ~12% of its precision and dragging the estimate in from about -2.9.
        # A form whose whole selling point is estimating the peak directly,
        # instead of propagating error through -gamma1/(2 gamma2), cannot have
        # a prior deciding where the peak may be. N(0, 3) spans the observed
        # height range, so the data locates it.
        kappa_h = pm.HalfNormal(f'{prefix}vq_curv', 0.3)
        peak = pm.Normal(f'{prefix}vq_peak', 0, 3.0)   # in SDs from median height
        return -kappa_h * (h - peak) ** 2
    if form == 'saturating':
        # A * logistic((h - h0)/s): monotone, bounded, saturating. Encodes
        # "reach helps until you have enough of it, then stops paying."
        # h is in z-units here, so the knee and width priors are too.
        #
        # Widened 2026-08-06 from N(0,1) / N(0,1.5) / HalfNormal(1.0) for the
        # same reason as vq_peak: all three sat parameters came back with the
        # prior only 2-3x wider than the posterior, so this form was being
        # compared against the others under materially tighter regularisation.
        # Three weakly-identified parameters make that easy to miss -- the
        # symptom is a well-behaved-looking posterior that is mostly prior.
        amp = pm.Normal(f'{prefix}sat_amp', 0, 2.0)
        h0 = pm.Normal(f'{prefix}sat_h0', 0, 3.0)    # knee, in SDs from median height
        s = pm.HalfNormal(f'{prefix}sat_scale', 2.0)
        return amp * pm.math.sigmoid((h - h0) / (s + 1e-6))
    raise ValueError(f'unknown height_form {form!r}')


def build_model_v2(
    dataset: DatasetV2,
    *,
    height_form: str = 'quadratic_x_gender',
    gender_mode: str = 'point',          # 'point' | 'marginalize'
    ape_quadratic: bool = True,
    ape_x_gender: bool = False,
    likelihood: str = 'emg',             # 'emg' | 'gap_latent'
    estimate_sigma_link: bool = True,
    sigma_link_fixed: float = 0.5,
    climb_quantization: bool = False,
    quant_halfwidth: float = 0.5,
    include_reliability: bool = True,
    use_n_at_max: bool = False,
    store_user_terms: bool = False,
    zero_sum_users: bool = False,
    marginalize_singles: bool = False,
    center_user_offsets: bool = False,
    marginalize_all: bool = False,
    n_quad: int = 31,
    orthogonal_design: bool = False,
    advancement: bool = False,
) -> pm.Model:
    """Build the v2 PyMC model.

    gender_mode:
      'point'       -- plug in w_female (P(female | name, height)) as a
                       continuous covariate. Correct for the linear main
                       effect, but biased for interactions, since
                       E[f(G)] != f(E[G]).
      'marginalize' -- treat sex as a latent binary per user and marginalize
                       it out exactly (NUTS cannot sample discrete
                       parameters). Handles the interaction correctly. Costs
                       two likelihood evaluations.
    """
    obs, users, climbs = dataset.observations, dataset.users, dataset.climbs

    user_ids = users.index.tolist()
    uidx = {u: i for i, u in enumerate(user_ids)}
    gym_ids = sorted(obs['gym_id'].unique())
    gidx = {g: i for i, g in enumerate(gym_ids)}
    climb_ids = climbs['climb_id'].tolist()
    cidx = {c: i for i, c in enumerate(climb_ids)}

    obs_u = obs['user_id'].map(uidx).to_numpy()
    obs_g = obs['gym_id'].map(gidx).to_numpy()
    obs_c = obs['climb_id'].map(cidx).to_numpy()
    m = obs['m'].to_numpy(float)
    # Centered on its own median, exactly as grading_model.py already does for
    # the reliability signal r -- and for the same reason it documents there
    # (rho was measured at corr = -0.91 with lambda0 before centering). Raw
    # n_visits has median ~7, so (1 + kappa*n) is ~(1 + 7*kappa) for a typical
    # user, making kappa little more than a rescaling of lambda0. That fix was
    # applied to rho but never to kappa, which sits in the same product.
    n_visits_raw = obs['n_visits'].to_numpy(float)
    nv_scale = float(np.nanmedian(n_visits_raw)) or 1.0
    n_visits = n_visits_raw / nv_scale - 1.0
    n_at_max_raw = obs['n_at_max'].to_numpy(float) if 'n_at_max' in obs else np.ones(len(obs))
    nm_scale = float(np.nanmedian(n_at_max_raw)) or 1.0
    n_at_max = n_at_max_raw / nm_scale - 1.0
    # Kept in raw grades on purpose: it is added straight to the ceiling, which
    # is also in grades, so there is nothing to scale it against.
    adv_offset = (advancement_offset(obs) if advancement else np.zeros(len(obs)))
    if advancement and not np.any(adv_offset):
        # Silent no-ops are how the retry budget shipped as a fix that changed
        # nothing. If the correction was asked for and cannot apply, say so.
        raise ValueError(
            'advancement=True but the offset is identically zero -- this '
            f'dataset has no usable {ADVANCEMENT_DATE_COL!r} column. Rebuild '
            'the snapshot with scripts/build_base_snapshot.py.')
    n_users = len(user_ids)

    # Centered AND standardized. The original model used raw centered inches,
    # so h_c**2 ran to 3136 while gamma2 ~ N(0, 0.3) -- a prior implying the
    # height quadratic could contribute +/-106 grades when the whole grade
    # range is ~12. That is both a nonsense prior and severe anisotropy for
    # NUTS. In z-units h_z**2 is O(1) and the priors below mean what they say.
    height = users['height'].to_numpy(float)
    ape = users['ape_index'].to_numpy(float)
    h_sd = float(np.nanstd(height)) or 1.0
    a_sd = float(np.nanstd(ape)) or 1.0
    h_obs = ~np.isnan(height)
    a_obs = ~np.isnan(ape)
    h_c = np.nan_to_num((height - np.nanmedian(height)) / h_sd, nan=0.0)
    a_c = np.nan_to_num((ape - np.nanmedian(ape)) / a_sd, nan=0.0)
    # Missingness INDICATORS rather than pretending the missing are average.
    # 15.8% of heights and 43.9% of ape indices are absent; zero-filling them
    # asserts a measurement that was never taken, piles ~44% of users onto a
    # single covariate value, attenuates the slope toward zero, and drags the
    # intercept with it (measured: design-column means of 0.85/0.64, beta0 at
    # R-hat 1.19 / ESS 16). With an indicator the slope is identified only by
    # users who actually reported a value, while the missing group gets its
    # own offset -- unbiased under missing-at-random, and one parameter each
    # instead of the ~19k latents full imputation would add.
    h_miss = (~h_obs).astype(float)
    a_miss = (~a_obs).astype(float)
    # Kept as an inline record of the standardization constants this fit used --
    # the numbers quoted in the comment above are only meaningful next to them.
    # Not currently returned; noqa rather than deleted so the record survives.
    scales = {'h_sd': h_sd, 'a_sd': a_sd,  # noqa: F841
              'h_median': float(np.nanmedian(height)), 'a_median': float(np.nanmedian(ape)),
              'h_missing_frac': float(h_miss.mean()), 'a_missing_frac': float(a_miss.mean())}
    w_female = users['w_female'].fillna(0.5).to_numpy(float)

    nsps = users['n_sends_per_sesh'].to_numpy(float)
    r_scale = np.nanmedian(nsps)
    r_user = (nsps / r_scale - 1.0) if r_scale else np.zeros_like(nsps)
    r_obs = np.nan_to_num(r_user[obs_u], nan=0.0)
    median_m = float(np.nanmedian(m))

    coords = {'user': user_ids, 'gym': gym_ids, 'climb': climb_ids, 'obs': np.arange(len(obs))}

    if marginalize_all:
        # Integrating every climber out is the same closed form for the
        # singles plus quadrature for the rest, so it needs everything
        # marginalize_singles needs and then some.
        marginalize_singles = True

    if marginalize_singles:
        if gender_mode != 'point':
            raise NotImplementedError(
                'marginalize_singles needs gender_mode="point": the latent-sex '
                'branch mixes at user level, and a marginalized single has no '
                'user-level term left to mix.')
        if zero_sum_users:
            raise NotImplementedError(
                'marginalize_singles is incompatible with zero_sum_users: the '
                'zero-sum constraint couples every climber, so an offset that '
                'has been integrated out still appears in the constraint.')
        # Which observations belong to a climber with only this one row.
        counts = np.bincount(obs_u, minlength=n_users)
        single_obs = np.flatnonzero(counts[obs_u] == 1)
        multi_obs = np.flatnonzero(counts[obs_u] > 1)
        multi_users = np.flatnonzero(counts > 1)
        remap = np.full(n_users, -1)
        remap[multi_users] = np.arange(len(multi_users))
        multi_seg = remap[obs_u[multi_obs]]
        coords['obs_single'] = single_obs
        coords['obs_multi'] = multi_obs
        coords['user_multi'] = [user_ids[i] for i in multi_users]
        if marginalize_all:
            # Reorders multi_obs so each climber's rows are contiguous; the
            # likelihood below must index with layout.obs, not multi_obs.
            layout = build_layout(obs_u, multi_obs, n_quad=n_quad)

    with pm.Model(coords=coords) as model:
        beta0 = pm.Normal('beta0', mu=median_m, sigma=5)
        sigma_user = pm.HalfNormal('sigma_user', sigma=2)
        # Same argument as the gym corrections below, applied to climbers.
        # With a plain Normal the model can add c to beta0 and subtract c from
        # all N user offsets and land in nearly the same place -- the prior
        # penalises that only weakly, since the realized mean of N standard
        # normals drifts with sd 1/sqrt(N). That soft ridge is the most likely
        # cause of beta0's poor mixing (measured on v3_conf: r_hat 1.09,
        # ESS 45, chain means 5.61/5.63/5.63/5.64 sitting on distinct levels),
        # while the ZeroSumNormal gym corrections converged at r_hat 1.00-1.04.
        # Off by default so the queued height-form comparison stays like-for-like.
        if marginalize_all:
            # No climber offset is sampled at all -- see marginal_pt. This is
            # what takes the model from 4,241 parameters to 40 and removes the
            # sigma_user/epsilon funnel entirely rather than re-coordinatising
            # around it.
            eps_multi = None
            epsilon = None
        elif marginalize_singles:
            # Offsets exist only for climbers who have more than one row. The
            # rest are integrated out in closed form at the likelihood, so
            # sampling them would be sampling a parameter the data cannot
            # distinguish from noise.
            if center_user_offsets:
                # CENTERED. Which parameterization samples better is not a
                # matter of taste -- it depends on how much data each climber
                # carries. Non-centered wins when groups are data-poor;
                # centered wins when the likelihood dominates the prior
                # (Betancourt & Girolami 2015).
                #
                # Measured on net50/confident: the likelihood:prior information
                # ratio is 21x at the 10th percentile and 64x at the 90th, and
                # the data dominates for 100% of the 4,201 multi-row climbers.
                # That is the centered regime by a wide margin, and the
                # non-centered fits show exactly the damage: corr(log
                # sigma_user, spread of epsilon_raw) = -0.847, because a
                # tightly-pinned epsilon forces z = epsilon/sigma to shrink as
                # sigma grows. Step size collapsed to 0.003 and 100% of
                # iterations truncated at the tree-depth ceiling.
                #
                # The usual objection -- centered funnels as sigma -> 0 -- does
                # not apply here: sigma_user's posterior is 1.627 +/- 0.015,
                # far from zero and tightly determined.
                eps_multi = pm.Normal('epsilon', 0, sigma_user, dims='user_multi')
            else:
                eps_raw = pm.Normal('epsilon_raw', 0, 1, dims='user_multi')
                eps_multi = sigma_user * eps_raw
            epsilon = None
        elif zero_sum_users:
            eps_raw = pm.ZeroSumNormal('epsilon_raw', sigma=1, dims='user')
            epsilon = sigma_user * eps_raw
        else:
            eps_raw = pm.Normal('epsilon_raw', 0, 1, dims='user')
            epsilon = sigma_user * eps_raw
        # Not a Deterministic by default: at 16k users x 2k draws that is
        # ~250MB of stored trace per user-dimensioned quantity, and it is
        # recoverable post-hoc as sigma_user * epsilon_raw.
        if store_user_terms and epsilon is not None:
            pm.Deterministic('epsilon', epsilon, dims='user')

        # --- covariate block, built as an explicitly centered design matrix ---
        # Centering every column makes beta0 orthogonal to all slopes. Before
        # this, h**2 and a**2 had means of 0.85 and 0.64, so the intercept was
        # entangled with every coefficient and mixed terribly.
        Xcols, Xnames = _design_columns(
            height_form, h_c, a_c, w_female, ape_quadratic, ape_x_gender)
        Xcols = np.column_stack([Xcols, h_miss, a_miss])
        Xnames = Xnames + ['beta_h_missing', 'beta_a_missing']
        X_mean = Xcols.mean(axis=0)
        Xc = Xcols - X_mean

        if orthogonal_design:
            # Sample on an orthogonal basis, report on the raw one. The design
            # columns are badly collinear where they interact with gender --
            # g*h against g*h**2 sits at -0.899 on quadratic_x_gender, and the
            # block's condition number is 36 -- and NUTS's diagonal mass matrix
            # cannot represent a rotation no matter how long it tunes.
            #
            # theta is what the sampler moves; beta = T @ theta is what the
            # model means. Substituting beta for theta below leaves every other
            # line of this function untouched, and the Deterministics keep the
            # raw-basis names pointing at raw-basis quantities so no downstream
            # reader of the trace has to know this happened.
            T = orthogonal_transform(Xc)
            thetas = [pm.Normal(f'{nm}_orth', 0, PRIOR_SD.get(nm, 1.0))
                      for nm in Xnames]
            beta_vec = pt.dot(pt.as_tensor_variable(T), pt.stack(thetas))
            coefs = [pm.Deterministic(nm, beta_vec[i])
                     for i, nm in enumerate(Xnames)]
        else:
            coefs = [pm.Normal(nm, 0, PRIOR_SD.get(nm, 1.0)) for nm in Xnames]
            beta_vec = pt.stack(coefs)
        # Columns involving gender are rebuilt per-branch in marginalize mode,
        # so keep the pieces separate rather than collapsing to one dot product.
        gender_cols = [i for i, nm in enumerate(Xnames)
                       if nm == 'beta_gender' or nm.endswith('_x')]
        static_idx = [i for i in range(len(Xnames)) if i not in gender_cols]
        static_term = pt.dot(Xc[:, static_idx], beta_vec[static_idx])

        sigma_gym = pm.HalfNormal('sigma_gym', sigma=1.5)
        # ZeroSumNormal, not Normal: the model is identified only up to an
        # additive shift between ability and gym correction (design doc
        # section 6). A zero-MEAN prior anchors that only softly -- with G
        # gyms the realized mean still drifts with sd sigma_gym/sqrt(G),
        # leaving a near-flat ridge that NUTS crawls along (measured: tree
        # depth pinned at the maximum of 10, 1023 leapfrog steps per draw,
        # step size 0.016, zero divergences -- textbook elongated geometry).
        # Summing to zero by construction removes the direction entirely,
        # and is exactly what "anchor to the average gym" was meant to mean.
        gym_raw = pm.ZeroSumNormal('gym_correction_raw', sigma=1, dims='gym')
        gym_correction = pm.Deterministic('gym_correction', sigma_gym * gym_raw, dims='gym')
        # Mean-centered view: the model is only identified up to an additive
        # shift between ability and correction (design doc section 6), so
        # gym-vs-gym differences are the meaningful quantity, not levels.
        pm.Deterministic('gym_correction_c', gym_correction - pt.mean(gym_correction),
                         dims='gym')

        correction = gym_correction[obs_g]
        if advancement:
            # Data, not a parameter. Each observation's ceiling is shifted by
            # the grades that climber had gained (or not yet gained) by the day
            # of that send, relative to their own other sends. It rides on
            # `correction` because that is the one term every likelihood branch
            # below shares. Fixed, never fitted -- see kaya.advancement.
            correction = correction + adv_offset
        if climb_quantization:
            # Bounded per-climb offset: grades are continuous, labels are
            # integers, so a climb truly at 5.3 labelled 5 understates every
            # sender by the same 0.3. Physically |offset| <= 0.5, which is
            # why this cannot eat sigma_user the way the previous unbounded
            # Normal/StudentT/horseshoe priors did.
            q = pm.Uniform('climb_quant', -quant_halfwidth, quant_halfwidth, dims='climb')
            correction = correction - q[obs_c]

        sigma_link = (pm.HalfNormal('sigma_link', sigma=1.0) if estimate_sigma_link
                      else sigma_link_fixed)

        # Log-link rate. The original multiplicative form (1 + kappa*n) can go
        # NEGATIVE once n is centered (a 1-visit user sits at -0.875, so any
        # kappa > 1.14 breaks the Exponential), and its coefficients are only
        # interpretable relative to a raw-unit baseline. exp() is positive by
        # construction, needs no sign constraint, and with mean-zero centered
        # covariates lambda0 is cleanly "the rate for a typical user" instead
        # of being entangled with kappa and rho.
        log_lambda0 = pm.Normal('log_lambda0', mu=0.0, sigma=1.0)
        log_rate = log_lambda0
        if include_reliability:
            kappa = pm.Normal('kappa', 0, 0.5)
            rho = pm.Normal('rho', 0, 0.5)
            log_rate = log_rate + kappa * n_visits + rho * r_obs
        if use_n_at_max:
            # Repeatedly topping out at the same grade is direct evidence the
            # ceiling is near it, so it raises the rate (shrinking the gap).
            psi = pm.Normal('psi', 0, 0.5)
            log_rate = log_rate + psi * n_at_max
        rate = pm.math.exp(log_rate)
        pm.Deterministic('lambda0', pm.math.exp(log_lambda0))
        nu = 1.0 / rate      # ExGaussian's nu is the exponential's MEAN

        name_to_coef = {nm: coefs[i] for i, nm in enumerate(Xnames)}
        col_of = {nm: i for i, nm in enumerate(Xnames)}

        def ability_for(gender_vec, use_static=True):
            """Ability, with the gender-dependent design columns rebuilt.

            In marginalize mode gender_vec is a hard 0/1 per branch rather
            than the plug-in probability, so every column containing gender
            has to be recomputed; the gender-free columns (height main
            effects, ape, missingness indicators) are shared and precomputed.
            Each rebuilt column keeps the SAME centering constant as the
            fitted design matrix, so beta0 stays orthogonal in both branches.
            """
            term = beta0 if epsilon is None else beta0 + epsilon
            if use_static:
                term = term + static_term
            for nm in Xnames:
                if nm != 'beta_gender' and not nm.endswith('_x'):
                    continue
                j = col_of[nm]
                if nm == 'beta_gender':
                    col = gender_vec
                elif nm == 'gamma1_x':
                    col = gender_vec * h_c
                elif nm == 'gamma2_x':
                    col = gender_vec * h_c ** 2
                elif nm == 'delta1_x':
                    col = gender_vec * a_c
                elif nm == 'delta2_x':
                    col = gender_vec * a_c ** 2
                else:
                    raise ValueError(f'unhandled gender column {nm!r}')
                term = term + name_to_coef[nm] * (col - X_mean[j])
            if height_form not in LINEAR_IN_PARAMS:
                # Nonlinear-in-parameters forms (saturating, vertex_quadratic)
                # cannot live in the design matrix, so they are added here.
                # Guarding on the set rather than naming one form is what a
                # smoke test caught: vertex_quadratic was silently contributing
                # nothing because only 'saturating' was routed through.
                term = term + _height_term(height_form, h_c, gender_vec)
            return term

        def obs_logp(ability):
            ceiling = ability[obs_u] + correction
            # m = ceiling - gap + noise  =>  -m = Normal(-ceiling, s) + Exp
            return pm.logp(pm.ExGaussian.dist(mu=-ceiling, sigma=sigma_link, nu=nu), -m)

        if gender_mode == 'marginalize':
            # Sex is a latent binary per user; marginalize exactly (NUTS
            # cannot sample discrete parameters). Ability is shared across a
            # user's observations, so the mixture must be taken at USER
            # level, not per observation -- hence the segment-sum before
            # logsumexp. inc_subtensor accumulates over repeated indices.
            ll_f = obs_logp(ability_for(pt.ones(n_users)))
            ll_m = obs_logp(ability_for(pt.zeros(n_users)))
            user_ll_f = pt.inc_subtensor(pt.zeros(n_users)[obs_u], ll_f)
            user_ll_m = pt.inc_subtensor(pt.zeros(n_users)[obs_u], ll_m)
            logw_f = np.log(np.clip(w_female, 1e-6, 1 - 1e-6))
            logw_m = np.log(np.clip(1 - w_female, 1e-6, 1 - 1e-6))
            stacked = pt.stack([logw_f + user_ll_f, logw_m + user_ll_m])
            per_user = pt.logsumexp(stacked, axis=0)
            pm.Potential('marginal_lik', pt.sum(per_user))
            # Leave-one-USER-out is the right LOO unit here anyway, since a
            # user's observations share both epsilon and the latent sex.
            pm.Deterministic('log_lik_user', per_user, dims='user')
            if store_user_terms:
                pm.Deterministic('p_female_post',
                                 pt.exp(logw_f + user_ll_f - per_user), dims='user')
        elif marginalize_all:
            # Singles in closed form, exactly as the marginalize_singles branch
            # below. The multi-observation climbers get a one-dimensional
            # adaptive Gauss-Hermite quadrature over their shared offset --
            # a Potential rather than an observed node, because the quantity
            # is a per-CLIMBER marginal and PyMC has no distribution for it.
            ability0 = ability_for(w_female, use_static=True)   # no epsilon
            ceiling0 = ability0[obs_u] + correction
            sigma_single = pt.sqrt(sigma_link ** 2 + sigma_user ** 2)
            pm.ExGaussian('m_single', mu=-ceiling0[single_obs],
                          sigma=sigma_single, nu=nu[single_obs],
                          observed=-m[single_obs], dims='obs_single')
            o = layout.obs
            per_user = multi_log_integral_pt(
                pt.as_tensor_variable(m[o]), ceiling0[o], nu[o],
                sigma_link, sigma_user, layout)
            # Named so az.loo and the diagnostics can find it; the pointwise
            # unit here is a climber, not an observation, which is the honest
            # unit once a climber's rows share an integrated-out offset.
            pm.Deterministic('log_lik_multi', per_user)
            pm.Potential('m_multi_marginal', pt.sum(per_user))
        elif marginalize_singles:
            # 59% of climbers contribute exactly one observation, and their
            # offset can absorb that observation entirely -- which is what
            # makes leave-one-out meaningless for those rows (8,400 effective
            # parameters, and two fits of the identical model scoring 31.1
            # elpd apart). Integrate those offsets out instead.
            #
            # It is exact. epsilon enters the ceiling additively and is
            # Gaussian, the observation noise is Gaussian, so the two convolve
            # into one wider Gaussian and the density stays ExGaussian:
            #
            #   -m ~ ExGaussian(-c, sqrt(sigma_link^2 + sigma_user^2), nu)
            #
            # Nothing is approximated and nothing is dropped -- sigma_user is
            # still estimated, from the multi-observation climbers who
            # actually carry information about it.
            ability0 = ability_for(w_female, use_static=True)   # no epsilon
            ceiling0 = ability0[obs_u] + correction
            sigma_single = pt.sqrt(sigma_link ** 2 + sigma_user ** 2)

            pm.ExGaussian('m_single', mu=-ceiling0[single_obs],
                          sigma=sigma_single, nu=nu[single_obs],
                          observed=-m[single_obs], dims='obs_single')
            pm.ExGaussian('m_multi',
                          mu=-(ceiling0[multi_obs] + eps_multi[multi_seg]),
                          sigma=sigma_link, nu=nu[multi_obs],
                          observed=-m[multi_obs], dims='obs_multi')
        else:
            ability = ability_for(w_female)
            if store_user_terms:
                pm.Deterministic('ability', ability, dims='user')
            # A real observed node (not a Potential): PyMC then derives the
            # pointwise log-likelihood itself, so az.loo / posterior
            # predictive checks work without hand-built log_lik terms.
            ceiling = ability[obs_u] + correction
            pm.ExGaussian('m_obs', mu=-ceiling, sigma=sigma_link, nu=nu,
                          observed=-m, dims='obs')

    return model
