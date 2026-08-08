"""The ledger of every effect that could distort a gym grading correction.

The model's headline output is one number per gym: how much harder a V5 is
there than at the average gym. Everything that makes two climbers' logged
grades differ for a reason OTHER than the gym's grading is a threat to that
number. This script assembles every such effect that has been identified, in a
single structure, whether or not the model currently does anything about it.

Each factor carries the same five fields, in the order a reader needs them:

  evidence   what was measured, and how -- never an assertion on its own
  models     the competing mental models, including the ones ruled out
  why        the mechanism by which it contaminates a gym correction
  magnitude  how big, in grades, against the 0.408-grade spread of the
             corrections themselves -- the only scale that makes it meaningful
  handling   what the model does now, and what it would take to do better,
             with the actual equation

Writing them down together is the point. Four of these were discovered by
accident while looking for something else, and the reason they were missable is
that nothing enumerated them. A factor that is deliberately ignored is a
decision; a factor nobody listed is a bug waiting to be found by someone else.

Most numbers are read from payloads that already computed them --
v2_structure.json (compression, gym drift) and v2_time.json (advancement) --
rather than recomputed, so this file cannot drift from the pages that already
show them. What it computes fresh is the material with no home yet: the
exposure curve implied by the fitted posterior, the timing bias per gym pair,
the height comparison, and the gender-coding distribution.

Writes src/kaya/viewer_static/v2_factors.json.

    python scripts/build_v2_factors.py

Run from the repo root: src/kaya/secrets.py shadows the stdlib module numpy's
bit generator imports, so running from inside src/kaya breaks numpy.
"""
import itertools
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

from kaya.grading_model_v2 import make_dataset

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
STATIC = ROOT / 'src' / 'kaya' / 'viewer_static'
OUT = STATIC / 'v2_factors.json'

NETWORK = 'net50'
NAME_FILTER = 'confident'
REFERENCE_FIT = 'v10_lin_marg'
PAIR_MIN_SHARED = 25          # climbers a gym pair needs before its gap means anything

# The empirical grade-change-per-year-of-separation, used to convert a date gap
# into grades. Flat on purpose here: the proper rate varies with ability
# (0.338 - 0.035*v) and the per-pair mix of abilities is not modelled, so this
# is the average and is labelled as such wherever it is shown.
FLAT_RATE = 0.185


def _gym_names():
    """gym_id -> name, from the discovery roster rather than the send rows.

    A static config file, so it cannot go stale relative to a database or pick
    up a gym's renamed variant the way a mode over the send rows can.
    """
    roster = json.loads(
        (ROOT / 'src' / 'kaya' / 'config' / 'gyms_available.json').read_text())
    return {str(g['id']): g['name'] for g in roster}


def coverage(obs):
    """Confirm the snapshot is fully dated before anything trusts its dates.

    This used to report a 49.8% shortfall and describe it as the blocker ahead
    of all time-resolved work. That was wrong, and worth recording why: the
    figure came from reading `data/kaya_data.db`, an abandoned June 2025 copy,
    instead of the live mirror at LOCAL_DB_URL. The live mirror holds 2.46M
    sends with zero null dates and covers 100% of the modelled pairs. There
    was never a data gap — only an aggregation that discarded dates and a
    stale path that made the loss look like missing data.

    Kept as an assertion rather than deleted: `max_send_date` is now load
    bearing, and a snapshot rebuilt from a partial source would quietly
    reintroduce exactly the problem that did not exist.
    """
    d = pd.to_datetime(obs.max_send_date, errors='coerce')
    dated = int(d.notna().sum())
    return {
        'model_pairs': int(len(obs)),
        'dated_pairs': dated,
        'pct': round(100 * dated / len(obs), 1),
        'missing': int(len(obs)) - dated,
        'date_min': str(d.min().date()) if dated else None,
        'date_max': str(d.max().date()) if dated else None,
    }


def _observations():
    """The modelled observations, which now carry their own dates.

    Earlier this opened a sqlite file directly. That was a mistake with a
    consequence: the path it used, `data/kaya_data.db`, is an abandoned June
    2025 copy, while the live mirror lives at LOCAL_DB_URL and is four times
    the size. Every dated figure computed from it covered about half the
    modelled pairs, and the shortfall looked like a real data gap rather than a
    wrong filename.

    There is no database here any more. `prepare_base_data` carries
    `max_send_date`, `first_send` and `last_send` through the aggregation, so
    the dates arrive with the observations and cannot disagree with the rows
    they belong to. Rebuild the snapshot with scripts/build_base_snapshot.py.
    """
    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets[NETWORK], name_filter=NAME_FILTER,
                      label='factors')
    obs = ds.observations.copy()
    if 'max_send_date' not in obs.columns:
        raise SystemExit(
            'base_bouldering.pkl carries no dates. Rebuild it first:\n'
            '    python scripts/build_base_snapshot.py'
        )
    obs['max_send_date'] = pd.to_datetime(obs.max_send_date, errors='coerce')
    return obs, ds.users


# ------------------------------------------------------------------ exposure

def exposure(obs):
    """How far below their ceiling the model assumes a climber logged.

    The likelihood is ex-Gaussian: observed max = ceiling - Exponential(nu) +
    Normal. `nu` is the expected shortfall and shrinks with exposure, so this
    reads the fitted posterior back out as a curve in days-at-that-gym, which
    is the form the effect is actually intuitive in.
    """
    import arviz as az
    p = az.from_netcdf(RUNS / 'traces' / f'idata_{REFERENCE_FIT}.nc').posterior
    ll0, kappa = float(p['log_lambda0'].mean()), float(p['kappa'].mean())
    nv = obs.n_visits.to_numpy(float)
    scale = float(np.nanmedian(nv)) or 1.0

    def shortfall(days):
        return float(np.exp(-(ll0 + kappa * (days / scale - 1.0))))

    days = [1, 2, 3, 5, 10, 25, 50, 100, 200]
    ratios = []
    for _, g in obs.groupby('user_id'):
        if len(g) < 2:
            continue
        v = np.sort(g.n_visits.values)
        if v[0] > 0:
            ratios.append(v[-1] / v[0])
    ratios = np.array(ratios)

    return {
        'fit': REFERENCE_FIT, 'log_lambda0': round(ll0, 4),
        'kappa': round(kappa, 4), 'nv_scale': scale,
        'curve': [{'days': d, 'shortfall': round(shortfall(d), 3)} for d in days],
        'pct_le5': round(100 * float((nv <= 5).mean()), 1),
        'pct_ge100': round(100 * float((nv >= 100).mean()), 1),
        'nv_median': float(np.median(nv)),
        'nv_p90': float(np.percentile(nv, 90)),
        'ratio_median': round(float(np.median(ratios)), 1),
        'ratio_p90': round(float(np.percentile(ratios, 90)), 1),
        'contrast': round(shortfall(3) - shortfall(100), 3),
        'n_cells': int(len(obs)),
    }


# ---------------------------------------------------------------- advancement

def timing_bias(obs):
    """Grades of climber improvement that land in a gym correction.

    The model compares a climber's hardest send at gym A with their hardest at
    gym B and attributes the whole difference to the gyms. When those two sends
    are years apart the climber improved in between, and that improvement is
    booked as grading. This measures the date gap PER GYM PAIR, because a gap
    only biases a correction insofar as one gym is systematically the later one
    -- a pair whose ordering is symmetric cancels, and the average gap across
    all pairs would badly overstate the damage.

    Reads `max_send_date` straight off the observations: that is by definition
    the date of the row the model turns into a number, so there is no join to
    get wrong and no second source to fall out of step with.
    """
    names = _gym_names()
    per_gym = obs.groupby('gym_id').agg(
        n=('m', 'size'), mean_date=('max_send_date', 'mean'))
    per_gym['yr'] = (per_gym.mean_date.dt.year
                     + per_gym.mean_date.dt.dayofyear / 365.25)

    rows = []
    for _, g in obs.groupby('user_id'):
        if len(g) < 2:
            continue
        ids = g.gym_id.values
        dts = g.max_send_date.values
        for i, j in itertools.combinations(range(len(g)), 2):
            a, b = (i, j) if ids[i] < ids[j] else (j, i)
            rows.append((ids[a], ids[b],
                         (dts[a] - dts[b]) / np.timedelta64(365, 'D')))
    df = pd.DataFrame(rows, columns=['a', 'b', 'dt'])
    agg = df.groupby(['a', 'b']).agg(n=('dt', 'size'), mean_dt=('dt', 'mean'))
    agg = agg[agg.n >= PAIR_MIN_SHARED].copy()
    agg['bias'] = agg.mean_dt.abs() * FLAT_RATE
    agg = agg.sort_values('bias', ascending=False)
    gaps = df.dt.abs().values

    worst = [{'a': str(names.get(a, a))[:34], 'b': str(names.get(b, b))[:34],
              'n': int(r.n), 'gap': round(float(r.mean_dt), 2),
              'bias': round(float(r.bias), 3)}
             for (a, b), r in agg.head(6).iterrows()]

    return {
        'n_pairs_total': int(len(df)),
        'gap_median': round(float(np.median(gaps)), 2),
        'gap_mean': round(float(gaps.mean()), 2),
        'gap_over_1y': round(100 * float((gaps > 1).mean()), 1),
        'gap_over_2y': round(100 * float((gaps > 2).mean()), 1),
        'rate_used': FLAT_RATE,
        'n_gym_pairs': int(len(agg)),
        'min_shared': PAIR_MIN_SHARED,
        'bias_median': round(float(agg.bias.median()), 3),
        'bias_p90': round(float(agg.bias.quantile(0.9)), 3),
        'bias_max': round(float(agg.bias.max()), 3),
        'worst': worst,
        'gym_date_sd': round(float(per_gym.yr.std()), 2),
        'gym_date_range': round(float(per_gym.yr.max() - per_gym.yr.min()), 2),
    }


# --------------------------------------------------------------------- height

def height(users, obs):
    """Re-run the model-free height comparison so the page cannot go stale."""
    import probe_height_forms as ph

    ability, corr = ph.two_way(obs)
    u = users.loc[ability.index].copy()
    u['ability'] = ability.values
    u['n_obs'] = obs.groupby('user_id').size().reindex(ability.index).values
    d = u[u.height.notna() & u.w_female.notna()].copy()

    a_missing = d.ape_index.isna().values.astype(float)
    ape = d.ape_index.fillna(d.ape_index.median()).values
    h = d.height.values - d.height.median()
    a = ape - np.median(ape)
    g, y = d.w_female.values, d.ability.values
    w = d.n_obs.values.astype(float)
    n = len(y)

    X_by = {f: ph.design(f, h, a, g, a_missing) for f in ph.FORMS}
    sig = {lbl: [] for _, _, lbl in ph.PAIRS}
    winners = []
    for s in range(ph.N_SEEDS):
        fold = np.random.default_rng(ph.BASE_SEED + s).permutation(n) % ph.N_FOLDS
        e = ph.cv_errors(X_by, y, w, fold)
        winners.append(min(ph.FORMS, key=lambda f: e[f].mean()))
        for lo, hi, lbl in ph.PAIRS:
            sig[lbl].append(ph.sigmas(e, lo, hi, n))

    beta = np.linalg.lstsq(X_by['quadratic_x_gender'] * np.sqrt(w)[:, None],
                           y * np.sqrt(w), rcond=None)[0]
    g1, g2, g1x, g2x = beta[2], beta[3], beta[4], beta[5]
    grid = np.linspace(-8, 8, 33)
    curves = []
    for lbl, gv in [('male-coded', 0.0), ('female-coded', 1.0)]:
        s1, s2 = g1 + gv * g1x, g2 + gv * g2x
        yy = s1 * grid + s2 * grid ** 2
        curves.append({'label': lbl, 'slope': round(float(s1), 4),
                       'curv': round(float(s2), 5),
                       'span': round(float(yy.ptp()), 2),
                       'x': [round(float(v), 2) for v in grid],
                       'y': [round(float(v - yy.mean()), 4) for v in yy]})

    return {
        'n': int(n), 'n_seeds': ph.N_SEEDS,
        'median_height': float(d.height.median()),
        'comparisons': [
            {'label': lbl, 'median': round(float(np.median(sig[lbl])), 2),
             'min': round(float(np.min(sig[lbl])), 2),
             'max': round(float(np.max(sig[lbl])), 2),
             'clears': int(np.sum(np.array(sig[lbl]) > 2))}
            for _, _, lbl in ph.PAIRS],
        'winner': max(set(winners), key=winners.count),
        'winner_count': int(max(winners.count(w_) for w_ in set(winners))),
        'curves': curves,
    }


# --------------------------------------------------------------------- gender

def gender(users, obs):
    """Gender is inferred from a first name, so it is a probability, not a fact."""
    u = users.loc[users.index.isin(set(obs.user_id))]
    p = u.w_female.dropna().to_numpy(float)
    return {
        'n': int(len(p)),
        'confident_female': round(100 * float((p >= 0.9).mean()), 1),
        'confident_male': round(100 * float((p <= 0.1).mean()), 1),
        'ambiguous': round(100 * float(((p > 0.1) & (p < 0.9)).mean()), 1),
        'mean': round(float(p.mean()), 3),
    }


# --------------------------------------------------------------- quantization

def quantization(obs):
    """Grades are integers, so a ceiling is only ever seen to the nearest V.

    `n_at_max` and `n_sends_gym` already ride on the observations, so this needs
    no send-level data -- which also means it cannot disagree with the rows the
    model actually fits.
    """
    at_max = obs.n_at_max.to_numpy(float)
    return {
        'pct_once': round(100 * float((at_max == 1).mean()), 1),
        'pct_10plus': round(100 * float((at_max >= 10).mean()), 1),
        'n_cells': int(len(obs)),
        'pct_sends_used': round(100 * len(obs) / float(obs.n_sends_gym.sum()), 1),
        'n_sends': int(obs.n_sends_gym.sum()),
    }


def main():
    struct = json.loads((STATIC / 'v2_structure.json').read_text())
    tm = json.loads((STATIC / 'v2_time.json').read_text())
    obs, users = _observations()

    payload = {
        'built_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M'),
        'network': NETWORK, 'name_filter': NAME_FILTER,
        'reference_fit': REFERENCE_FIT,
        # every magnitude on the page is quoted against this
        'correction_sd': struct['drift']['correction_sd'],
        'correction_range': struct['drift']['correction_range'],
        'coverage': coverage(obs),
        'exposure': exposure(obs),
        'advancement': {'fit': tm['advancement']['fit'],
                        'debiased': tm['advancement']['debiased'],
                        'n_climbers': tm['advancement']['n_climbers'],
                        'n_pairs': tm['advancement']['n_pairs'],
                        'timing': timing_bias(obs)},
        'compression': struct['compression'],
        'drift': struct['drift'],
        'height': height(users, obs),
        'gender': gender(users, obs),
        'quantization': quantization(obs),
    }
    OUT.write_text(json.dumps(payload, indent=1))
    a = payload['advancement']['timing']
    print(f'wrote {OUT.relative_to(ROOT)}  ({OUT.stat().st_size / 1024:.1f} KB)')
    print(f"  exposure   3d vs 100d = {payload['exposure']['contrast']:+.3f} grades")
    print(f"  advancement bias median {a['bias_median']:.3f}, max {a['bias_max']:.3f}")
    print(f"  compression tau {payload['compression']['linear']['tau']}, "
          f"drift tau {payload['drift']['het']['tau']}")
    print(f"  height winner {payload['height']['winner']} "
          f"{payload['height']['winner_count']}/{payload['height']['n_seeds']}")
    c = payload['coverage']
    print(f"  DATE COVERAGE {c['pct']}% — {c['dated_pairs']:,} of "
          f"{c['model_pairs']:,} modelled pairs have a dated send locally; "
          f"{c['missing']:,} do not")


if __name__ == '__main__':
    main()
