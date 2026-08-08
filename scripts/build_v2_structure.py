"""Is one number per gym enough? Measures the two ways it is not.

The model gives each gym a single grading correction. That assumes the gym's
offset is the same for a V2 climber and a V9 climber, and the same in 2021 and
2026. Both assumptions are testable without fitting anything, and both fail.

Two effects, measured by the same identity on perpendicular axes. For a climber
who appears at two gyms, the difference of their two levels cancels the climber
entirely -- ability, height, gender, and improvement all subtract away -- leaving
a statement about the two gyms alone:

    level_A - level_B  =  correction_A - correction_B  +  noise

  * regress that difference on the CLIMBER'S ABILITY -> grade compression
  * regress that difference on the CALENDAR DATE      -> gym drift

Neither needs a model, a sampler, or a prior. Both need the same care: 100-odd
noisy per-pair slopes will scatter even when every gym is identical, so the
scatter has to be split into real variation and estimation noise before any of
it means anything. That split is Cochran's Q / I^2 / tau, and it is the reason
this file is longer than the two regressions it runs.

Writes src/kaya/viewer_static/v2_structure.json.

Run from the repo root -- running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import collections
import itertools
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
from scipy import stats

from kaya.data_access import KayaDataAccessor
from kaya.grading_model_v2 import BOULDER_GRADE_TO_NUM, make_dataset

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_structure.json'

NETWORK = 'net50'
NAME_FILTER = 'confident'

WINDOW_D = 90          # days per activity window for the drift design
MIN_SENDS = 3          # sends needed before a (climber, gym, window) cell counts
DRIFT_MIN_PAIRS = 25   # per gym pair; see connectivity note below
COMP_MIN_PAIRS = 40    # per gym pair for the compression design
CURV_MIN_PAIRS = 60    # curvature needs more per pair than the slope does
N_POWER_REPS = 2000
RNG_SEED = 20260807


# ---------------------------------------------------------------- heterogeneity

def heterogeneity(est, var):
    """Split observed scatter into real variation and estimation noise.

    Returns Cochran's Q, its degrees of freedom, I^2 and tau. The whole method
    rests on one fact: if a per-pair estimate differs from the pooled mean only
    by noise, then (estimate - mean) / SE is a standard normal, so its square
    averages exactly 1. Summing those squares over k pairs therefore averages
    k - 1 (one degree of freedom is spent estimating the mean). Q far above
    that is disagreement the error bars cannot account for.

    tau converts the excess back into the original units. Setting E[Q] under
    real variation tau^2 gives E[Q] = df + tau^2 * C, so tau^2 = (Q - df) / C
    with C = sum(w) - sum(w^2)/sum(w) and w = 1/SE^2 the precision. Floored at
    zero: there is no negative variance, and Q below df is ordinary sampling
    luck rather than evidence of unusually good agreement.
    """
    est, var = np.asarray(est, float), np.asarray(var, float)
    k = len(est)
    w = 1.0 / var
    mu = float((w * est).sum() / w.sum())
    Q = float((w * (est - mu) ** 2).sum())
    df = k - 1
    C = float(w.sum() - (w ** 2).sum() / w.sum())
    tau2 = max(0.0, (Q - df) / C) if C > 0 else 0.0
    return {
        'k': k, 'df': df, 'Q': round(Q, 1),
        'crit': round(float(stats.chi2.ppf(0.95, df)), 1),
        'p': float(stats.chi2.sf(Q, df)),
        'pooled': round(mu, 4),
        'pooled_se': round(float(np.sqrt(1.0 / w.sum())), 4),
        'i2': round(100 * max(0.0, (Q - df) / Q), 0) if Q > 0 else 0.0,
        'tau': round(float(np.sqrt(tau2)), 4),
        'per_gym': round(float(np.sqrt(tau2 / 2)), 4),
        'se_median': round(float(np.sqrt(np.median(var))), 4),
    }


def power_curve(var, taus, reps=N_POWER_REPS, seed=RNG_SEED):
    """How small an effect this design could have caught.

    Detecting something is only half an answer without knowing what would have
    been missed -- a null result means "smaller than the floor", never "zero".
    Simulated rather than approximated, using the actual per-pair standard
    errors, because the asymptotic form is a weighted sum of noncentral
    chi-squares and not worth trusting at k ~ 100.
    """
    var = np.asarray(var, float)
    k = len(var)
    w = 1.0 / var
    crit = stats.chi2.ppf(0.95, k - 1)
    rng = np.random.default_rng(seed)
    out = []
    for tau in taus:
        hits = 0
        for _ in range(reps):
            e = rng.normal(0.0, np.sqrt(tau ** 2 + var))
            mu = (w * e).sum() / w.sum()
            hits += (w * (e - mu) ** 2).sum() > crit
        out.append({'tau': tau, 'power': round(hits / reps, 3)})
    return out


def power_floor(curve, target=0.80):
    """Linear interpolation to the tau reaching `target` power."""
    for a, b in zip(curve, curve[1:]):
        if a['power'] < target <= b['power']:
            f = (target - a['power']) / (b['power'] - a['power'])
            return round(a['tau'] + f * (b['tau'] - a['tau']), 4)
    return None


def _slope(y, cols, which=0):
    """Weighted-free OLS slope on `which` column, with its sampling variance."""
    A = np.vstack(cols + [np.ones(len(y))]).T
    beta, *_ = np.linalg.lstsq(A, y, rcond=None)
    resid = y - A @ beta
    dof = len(y) - A.shape[1]
    if dof <= 0:
        return None
    v = ((resid ** 2).sum() / dof) * np.linalg.inv(A.T @ A)[which, which]
    return float(beta[which]), float(v)


# ---------------------------------------------------------------- compression

def load_observations():
    with open(RUNS / 'base_bouldering.pkl', 'rb') as f:
        base = pickle.load(f)
    nets = json.loads((RUNS / 'networks.json').read_text())['networks']
    ds = make_dataset(base, nets[NETWORK], name_filter=NAME_FILTER, label='structure')
    return ds.observations


def compression(obs):
    """Does a gym's correction depend on how strong the climber is?

    Ability is the climber's mean grade across their gyms, which shares noise
    with the outcome: cov(m_A - m_B, (m_A + m_B)/2) = (var_A - var_B)/2, so a
    slope appears by construction whenever the two gyms differ in spread. The
    clean replication below removes that by measuring ability from gyms OUTSIDE
    the pair, which is only possible for climbers who appear at 3+ gyms.
    """
    abar = float(obs.m.mean())

    pairs = collections.defaultdict(list)
    for _, grp in obs.groupby('user_id'):
        if len(grp) < 2:
            continue
        ms, gs = grp.m.values, grp.gym_id.values
        a = float(ms.mean())
        for i, j in itertools.combinations(range(len(grp)), 2):
            if gs[i] == gs[j]:
                continue
            lo, hi = (i, j) if gs[i] < gs[j] else (j, i)
            pairs[(gs[lo], gs[hi])].append((ms[lo] - ms[hi], a))

    lin_e, lin_v, spans, ns = [], [], [], []
    for v in pairs.values():
        if len(v) < COMP_MIN_PAIRS:
            continue
        d = np.array([x[0] for x in v], float)
        a = np.array([x[1] for x in v], float)
        if a.std() < 0.5:
            continue
        r = _slope(d, [a - abar])
        if r is None:
            continue
        lin_e.append(r[0]); lin_v.append(r[1])
        spans.append(float(a.max() - a.min())); ns.append(len(d))

    lin = heterogeneity(lin_e, lin_v)
    lin['n_obs'] = int(sum(ns))
    lin['n_median'] = int(np.median(ns))
    lin['span_median'] = round(float(np.median(spans)), 1)
    lin['power'] = power_curve(lin_v, [0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.10])
    lin['floor'] = power_floor(lin['power'])

    # curvature, on the pairs big enough to support a second-order term
    cur_e, cur_v = [], []
    for v in pairs.values():
        if len(v) < CURV_MIN_PAIRS:
            continue
        d = np.array([x[0] for x in v], float)
        a = np.array([x[1] for x in v], float) - abar
        if a.std() < 0.5:
            continue
        r = _slope(d, [a ** 2, a], which=0)
        if r is None:
            continue
        cur_e.append(r[0]); cur_v.append(r[1])
    cur = heterogeneity(cur_e, cur_v)
    cur['power'] = power_curve(cur_v, [0.005, 0.01, 0.02, 0.03, 0.05])
    cur['floor'] = power_floor(cur['power'])

    # artifact-free replication: ability from gyms outside the pair
    n_by_user = obs.groupby('user_id').size()
    elig = set(n_by_user[n_by_user >= 3].index)
    cpairs = collections.defaultdict(list)
    for uid, grp in obs.groupby('user_id'):
        if uid not in elig:
            continue
        ms, gs = grp.m.values, grp.gym_id.values
        for i, j in itertools.combinations(range(len(grp)), 2):
            if gs[i] == gs[j]:
                continue
            others = [ms[k] for k in range(len(grp)) if k not in (i, j)]
            if not others:
                continue
            lo, hi = (i, j) if gs[i] < gs[j] else (j, i)
            cpairs[(gs[lo], gs[hi])].append((ms[lo] - ms[hi], float(np.mean(others))))
    ce, cv = [], []
    for v in cpairs.values():
        if len(v) < 30:
            continue
        d = np.array([x[0] for x in v], float)
        a = np.array([x[1] for x in v], float)
        if a.std() < 0.5:
            continue
        r = _slope(d, [a - abar])
        if r is None:
            continue
        ce.append(r[0]); cv.append(r[1])
    clean = heterogeneity(ce, cv)
    clean['n_climbers'] = len(elig)

    p10, p90 = np.percentile(obs.m, [10, 90])
    return {
        'linear': lin, 'curvature': cur, 'clean': clean,
        'ability_p10': round(float(p10), 1), 'ability_p90': round(float(p90), 1),
        'differential': round(float(2 * lin['per_gym'] * (p90 - p10)), 2),
        'n_climbers_2plus': int((n_by_user >= 2).sum()),
        'n_climbers_3plus': len(elig),
    }


# ---------------------------------------------------------------------- drift

def load_sends(gyms):
    acc = KayaDataAccessor()
    s = acc.read_sends(source='local_db',
                       columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type'],
                       parse_dates=False, order_by=False)
    ct = s['climb_type'].fillna('').astype(str).str.lower()
    s = s[ct.str.contains('boulder')].copy()
    s['m'] = s['grade'].map(BOULDER_GRADE_TO_NUM)
    s = s[s['m'].notna() & s['user_id'].notna() & s['gym_id'].notna()]
    s['date'] = pd.to_datetime(s['date'], errors='coerce', utc=True).dt.tz_localize(None)
    s = s[s['date'].notna()].copy()
    for c in ('user_id', 'gym_id'):
        s[c] = s[c].astype(str)
    return s[s.gym_id.isin(gyms)].copy()


def _drift_pairs(sends, window_d):
    """(gym_a, gym_b) -> list of (level difference, calendar date, log-n difference,
    within-window date gap), one entry per climber-window that saw both gyms."""
    t0 = sends.date.min()
    s = sends.assign(w=((sends.date - t0).dt.days // window_d).astype(int))
    cell = (s.groupby(['user_id', 'gym_id', 'w'])
             .agg(lvl=('m', 'max'), n=('m', 'size'), mid=('date', 'mean')).reset_index())
    cell = cell[cell.n >= MIN_SENDS].copy()
    cell['t'] = (cell.mid - t0).dt.days / 365.25
    cell['ln'] = np.log(cell.n)
    out = collections.defaultdict(list)
    for _, g in cell.groupby(['user_id', 'w']):
        if len(g) < 2:
            continue
        g = g.sort_values('gym_id')
        L, G, T, N = g.lvl.values, g.gym_id.values, g.t.values, g.ln.values
        for i, j in itertools.combinations(range(len(g)), 2):
            out[(G[i], G[j])].append((L[i] - L[j], T[i], N[i] - N[j], T[i] - T[j]))
    return out, cell


def _drift_slopes(pairs, thr, control_gap):
    """Per-pair relative drift rate, controlling for effort and optionally for
    the within-window date gap.

    log(send count) is always controlled: a window's max grade rises with how
    many sends went into it, so a pair whose send counts drift over time would
    fake a grading drift.
    """
    rows = []
    for (a, b), v in pairs.items():
        if len(v) < thr:
            continue
        arr = np.array(v, float)
        d, t, dn, dg = arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3]
        if t.std() < 0.3:
            continue
        cols = [t, dn] + ([dg] if control_gap else [])
        r = _slope(d, cols)
        if r is None:
            continue
        rows.append({'a': a, 'b': b, 'slope': r[0], 'var': r[1],
                     'n': len(d), 'span': float(t.max() - t.min()),
                     'gap': float(np.abs(dg).mean())})
    return rows


def _components(rows):
    """Connected components of the gym-comparison graph.

    Every measurement is a DIFFERENCE, so per-gym rates only exist if the
    comparisons chain across all gyms. If the graph splits, the offset between
    groups is unknowable in principle -- and least squares will not say so, it
    will return enormous standard errors and per-gym rates inflated by the
    unconstrained gaps. Checked before solving, never after.
    """
    adj = collections.defaultdict(set)
    for r in rows:
        adj[r['a']].add(r['b']); adj[r['b']].add(r['a'])
    seen, comps = set(), []
    for g in sorted(adj):
        if g in seen:
            continue
        stack, comp = [g], set()
        while stack:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x)
            stack.extend(y for y in adj[x] if y not in comp)
        seen |= comp
        comps.append(len(comp))
    return sorted(comps, reverse=True)


def _solve_per_gym(rows, tau2):
    """Pairwise differences -> per-gym rates, zero-sum, shrunk at the measured tau.

    The ridge term is not a regularisation convenience: it IS the hierarchical
    posterior mean under a Normal(0, tau/sqrt(2)) prior on the per-gym rates,
    which is exactly what the heterogeneity step just estimated.
    """
    gyms = sorted({g for r in rows for g in (r['a'], r['b'])})
    gi = {g: i for i, g in enumerate(gyms)}
    X = np.zeros((len(rows), len(gyms)))
    y = np.array([r['slope'] for r in rows])
    w = np.array([1.0 / r['var'] for r in rows])
    for i, r in enumerate(rows):
        X[i, gi[r['a']]] = 1.0
        X[i, gi[r['b']]] = -1.0
    lam = 1.0 / (tau2 / 2) if tau2 > 0 else 1e3
    W = np.diag(w)
    P = X.T @ W @ X + lam * np.eye(len(gyms)) + 1e4 * np.ones((len(gyms), len(gyms)))
    beta = np.linalg.solve(P, X.T @ W @ y)
    se = np.sqrt(np.diag(np.linalg.inv(P)))
    return pd.DataFrame({'gym_id': gyms, 'rate': beta, 'se': se})


def drift(sends, gym_names, corrections, t_c):
    pairs, cell = _drift_pairs(sends, WINDOW_D)
    rows = _drift_slopes(pairs, DRIFT_MIN_PAIRS, control_gap=False)
    het = heterogeneity([r['slope'] for r in rows], [r['var'] for r in rows])
    het['n_median'] = int(np.median([r['n'] for r in rows]))
    het['span_median'] = round(float(np.median([r['span'] for r in rows])), 1)
    het['power'] = power_curve([r['var'] for r in rows],
                               [0.02, 0.05, 0.075, 0.10, 0.15, 0.20])
    het['floor'] = power_floor(het['power'])

    # the connectivity check, at the threshold that fails and the one that works
    strict = _drift_slopes(pairs, 40, control_gap=False)
    connectivity = {
        'strict_threshold': 40, 'strict_components': _components(strict),
        'used_threshold': DRIFT_MIN_PAIRS, 'used_components': _components(rows),
    }

    # robustness: within-window improvement cannot be what is producing tau
    robust = []
    for wd in (WINDOW_D, WINDOW_D // 2):
        wp, _ = _drift_pairs(sends, wd)
        for cg in (False, True):
            rr = _drift_slopes(wp, DRIFT_MIN_PAIRS, control_gap=cg)
            h = heterogeneity([r['slope'] for r in rr], [r['var'] for r in rr])
            robust.append({'window_days': wd, 'gap_controlled': cg,
                           'k': h['k'], 'Q': h['Q'], 'df': h['df'],
                           'i2': h['i2'], 'tau': h['tau'],
                           'gap_days': round(float(np.mean([r['gap'] for r in rr]) * 365.25), 0)})

    tau2 = het['tau'] ** 2
    per = _solve_per_gym(rows, tau2)
    span = float((sends.date.max() - sends.date.min()).days / 365.25)

    abs_t = cell.groupby('gym_id').t.mean()
    per['abs_t'] = per.gym_id.map(abs_t)
    per['name'] = per.gym_id.map(gym_names)
    per['correction'] = per.gym_id.map(corrections)
    per['t_c'] = per.gym_id.map(t_c)
    per['accum'] = per.rate * (per.abs_t - per.abs_t.mean())

    d2 = per.dropna(subset=['correction'])
    A = np.vstack([d2.t_c.values, np.ones(len(d2))]).T
    raw_slope = float(np.linalg.lstsq(A, d2.correction.values, rcond=None)[0][0])
    adj = d2.correction.values - d2.accum.values
    adj_slope = float(np.linalg.lstsq(A, adj, rcond=None)[0][0])

    ordered = per.sort_values('rate')
    return {
        'het': het,
        'connectivity': connectivity,
        'robust': robust,
        'span_years': round(span, 1),
        'n_sends': int(len(sends)),
        'n_climbers': int(sends.user_id.nunique()),
        'date_min': str(sends.date.min().date()),
        'date_max': str(sends.date.max().date()),
        'window_days': WINDOW_D,
        'per_gym_sd': round(float(per.rate.std()), 4),
        'per_gym_se_median': round(float(per.se.median()), 4),
        'route1': round(float(het['tau'] / np.sqrt(2)), 4),
        'accumulated_span': round(float(per.rate.std() * span), 2),
        'mean_date_sd': round(float(abs_t.std()), 2),
        'mean_date_range': round(float(abs_t.max() - abs_t.min()), 2),
        'accum_sd': round(float(d2.accum.std()), 3),
        'correction_sd': round(float(d2.correction.std()), 3),
        'correction_range': round(float(d2.correction.max() - d2.correction.min()), 2),
        'confound': {
            'raw_r': round(float(np.corrcoef(d2.t_c, d2.correction)[0, 1]), 3),
            'raw_slope': round(raw_slope, 3),
            'adj_r': round(float(np.corrcoef(d2.t_c, adj)[0, 1]), 3),
            'adj_slope': round(adj_slope, 3),
            'rate_vs_correction_r': round(float(np.corrcoef(d2.rate, d2.correction)[0, 1]), 3),
            'accum_vs_correction_r': round(float(np.corrcoef(d2.accum, d2.correction)[0, 1]), 3),
        },
        'stiffening': [{'name': r['name'] or r.gym_id, 'rate': round(r.rate, 3),
                        'se': round(r.se, 3)} for _, r in ordered.tail(5)[::-1].iterrows()],
        'softening': [{'name': r['name'] or r.gym_id, 'rate': round(r.rate, 3),
                       'se': round(r.se, 3)} for _, r in ordered.head(5).iterrows()],
    }


def main():
    obs = load_observations()
    print(f'observations: {len(obs):,} rows, {obs.gym_id.nunique()} gyms')

    comp = compression(obs)
    lin, cur = comp['linear'], comp['curvature']
    print(f"compression  slope: k={lin['k']} Q={lin['Q']}/{lin['df']} "
          f"I2={lin['i2']}% tau={lin['tau']} floor={lin['floor']}")
    print(f"compression  curve: k={cur['k']} Q={cur['Q']}/{cur['df']} "
          f"I2={cur['i2']}% tau={cur['tau']} floor={cur['floor']}")
    print(f"compression  clean: k={comp['clean']['k']} I2={comp['clean']['i2']}% "
          f"tau={comp['clean']['tau']}")

    tv = json.loads((ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_time.json').read_text())
    gt = tv['gym_time']['gyms']
    names = {g['id']: g['g'] for g in gt}
    corrections = {g['id']: g['m'] for g in gt}
    t_c = {g['id']: g['t_c'] for g in gt}

    sends = load_sends(set(obs.gym_id.astype(str)))
    print(f'sends: {len(sends):,} dated boulder sends')
    dr = drift(sends, names, corrections, t_c)
    h = dr['het']
    print(f"drift: k={h['k']} Q={h['Q']}/{h['df']} I2={h['i2']}% tau={h['tau']} "
          f"floor={h['floor']}  per-gym {dr['per_gym_sd']} vs route1 {dr['route1']}")
    print(f"  components at 40: {dr['connectivity']['strict_components']}  "
          f"at {DRIFT_MIN_PAIRS}: {dr['connectivity']['used_components']}")
    print(f"  confound slope {dr['confound']['raw_slope']} -> {dr['confound']['adj_slope']}")

    payload = {
        'network': NETWORK, 'name_filter': NAME_FILTER,
        'compression': comp, 'drift': dr,
    }
    OUT.write_text(json.dumps(payload, indent=1, default=float))
    print(f'wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KB)')


if __name__ == '__main__':
    main()
