"""The emcee run itself, made readable: chains, posteriors, and why it is slow.

`compare_samplers.py` already answers "do the samplers agree" with a table of
means. This answers the question underneath it -- *what did the second sampler
actually do* -- which is the part that justifies trusting the first one.

emcee is an ensemble sampler: 128 walkers explore at once, each proposing moves
by looking at where the other walkers are, using no gradient at all. That makes
it a genuinely independent check on PyMC/NUTS, which is gradient-driven and
runs a different implementation of the same likelihood (PyTensor graph versus
plain NumPy). It also makes its diagnostics different in kind, which is why
they need their own section rather than a row in an existing table:

  * There is no R-hat in the usual sense. 128 walkers are not 4 independent
    chains -- they interact by construction -- so R-hat across walkers measures
    something weaker than it does for NUTS. It is exported anyway, labelled for
    what it is, because a walker parked somewhere the rest never went still
    shows up in it.
  * The number that governs everything is the **integrated autocorrelation
    time** tau: how many steps the ensemble takes to produce one draw's worth
    of new information. 1.92 million draws at tau = 542 is ~3,500 independent
    samples, and the gap between those two numbers is the entire story of this
    run.

Exports, per parameter:
  * posterior mean, sd, quantiles, tau, effective sample size
  * a thinned trace for a subset of walkers -- the chain, as drawn
  * the running mean, with the spread across walkers' own running means, which
    is where "has it settled" is actually visible
  * the autocorrelation function, which is what tau is a summary of
  * PyMC's posterior for the same parameter, for an overlay

Plus the 29 gym corrections reconstructed from the zero-sum basis, since those
are the numbers the page is ultimately about and neither sampler stores them in
a directly comparable form.

Writes src/kaya/viewer_static/v2_emcee.json. Run from the repo root.
"""
import argparse
import json
import pickle
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

from kaya.viewer_paths import data_file, trace_file

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_emcee.json'

# How much of the chain crosses to the browser. 8 walkers is enough to see
# whether they sit on top of each other; 400 steps is enough to see texture.
# The full array is 15,000 x 128 x 40, which is 614 MB in memory and would be
# ~350 MB of JSON.
TRACE_WALKERS = 8
TRACE_STEPS = 400
RUNMEAN_POINTS = 200
ACF_LAGS = 60
DENS_BINS = 80

# emcee samples an unconstrained vector; PyMC samples the natural scale for the
# two sigmas. Compared on the log scale, which is where emcee is exploring.
PYMC_DERIVED = {'log_sigma_user': 'sigma_user', 'log_sigma_gym': 'sigma_gym'}

# Plain-language name and units for each parameter, so the picker is readable
# without cross-referencing the symbol table. Two lengths on purpose: the long
# one is for the dropdown, which has a whole line to itself, and the short one
# is for the table, where a three-line wrap in the first column triples every
# row's height. Anything not listed falls back to its raw name (the 28 gym_raw
# coordinates are basis coefficients with no standalone meaning -- see `gyms`
# in the payload for what they add up to).
LABELS = {
    'beta0': ('baseline ability', 'baseline ability', 'grades'),
    'log_sigma_user': ('log spread of ability between climbers',
                       'ability spread', 'log grades'),
    'beta_gender': ('gender effect on ability', 'gender effect', 'grades'),
    'gamma1': ('height slope', 'height slope', 'grades / SD'),
    'gamma2': ('height curvature', 'height curvature', 'grades / SD²'),
    'delta1': ('ape-index slope', 'ape-index slope', 'grades / SD'),
    'delta2': ('ape-index curvature', 'ape curvature', 'grades / SD²'),
    'beta_h_missing': ('offset for climbers with no height',
                       'no-height offset', 'grades'),
    'beta_a_missing': ('offset for climbers with no wingspan',
                       'no-wingspan offset', 'grades'),
    'log_sigma_gym': ('log spread of grading style across gyms',
                      'gym-style spread', 'log grades'),
    'log_lambda0': ('log baseline gap rate', 'baseline gap rate',
                    'log 1/grades'),
    'kappa': ('ceiling found per extra visit', 'ceiling per visit', '—'),
    'rho': ('effect of logging completeness on the gap',
            'logging completeness', '—'),
}


def label_for(name):
    """(long label, short label, unit)."""
    if name in LABELS:
        return LABELS[name]
    if name.startswith('gym_raw['):
        k = name[len('gym_raw['):-1]
        return (f'zero-sum basis coordinate {k}', f'basis coord {k}', 'SDs')
    return (name, name, '—')


def autocorr_1d(x):
    """Normalised autocorrelation of one 1-D series, by FFT.

    Averaged over walkers by the caller. This is the function emcee's tau is
    an integral of, and it is worth plotting directly: tau = 542 as a bare
    number invites the reading "the chain is broken", whereas the curve shows
    a slow, smooth decay -- correlated, not stuck.
    """
    x = np.asarray(x, float) - np.mean(x)
    n = 1 << (2 * len(x) - 1).bit_length()
    f = np.fft.fft(x, n=n)
    acf = np.fft.ifft(f * np.conjugate(f))[:len(x)].real
    return acf / acf[0] if acf[0] > 0 else np.zeros_like(acf)


def walker_rhat(x):
    """Gelman-Rubin across walkers, from the definition. x is (walkers, steps).

    Reported with a caveat rather than as a pass/fail: emcee's walkers propose
    moves from each other's positions, so they are not the independent chains
    R-hat assumes. Between-walker variance is therefore biased *down* relative
    to four NUTS chains started from different points -- a small R-hat here is
    weaker evidence than the same number would be there. It still catches the
    failure it is being asked about: a walker stuck somewhere the ensemble
    never visited.
    """
    m, n = x.shape
    means = x.mean(axis=1)
    b = n / (m - 1) * ((means - means.mean()) ** 2).sum()
    w = float(x.var(axis=1, ddof=1).mean())
    if w <= 0:
        return 1.0
    return float(np.sqrt(((n - 1) / n * w + b / n) / w))


def density(samples, lo=None, hi=None, bins=DENS_BINS):
    """Histogram as a normalised density, on a caller-supplied common range."""
    s = np.asarray(samples, float)
    if lo is None:
        lo, hi = float(np.percentile(s, 0.2)), float(np.percentile(s, 99.8))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return [], []
    y, edges = np.histogram(s, bins=bins, range=(lo, hi), density=True)
    x = 0.5 * (edges[:-1] + edges[1:])
    return [round(float(v), 5) for v in x], [round(float(v), 5) for v in y]


def r4(a):
    return [round(float(v), 4) for v in np.asarray(a).ravel()]


def load_pymc(name):
    """PyMC's posterior for the shared scalars, plus its gym corrections."""
    f = trace_file(name)
    if not f.exists():
        print(f'-- PyMC trace {f.name} not found; overlays will be omitted')
        return None, None
    import arviz as az
    post = az.from_netcdf(str(f)).posterior
    draws = {}
    for v in post.data_vars:
        if post[v].values.ndim == 2:
            draws[v] = post[v].values.ravel()
    for log_name, nat in PYMC_DERIVED.items():
        if nat in draws:
            draws[log_name] = np.log(draws[nat])
    gym = None
    if 'gym_correction_c' in post:
        gym = (post['gym_correction_c'].values.reshape(-1, post.sizes['gym']),
               [str(g) for g in post.coords['gym'].values])
    return draws, gym


def gym_names():
    """gym_id -> (name, brand), from the annotated correction table."""
    import csv
    src = data_file('gymcorr_net50_conf.csv')
    if not src.exists():
        return {}
    with open(src) as f:
        return {r['gym_id']: (r['gym'], r['brand']) for r in csv.DictReader(f)}


def gym_block(chain_flat, names, height_form, n_quad, pymc_gym):
    """Reconstruct the 29 per-gym corrections from the 28 sampled coordinates.

    Neither sampler stores these in a comparable form. emcee samples free
    coordinates in an orthonormal zero-sum basis and multiplies by sigma_gym at
    likelihood time; PyMC stores the corrections directly. Rebuilding the model
    is the only way to get the basis, so this needs the dataset -- if it is not
    on disk the rest of the payload is still built.
    """
    base_f = data_file('base_bouldering.pkl')
    nets_f = data_file('networks.json')
    if not (base_f.exists() and nets_f.exists()):
        print('-- dataset not found; skipping the gym-correction comparison')
        return []
    from kaya.grading_model_v2 import make_dataset
    from kaya.marginal_v2 import MarginalModel
    with open(base_f, 'rb') as f:
        base = pickle.load(f)
    nets = json.loads(nets_f.read_text())['networks']
    ds = make_dataset(base, nets['net50'], name_filter='confident',
                      label='net50/confident')
    mm = MarginalModel.from_dataset(ds, height_form=height_form,
                                    sigma_link_fixed=0.5, n_quad=n_quad)
    if list(mm.param_names) != list(names):
        print('-- rebuilt model does not match the saved parameter names; '
              'skipping the gym-correction comparison')
        return []

    idx = {nm: j for j, nm in enumerate(names)}
    free = np.column_stack([chain_flat[:, idx[f'gym_raw[{i}]']]
                            for i in range(mm.n_gyms - 1)])
    sigma = np.exp(chain_flat[:, idx['log_sigma_gym']])
    corr = sigma[:, None] * (free @ mm.gym_basis.T)      # (draws, n_gyms)

    # Same ordering the model uses (marginal_v2 sorts the unique gym ids); the
    # basis columns are meaningless against any other order.
    ids = [str(g) for g in sorted(ds.observations['gym_id'].unique())]
    look = gym_names()
    pm_draws, pm_ids = (pymc_gym if pymc_gym else (None, None))
    pm_col = {g: k for k, g in enumerate(pm_ids)} if pm_ids else {}

    rows = []
    for k, gid in enumerate(ids):
        x = corr[:, k]
        name, brand = look.get(gid, (f'gym {gid}', 'Unknown'))
        row = {'i': gid, 'g': name, 'b': brand,
               'm': round(float(x.mean()), 4),
               'lo': round(float(np.percentile(x, 5.5)), 4),
               'hi': round(float(np.percentile(x, 94.5)), 4)}
        if pm_draws is not None and gid in pm_col:
            y = pm_draws[:, pm_col[gid]]
            row.update(pm=round(float(y.mean()), 4),
                       pm_lo=round(float(np.percentile(y, 5.5)), 4),
                       pm_hi=round(float(np.percentile(y, 94.5)), 4))
        rows.append(row)
    rows.sort(key=lambda r: r['m'])
    return rows


def sigma_gym_check():
    """scripts/check_sigma_gym.py's findings, if it has been run."""
    f = RUNS / 'results' / 'sigma_gym_check.json'
    if not f.exists():
        print('-- runs/results/sigma_gym_check.json not found; the page will '
              'omit the sigma_gym callout (run scripts/check_sigma_gym.py)')
        return None
    return json.loads(f.read_text())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--emcee', default='lin2',
                    help='run tag; reads runs/results/emcee_<tag>.npz')
    ap.add_argument('--pymc', default='v3_lin_marg',
                    help='PyMC fit of the same model, for the overlays')
    ap.add_argument('--burn', type=int, default=5000,
                    help='burn-in already discarded when the npz was written; '
                         'recorded for the page, not applied again')
    ap.add_argument('--moves', default='DE + DE-snooker')
    args = ap.parse_args()

    src = RUNS / 'results' / f'emcee_{args.emcee}.npz'
    if not src.exists():
        raise SystemExit(f'{src} not found -- run scripts/run_emcee.py first')
    d = np.load(src, allow_pickle=True)
    names = [str(s) for s in d['param_names']]
    chain = d['chain']                                   # (steps, walkers, dim)
    tau = np.asarray(d['tau'], float)
    acc = np.asarray(d['acceptance'], float)
    n_steps, n_walk, n_dim = chain.shape
    flat = chain.reshape(-1, n_dim)
    print(f'{src.name}: {n_steps:,} kept steps x {n_walk} walkers x {n_dim} '
          f'parameters = {len(flat):,} draws')

    pymc_draws, pymc_gym = load_pymc(args.pymc)

    # ---- indices for the thinned exports ------------------------------
    w_step = max(1, n_walk // TRACE_WALKERS)
    w_idx = list(range(0, n_walk, w_step))[:TRACE_WALKERS]
    s_step = max(1, n_steps // TRACE_STEPS)
    s_idx = np.arange(0, n_steps, s_step)[:TRACE_STEPS]
    rm_idx = np.unique(np.linspace(20, n_steps - 1, RUNMEAN_POINTS).astype(int))
    acf_step = max(1, int(np.nanmax(tau) * 4 / ACF_LAGS))
    acf_lags = np.arange(0, ACF_LAGS * acf_step, acf_step)

    params, trace, runmean, acf, dens, walker_means = [], {}, {}, {}, {}, {}
    for j, nm in enumerate(names):
        x = chain[:, :, j]                               # (steps, walkers)
        f = flat[:, j]
        t = max(float(tau[j]), 1.0)
        label, short, unit = label_for(nm)
        row = {
            'name': nm, 'label': label, 'short': short, 'unit': unit,
            'mean': round(float(f.mean()), 5),
            'sd': round(float(f.std(ddof=1)), 5),
            'q5': round(float(np.percentile(f, 5.5)), 5),
            'q50': round(float(np.percentile(f, 50)), 5),
            'q95': round(float(np.percentile(f, 94.5)), 5),
            'tau': round(t, 1),
            'ess': round(len(f) / t, 1),
            'rhat_walkers': round(walker_rhat(x.T), 4),
            'shared': nm in (pymc_draws or {}),
        }
        # Common horizontal range for the two samplers, so the overlay is a
        # comparison rather than two plots side by side.
        lo = float(np.percentile(f, 0.2))
        hi = float(np.percentile(f, 99.8))
        if pymc_draws and nm in pymc_draws:
            y = pymc_draws[nm]
            lo = min(lo, float(np.percentile(y, 0.2)))
            hi = max(hi, float(np.percentile(y, 99.8)))
            row['pm_mean'] = round(float(y.mean()), 5)
            row['pm_sd'] = round(float(y.std(ddof=1)), 5)
            # The gap between samplers, measured against the width of the
            # posterior -- the only scale on which "do they agree" has an
            # answer. Monte Carlo error would call almost anything a
            # discrepancy at 3,500 effective draws.
            row['gap_sd'] = round(abs(row['mean'] - row['pm_mean'])
                                  / max(row['sd'], 1e-12), 4)
        params.append(row)

        trace[nm] = [r4(x[s_idx, w]) for w in w_idx]
        # Cumulative mean per walker, then the ensemble mean and the spread
        # across walkers. A settled chain shows the band closing on a flat line.
        cum = np.cumsum(x, axis=0) / np.arange(1, n_steps + 1)[:, None]
        sel = cum[rm_idx]
        runmean[nm] = {
            'mean': r4(sel.mean(axis=1)),
            'lo': r4(np.percentile(sel, 5, axis=1)),
            'hi': r4(np.percentile(sel, 95, axis=1)),
        }
        # Autocorrelation, averaged over walkers -- the per-walker curves are
        # far noisier than the quantity tau is computed from.
        a = np.mean([autocorr_1d(x[:, w]) for w in range(0, n_walk, 8)], axis=0)
        acf[nm] = r4(a[acf_lags])
        ex, ey = density(f, lo, hi)
        cell = {'x': ex, 'y': ey}
        if pymc_draws and nm in pymc_draws:
            _, py = density(pymc_draws[nm], lo, hi)
            cell['pm_y'] = py
        dens[nm] = cell
        walker_means[nm] = r4(x.mean(axis=0))

    worst = max(params, key=lambda p: p['tau'])
    ess_min = min(p['ess'] for p in params)
    shared = [p for p in params if p.get('gap_sd') is not None]
    payload = {
        'run': {
            'tag': args.emcee,
            'pymc': args.pymc,
            'height_form': str(d['height_form']),
            'n_quad': int(d['n_quad']),
            'walkers': n_walk,
            'steps_kept': n_steps,
            'burn': args.burn,
            'steps_total': n_steps + args.burn,
            'n_params': n_dim,
            'draws': int(len(flat)),
            'elapsed_min': round(float(d['elapsed_min']), 1),
            'moves': args.moves,
            'acc_mean': round(float(acc.mean()), 4),
            'acc_min': round(float(acc.min()), 4),
            'acc_max': round(float(acc.max()), 4),
            'tau_max': round(float(np.nanmax(tau)), 1),
            'tau_max_param': worst['name'],
            'tau_median': round(float(np.nanmedian(tau)), 1),
            'ess_min': round(ess_min, 1),
            # emcee refuses to call a chain converged unless it is 50x tau.
            'steps_wanted': int(np.ceil(50 * np.nanmax(tau))),
            'converged': bool(n_steps + args.burn >= 50 * np.nanmax(tau)),
            'worst_gap_sd': (round(max(p['gap_sd'] for p in shared), 4)
                             if shared else None),
            'n_shared': len(shared),
        },
        'params': params,
        'trace_walkers': [int(w) for w in w_idx],
        'trace_x': [int(v) for v in s_idx],
        'trace': trace,
        'runmean_x': [int(v) for v in rm_idx],
        'runmean': runmean,
        'acf_lags': [int(v) for v in acf_lags],
        'acf': acf,
        'dens': dens,
        'acceptance': r4(acc),
        'walker_means': walker_means,
        'gyms': gym_block(flat, names, str(d['height_form']),
                          int(d['n_quad']), pymc_gym),
        # The three eliminated explanations for the sigma_gym gap, so the page
        # can quote them rather than assert them. Absent until
        # scripts/check_sigma_gym.py has been run, and the section renders
        # without the callout in that case.
        'sigma_gym_check': sigma_gym_check(),
    }

    OUT.write_text(json.dumps(payload, separators=(',', ':')))
    r = payload['run']
    print(f"acceptance {r['acc_mean']:.3f}  tau max {r['tau_max']:.0f} "
          f"({r['tau_max_param']})  ESS floor {r['ess_min']:,.0f}")
    print(f"converged by emcee's 50-tau rule: {r['converged']} "
          f"(wants {r['steps_wanted']:,} steps, has {r['steps_total']:,})")
    if r['worst_gap_sd'] is not None:
        print(f"largest gap vs {args.pymc} across {r['n_shared']} shared "
              f"parameters: {r['worst_gap_sd']:.3f} posterior sd")
    print(f'wrote {OUT}  {OUT.stat().st_size / 1e6:.1f} MB')


if __name__ == '__main__':
    main()
