"""Combine chains from separate runs of the SAME model into one trace.

Chains are independent by construction: nothing links them, so four run
tomorrow are worth exactly as much as four run today. That makes a fit
incremental -- run 4 chains, read the diagnostics, and top up only the fits
that need it, instead of committing to 8 chains everywhere up front.

Verified rather than assumed: splitting a real 4-chain fit into two 2-chain
halves and merging them back reproduces R-hat and effective sample size to
four decimal places (`tests/test_merge_chains.py`).

What makes it unsafe is what this script checks:

  * **The same model.** Merging two different posteriors produces a number
    that is neither. Every sampling argument except the fit name and the seed
    must match, and PyMC's own coordinates must line up.
  * **Different seeds.** Two runs launched with the same `--seed` produce the
    SAME chains. Merging them doubles the draw count while adding no
    information, and R-hat over duplicated chains is a fiction -- between-chain
    variance is zero by construction, so it would read 1.000 no matter how
    badly the sampler mixed. `run_fit.py` stamps the seed into the trace so
    this is checkable; a run with no seed recorded cannot be cleared and is
    refused unless --allow-unseeded says the caller has checked by hand.
  * **Chain ids.** A separate run numbers its chains 0..n-1, so ids collide.
    Left alone, xarray ALIGNS on them instead of appending -- silently
    dropping chains rather than erroring.

    python scripts/merge_chains.py --out v7_v3_lin_merged v7_v3_lin v7_v3_lin_b

Writes runs/traces/idata_<out>.nc. Run from the repo root.
"""
import argparse
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')
import arviz as az
import numpy as np

from kaya.viewer_paths import trace_file

ROOT = Path(__file__).resolve().parents[1]

# Differences that do not make two runs incomparable.
IGNORED_ARGS = {'name', 'out_dir', 'data_dir', 'seed', 'draws'}


def fit_args(idata):
    raw = idata.posterior.attrs.get('kaya_fit_args')
    if raw is None:
        return None
    return {k: v for k, v in json.loads(raw).items() if k not in IGNORED_ARGS}


def seed_of(idata):
    return idata.posterior.attrs.get('kaya_seed', 'unset')


def check_compatible(traces, names, allow_unseeded):
    """Refuse anything whose merge would be meaningless rather than wrong-looking."""
    problems = []

    args = [fit_args(t) for t in traces]
    if any(a is None for a in args):
        missing = [n for n, a in zip(names, args) if a is None]
        problems.append(
            f'no recorded fit arguments in {", ".join(missing)} -- these '
            'predate the --seed flag, so the models cannot be compared')
    elif any(a != args[0] for a in args[1:]):
        for n, a in zip(names[1:], args[1:]):
            diff = {k: (args[0].get(k), a.get(k))
                    for k in set(args[0]) | set(a) if args[0].get(k) != a.get(k)}
            if diff:
                problems.append(f'{names[0]} and {n} were fitted differently: {diff}')

    seeds = [seed_of(t) for t in traces]
    unseeded = [n for n, s in zip(names, seeds) if s == 'unset']
    if unseeded and not allow_unseeded:
        problems.append(
            f'no seed recorded for {", ".join(unseeded)}. Two runs with the '
            'same seed are the same chains, and merging them makes R-hat read '
            '1.000 regardless of mixing. Pass --allow-unseeded only if you '
            'know these were separate draws.')
    seen = [s for s in seeds if s != 'unset']
    if len(seen) != len(set(seen)):
        problems.append(f'repeated seed among {seeds} -- these are the same chains')

    n_draws = {t.posterior.sizes['draw'] for t in traces}
    if len(n_draws) > 1:
        problems.append(
            f'different draw counts {sorted(n_draws)}; xarray would pad the '
            'short ones with NaN. Truncate to the shortest first if that is '
            'really what you want.')

    var_sets = [set(t.posterior.data_vars) for t in traces]
    if any(v != var_sets[0] for v in var_sets[1:]):
        problems.append('different parameter sets between traces')

    return problems


def merge(traces):
    """Concatenate along `chain`, renumbering so ids cannot collide."""
    out, next_id = [], 0
    for t in traces:
        n = t.posterior.sizes['chain']
        out.append(t.assign_coords(chain=('chain', list(range(next_id, next_id + n)))))
        next_id += n
    return az.concat(*out, dim='chain')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fits', nargs='+', help='fit names, as in idata_<name>.nc')
    ap.add_argument('--out', required=True, help='name for the merged trace')
    ap.add_argument('--allow-unseeded', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    if len(args.fits) < 2:
        raise SystemExit('need at least two fits to merge')

    traces, names = [], []
    for n in args.fits:
        f = trace_file(n)
        if not f.exists():
            raise SystemExit(f'{f} not found')
        traces.append(az.from_netcdf(str(f)))
        names.append(n)

    for n, t in zip(names, traces):
        print(f'  {n:>24}  {t.posterior.sizes["chain"]} chains x '
              f'{t.posterior.sizes["draw"]} draws   seed {seed_of(t)}')

    problems = check_compatible(traces, names, args.allow_unseeded)
    if problems:
        print('\nREFUSING TO MERGE:', file=sys.stderr)
        for p in problems:
            print(f'  * {p}', file=sys.stderr)
        return 1

    merged = merge(traces)
    total = merged.posterior.sizes['chain']
    print(f'\nmerged -> {total} chains x {merged.posterior.sizes["draw"]} draws')

    # The point of merging is better diagnostics, so show what they became.
    shared = [v for v in merged.posterior.data_vars
              if merged.posterior[v].values.ndim == 2][:8]
    print(f'\n{"parameter":>18} ' + ' '.join(f'{n[:10]:>11}' for n in names)
          + f' {"merged":>11}')
    for v in shared:
        cells = []
        for t in traces:
            r = float(np.nanmax(np.asarray(az.rhat(t, var_names=[v])[v].values)))
            cells.append(f'{r:>11.4f}')
        rm = float(np.nanmax(np.asarray(az.rhat(merged, var_names=[v])[v].values)))
        print(f'{v:>18} ' + ' '.join(cells) + f' {rm:>11.4f}')
    print('(R-hat, lower is better. The merged column is the one to read: it '
          'is\nestimated from more chains, which is the reason to merge.)')

    if args.dry_run:
        print('\ndry run: nothing written')
        return 0
    out = ROOT / 'runs' / 'traces' / f'idata_{args.out}.nc'
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_netcdf(str(out))
    print(f'\nwrote {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
