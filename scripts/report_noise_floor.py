"""Is the height-form sweep a ranking, or a rearrangement?

Seven models each with a score invites exactly one behaviour: sort the column
and announce a winner. Whether that is allowed depends on a number that has
nothing to do with any of the seven -- how far the score moves when NOTHING
changes but the random seed.

The v7 sweep is the cautionary case. Its spread was 32.7 elpd against a
measured 31.1 elpd of seed-to-seed noise: a ranking 1.05x its own error bar,
which is to say not a ranking. That floor does not transfer to the marginalized
fits. It was measured per OBSERVATION on chains that never mixed; these are per
CLIMBER (leave-one-climber-out, because integrating a climber's offset out makes
their rows conditionally dependent) on chains that did. Neither the unit nor the
geometry carries over, so it is measured again from v10_lin_marg and its twin.

elpd = expected log pointwise predictive density, the PSIS-LOO score.
**Higher is better.** PSIS-LOO = Pareto-smoothed importance sampling
leave-one-out cross-validation.

    python scripts/report_noise_floor.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'runs' / 'results'

BASE, TWIN = 'v10_lin_marg', 'v10_lin_marg_s2'
# A spread this many times the floor before the ordering is worth reading. Two
# is not a convention, it is the smallest multiple at which the gap between
# adjacent entries survives the floor being under-measured by half.
SEPARATION = 2.0


def load(name):
    p = RESULTS / f'result_{name}.json'
    return json.loads(p.read_text()) if p.exists() else None


def main():
    base, twin = load(BASE), load(TWIN)
    if base is None or twin is None:
        print(f'need both {BASE} and {TWIN}; '
              f'{"base" if base is None else "twin"} is missing')
        return 1
    if not (base.get('loo') and twin.get('loo')):
        print('one of the pair has no LOO — run scripts/recover_marg_loo.py first')
        return 1
    if base['args']['seed'] == twin['args']['seed']:
        print('the pair shares a seed — that measures nothing'); return 1

    b, t = base['loo']['elpd_loo'], twin['loo']['elpd_loo']
    floor = abs(b - t)
    print('Noise floor, leave-one-climber-out elpd (higher is better)\n')
    print(f'  {BASE:18s} {b:+12.1f}  (seed {base["args"]["seed"]})')
    print(f'  {TWIN:18s} {t:+12.1f}  (seed {twin["args"]["seed"]})')
    print('  identical configuration otherwise\n')
    print(f'  NOISE FLOOR = {floor:.1f} elpd\n')

    fits = []
    for p in sorted(RESULTS.glob('result_v10_*_marg.json')):
        r = json.loads(p.read_text())
        if r['name'] == TWIN or not r.get('loo'):
            continue
        fits.append((r['name'], r['loo']['elpd_loo'],
                     r['args']['height_form'], r.get('max_rhat'),
                     r.get('min_ess')))
    if len(fits) < 2:
        print(f'{len(fits)} form(s) scored so far — nothing to rank yet')
        return 0

    fits.sort(key=lambda x: -x[1])
    spread = fits[0][1] - fits[-1][1]
    print(f'The sweep, best first ({len(fits)} of 7 forms scored)\n')
    w = max(len(f[2]) for f in fits)
    for name, elpd, form, rhat, ess in fits:
        d = elpd - fits[0][1]
        conv = '' if (rhat is not None and rhat <= 1.01
                      and ess is not None and ess >= 400) else '  [NOT CONVERGED]'
        print(f'  {form:{w}s} {elpd:+12.1f}  '
              f'{"best" if d == 0 else f"{d:+.1f}":>8s}{conv}')

    ratio = spread / floor if floor else float('inf')
    print(f'\n  spread {spread:.1f} elpd against a floor of {floor:.1f} '
          f'= {ratio:.2f}x')
    if ratio >= SEPARATION:
        print(f'  VERDICT: readable. The spread clears the floor by '
              f'{ratio:.1f}x, so the ordering carries information.')
    elif ratio >= 1.0:
        print('  VERDICT: MARGINAL. The spread barely clears the floor — the '
              'ordering is not\n           distinguishable from which seeds '
              'happened to be drawn. Do not\n           publish a winner.')
    else:
        print('  VERDICT: NOT A RANKING. The spread is smaller than the noise '
              'floor. These\n           forms are indistinguishable on this '
              'data; report that, not an order.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
