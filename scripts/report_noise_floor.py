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


def _usable(r):
    """Did this fit converge? Prefer the run's own verdict, fall back to gates."""
    conv = r.get('convergence')
    if isinstance(conv, dict) and 'usable' in conv:
        return bool(conv['usable'])
    rhat, ess = r.get('max_rhat'), r.get('min_ess')
    return (rhat is not None and rhat <= 1.01
            and ess is not None and ess >= 400)


def _why(r):
    conv = r.get('convergence') or {}
    reasons = conv.get('reasons')
    if reasons:
        return reasons[0]
    return f'R-hat {r.get("max_rhat")}, ESS {r.get("min_ess")}'


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

    # Every marginalized fit, rerun arms included. The glob has to be loose:
    # the reruns are named v10_zero_marg_r2, and a pattern anchored on
    # `_marg.json` silently dropped exactly the fits that were run to fix the
    # broken ones.
    scored, excluded = {}, []
    for p in sorted(RESULTS.glob('result_v10_*marg*.json')):
        r = json.loads(p.read_text())
        if r['name'] == TWIN or not r.get('loo'):
            continue
        form = r['args']['height_form']

        # A fit that did not converge is EXCLUDED, not annotated. It used to be
        # listed with a [NOT CONVERGED] tag and still counted in the spread --
        # which is how v10_zero_marg's -55,363 (chain 3 frozen, ESS 7, p_loo
        # 19,228) would have turned a 3 elpd question into a 19,845 elpd
        # "verdict: readable". An elpd computed on a dead chain is not a worse
        # measurement, it is not a measurement.
        if not _usable(r):
            excluded.append((r['name'], form, r['loo']['elpd_loo'], _why(r)))
            continue

        # Two clean fits of one form: keep the better-mixed one and say so.
        prev = scored.get(form)
        if prev is None or (r.get('min_ess') or 0) > (prev[2] or 0):
            scored[form] = (r['name'], r['loo']['elpd_loo'], r.get('min_ess'))

    if excluded:
        print('Excluded — did not converge, so their elpd means nothing\n')
        for name, form, elpd, why in excluded:
            print(f'  {name:22s} {form:20s} {elpd:+12.1f}   {why}')
        print()

    fits = [(n, e, form, ess) for form, (n, e, ess) in scored.items()]
    if len(fits) < 2:
        print(f'{len(fits)} form(s) scored so far — nothing to rank yet')
        return 0

    fits.sort(key=lambda x: -x[1])
    print(f'The sweep, best first ({len(fits)} of 7 forms scored)\n')
    w = max(len(f[2]) for f in fits)
    for name, elpd, form, ess in fits:
        d = elpd - fits[0][1]
        print(f'  {form:{w}s} {elpd:+12.1f}  '
              f'{"best" if d == 0 else f"{d:+.1f}":>8s}   ({name})')

    # TWO QUESTIONS, NOT ONE. `zero` is not a height form competing with the
    # others -- it is the null. Leaving it in the spread answers "does height
    # do anything" and then presents the answer as if it were "which form is
    # best", which are different questions with different stakes. The page's
    # primary claim depends on the second; the first decides whether there is
    # a height section at all.
    zero = next((f for f in fits if f[2] == 'zero'), None)
    forms = [f for f in fits if f[2] != 'zero']

    if zero is not None and forms:
        gain = forms[0][1] - zero[1]
        _verdict('DOES HEIGHT DO ANYTHING?',
                 f'best form ({forms[0][2]}) beats no-height by {gain:.1f} elpd',
                 gain, floor,
                 yes='height is doing measurable work; the section is warranted',
                 marginal=('height barely clears the seed-to-seed noise — say '
                           'so plainly rather than reporting an effect'),
                 no=('height is INDISTINGUISHABLE from no height on this data. '
                     'That is the\n           finding. Do not rank the forms '
                     'below it; there is nothing to rank.'))
    elif zero is None:
        print('\n  no clean `zero` fit yet — "does height matter at all" is '
              'unanswered,\n  and it is the question the height section rests '
              'on.')

    if len(forms) >= 2:
        fspread = forms[0][1] - forms[-1][1]
        _verdict('WHICH FORM?',
                 f'spread across {len(forms)} height forms is {fspread:.1f} elpd',
                 fspread, floor,
                 yes='the ordering carries information',
                 marginal=('the ordering is not distinguishable from which '
                           'seeds happened to be\n           drawn. Do not '
                           'publish a winner.'),
                 no=('these forms are indistinguishable. Report THAT, not an '
                     'order.'))
    return 0


def _verdict(question, measured, size, floor, *, yes, marginal, no):
    ratio = size / floor if floor else float('inf')
    print(f'\n{question}')
    print(f'  {measured}, against a floor of {floor:.1f} = {ratio:.2f}x')
    if ratio >= SEPARATION:
        print(f'  VERDICT: readable — {yes}')
    elif ratio >= 1.0:
        print(f'  VERDICT: MARGINAL — {marginal}')
    else:
        print(f'  VERDICT: NOT READABLE — {no}')


if __name__ == '__main__':
    sys.exit(main())
