"""The lab notebook: what is running, what is next, and everything already run.

Three layers, deliberately separated because they have different half-lives and
different authorities:

1. **Now** -- read from the scheduler's own STATUS.md and from the process
   table at build time. This is a SNAPSHOT. The viewer serves precomputed
   payloads and never computes on request, so the page carries its build time
   and says plainly that it is as-of. Re-run this script to refresh it.

2. **Next** -- the ordered plan, hand-maintained in NEXT below. Kept in source
   rather than parsed out of docs/run-plan.md because a plan that a script
   guesses at is a plan nobody trusts. Edit it here when priorities change.

3. **The log** -- every result file in runs/results, grouped into campaigns.
   The numbers are read from disk; the QUESTION each campaign asked and the
   VERDICT it reached are hand-written in CAMPAIGNS, because "what did we learn"
   is not recoverable from an R-hat.

The point of the third layer is the failures. A run that did not work is only
worth its compute if the reason it did not work is written down next to it --
otherwise the next campaign rediscovers it. Every campaign below records what
it ruled out, not just what it found.

Writes src/kaya/viewer_static/v2_runlog.json.

Run from the repo root -- running from src/kaya breaks numpy, because
src/kaya/secrets.py shadows the stdlib module its bit generator imports.
"""
import json
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
RESULTS = RUNS / 'results'
TRACES = RUNS / 'traces'
STATUS = RUNS / 'logs' / 'overnight' / 'STATUS.md'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_runlog.json'

RHAT_BAR = 1.01      # lower is better
ESS_FLOOR = 400      # higher is better


# ---------------------------------------------------------------- layer 2

NEXT = [
    {'title': 'Read the seven-form sweep against a re-measured noise floor',
     'state': 'blocked',
     'why': 'Seven scores sort into a leaderboard whether or not the ordering '
            'means anything. The v7 sweep spread 32.7 elpd against 31.1 elpd '
            'of pure seed-to-seed noise — no ranking at all. That floor does '
            'not transfer here: it was per observation on chains that never '
            'mixed, and these are per climber on chains that did.',
     'blocker': 'the four remaining forms, then v10_lin_marg_s2',
     'how': 'scripts/recover_marg_loo.py, then scripts/report_noise_floor.py'},
    {'title': 'Build stage 1 — gym corrections only, no height terms',
     'state': 'next',
     'why': 'Splits the one hard fit into two easy ones. A climber at two gyms '
            'cancels themselves out of the difference, so gym corrections are '
            'identified without any height model at all.',
     'gate': 'HARD PASS/FAIL — its corrections must reproduce v10\'s within '
             '0.019 grades. If they do not, stop: the whole two-stage plan '
             'rests on that agreement.',
     'how': 'docs/two-stage-and-grade-compression.md, Part 1'},
    {'title': 'Add grade compression to stage 1 — linear, then quadratic',
     'state': 'queued',
     'why': 'One correction per gym assumes a gym grades the same for a V2 and '
            'a V9 climber. Measured false: τ = 0.074 at 1.9× the detection '
            'floor, and the curvature is real too at τ = 0.023.',
     'how': 'per-gym slope in LATENT ability, never the observed grade — '
            'zero-sum, σ_b ~ HalfNormal(0.05)'},
    {'title': 'Build stage 2 with a cut posterior',
     'state': 'queued',
     'why': 'Stage 1\'s corrections are estimates, not constants. Treating '
            'them as known makes every downstream error bar too small.',
     'how': 'draw (a_g, b_g) JOINTLY per replicate — they are correlated'},
    {'title': 'Build the time-resolved dataset',
     'state': 'parallel',
     'why': 'Gym drift is the largest effect measured — 0.62 grades of '
            'accumulated spread over 5.9 years against 1.29 for the entire '
            'correction range — and it cannot be fitted at all right now.',
     'blocker': 'base_bouldering.pkl is one row per (climber, gym) and carries '
                'no dates; dates live only in the raw sends table',
     'how': 'depends on no fit, so it can proceed alongside stage 1'},
    {'title': 'Evidence and Bayes factors',
     'state': 'deferred',
     'why': 'Computes the marginal likelihood for a model form that is about '
            'to gain 29–58 parameters and split in two. Deferred until the '
            'form settles; bridge sampling resumes first, since it runs on '
            'draws that already exist.'},
]


# ---------------------------------------------------------------- layer 3

CAMPAIGNS = [
    {'prefix': 'v3', 'title': 'First broad sweep',
     'asked': 'Which height forms are even worth carrying, and does '
              'marginalizing the single-observation climbers help?',
     'found': 'Established the baseline every later campaign is measured '
              'against, and showed the closed-form marginalization of '
              'single-observation climbers is exact — verified to 1e-8.',
     'verdict': 'worked'},
    {'prefix': 'v4', 'title': 'Ape index and covariate variants',
     'asked': 'Does ape index (arm span minus height) earn its parameters, '
              'and does it interact with gender?',
     'found': 'Kept as a linear term. The quadratic did not pay for itself.',
     'verdict': 'partial'},
    {'prefix': 'v5', 'title': 'Long warm-up probe',
     'asked': 'Does a much longer warm-up fix the convergence problem on its '
              'own, without changing the model?',
     'found': 'No. More tuning did not rescue the geometry — the sampler was '
              'not under-tuned, it was in the wrong coordinates.',
     'verdict': 'ruled out'},
    {'prefix': 'v6', 'title': 'Orthogonal design basis',
     'asked': 'Does sampling the covariate block on a Gram-Schmidt '
              'orthogonalised basis fix R-hat?',
     'found': 'No. The global correlation matrix had a condition number of 6 '
              '— the covariates were never the problem, so rotating them '
              'could not be the fix.',
     'verdict': 'ruled out'},
    {'prefix': 'v7', 'title': 'Seven-form sweep, raw basis',
     'asked': 'Which shape for height predicts best?',
     'found': 'Unanswerable as run. Every fit saturated the tree-depth limit '
              'on 100% of iterations with ZERO divergences — the diagnostic '
              'signature of truncation, which R-hat and ESS cannot see. The '
              'elpd spread was 32.7 against a 31.1 noise floor, so the '
              'leaderboard was measuring random seeds.',
     'verdict': 'failed'},
    {'prefix': 'v8', 'title': 'Centered parameterisation probe',
     'asked': 'The data dominates the prior 21–64× per climber, which is the '
              'regime where centering samples better. Does it?',
     'found': 'Barely. R-hat 1.040 → 1.030, step size unchanged at 0.003, '
              'still 100% at the tree-depth limit. A real but useless '
              'improvement.',
     'verdict': 'ruled out'},
    {'prefix': 'v9', 'title': 'Quadrature probe',
     'asked': 'Integrating out every climber offset is 28× slower per '
              'gradient. Does the better geometry pay for that?',
     'found': 'Yes, decisively. Trajectories started finishing instead of '
              'being truncated — 42% at the depth limit instead of 100%, and '
              'the step size went from 0.003 to 0.141.',
     'verdict': 'worked'},
    {'prefix': 'v10', 'title': 'Quadrature sweep',
     'asked': 'Does the marginalized model converge across all seven height '
              'forms, and can they finally be compared?',
     'found': 'The first fit converged outright: R-hat 1.0000, effective '
              'sample size 762, zero divergences, 0% at the tree-depth limit '
              'against v7\'s 100%. 4,241 sampled parameters became 40.',
     'verdict': 'in progress'},
]

# Measurements that are not fits. They belong in the log because they cost real
# work and changed the plan, and because a log of only the things that needed a
# sampler would misrepresent where the answers actually came from.
MEASUREMENTS = [
    {'title': 'Grade compression is real', 'when': '2026-08-07',
     'how': 'Model-free. For a climber at two gyms the difference of their '
            'grades cancels the climber, so it can be regressed on ability '
            'directly. Cochran\'s Q separates real spread from noise.',
     'found': 'τ = 0.074 grades per grade (I² = 60%), at 1.9× the detection '
              'floor. Replicated at τ = 0.0753 on an artifact-free ability '
              'proxy. Curvature is real too: τ = 0.023 at 98% power.',
     'verdict': 'worked', 'where': 'scripts/build_v2_structure.py'},
    {'title': 'Gym drift is the largest effect found', 'when': '2026-08-07',
     'how': 'Same identity on the calendar axis — one climber, one 90-day '
            'window, two gyms, so ability, improvement and selection all '
            'cancel at once.',
     'found': 'τ = 0.163 grades/year (I² = 68%) at 2.4× the detection floor. '
              'Survives controlling the within-window date gap and halving the '
              'window. Two independent routes to the per-gym spread agree '
              '(0.105 vs 0.115).',
     'verdict': 'worked', 'where': 'scripts/build_v2_structure.py'},
    {'title': 'Climber advancement must stay fixed, not fitted',
     'when': '2026-08-07',
     'how': 'Compared the correction-vs-time slope in the data against the '
            'externally measured improvement rate.',
     'found': 'The data shows 0.827 grades/year; climbers really improve at '
              '~0.24. A free advancement parameter would absorb 3.4× its true '
              'value, silently eating gym-level drift.',
     'verdict': 'ruled out', 'where': 'docs/two-stage-and-grade-compression.md §6'},
    {'title': 'The timing confound is still unexplained', 'when': '2026-08-07',
     'how': 'Subtracted each gym\'s accumulated drift from its correction and '
            're-measured the relationship.',
     'found': 'Removing drift makes the confound slightly WORSE (slope +0.809 '
              '→ +0.955). Advancement explains ~8%, relative drift ~0%. Two '
              'candidates measured and eliminated; selection is what is left.',
     'verdict': 'open', 'where': 'docs/two-stage-and-grade-compression.md §7.9'},
    {'title': 'Chains and draws do not do the same thing', 'when': '2026-08-06',
     'how': 'Subset a finished trace by chain and by draw and re-score it — no '
            'new compute at all.',
     'found': 'Both scale effective sample size roughly linearly, but only '
              'DRAWS improve R-hat. PyMC cannot resume a fit, so draw count is '
              'a one-shot decision at launch.',
     'verdict': 'worked', 'where': 'scripts/build_v2_sweep.py'},
]


# Jobs the running scheduler still lists as pending but which will be skipped.
# Its plan was built at launch and cannot be edited in place; the gate that
# admits these reads its evidence from disk, and that evidence was retired. See
# runs/results/RETIRED.md. Recorded here so the page does not promise work that
# is never going to run.
RETIRED_JOBS = {
    'v10_lin_cen': 'superseded — centering moved R-hat 1.040→1.030 and left '
                   '100% of iterations at the tree-depth limit, while the '
                   'quadrature reached 1.0000 and 0%',
    'v10_conf_cen': 'superseded — same measurement',
}


def campaign_of(name):
    m = re.match(r'(v\d+)_', name)
    return m.group(1) if m else 'other'


def collect_runs():
    runs = []
    for p in sorted(RESULTS.glob('result_*.json')):
        try:
            r = json.loads(p.read_text())
        except Exception:
            continue
        name = r.get('name')
        if not name:
            continue
        a = r.get('args', {}) or {}
        td = r.get('tree_depth') or {}
        loo = r.get('loo') or {}
        conv = r.get('convergence') or {}
        rhat, ess = r.get('max_rhat'), r.get('min_ess')
        trace = TRACES / f'idata_{name}.nc'
        runs.append({
            'name': name,
            'campaign': campaign_of(name),
            'form': a.get('height_form'),
            'network': a.get('network'),
            'chains': a.get('chains'), 'draws': a.get('draws'),
            'tune': a.get('tune'), 'seed': a.get('seed'),
            'marginalize_all': bool(a.get('marginalize_all')),
            'centered': bool(a.get('center_user_offsets')),
            'orthogonal': bool(a.get('orthogonal_design')),
            'n_quad': a.get('n_quad') if a.get('marginalize_all') else None,
            'minutes': r.get('elapsed_min'),
            'rhat': rhat, 'ess': ess,
            'rhat_ok': (rhat is not None and rhat <= RHAT_BAR),
            'ess_ok': (ess is not None and ess >= ESS_FLOOR),
            'divergences': r.get('divergences'),
            'depth_frac': td.get('frac_at_limit'),
            'depth_mean': td.get('mean'),
            'step_size': td.get('step_size'),
            'elpd': loo.get('elpd_loo'), 'elpd_se': loo.get('se'),
            'loo_unit': r.get('loo_unit') or ('observation' if loo else None),
            'pareto_k_max': loo.get('pareto_k_max'),
            'converged': conv.get('converged'),
            'result_path': str(p.relative_to(ROOT)),
            'trace_path': str(trace.relative_to(ROOT)) if trace.exists() else None,
            'trace_mb': round(trace.stat().st_size / 1e6, 1) if trace.exists() else None,
        })
    return runs


def read_status():
    """The scheduler's own view, plus what is actually holding a core now."""
    rows, updated = [], None
    if STATUS.exists():
        txt = STATUS.read_text()
        m = re.search(r'_updated ([^_]+)_', txt)
        if m:
            updated = m.group(1).strip()
        for line in txt.splitlines():
            cells = [c.strip() for c in line.split('|')[1:-1]]
            if len(cells) == 4 and cells[0].startswith('`'):
                rows.append({'job': cells[0].strip('`'), 'state': cells[1],
                             'minutes': cells[2] or None, 'note': cells[3]})
    # Live process table beats a file the scheduler wrote minutes ago.
    live = {}
    try:
        out = subprocess.run(['ps', '-eo', 'etime,command'],
                             capture_output=True, text=True, timeout=10).stdout
        for line in out.splitlines():
            m = re.search(r'run_fit\.py --name (\S+)', line)
            if m:
                live[m.group(1)] = line.strip().split()[0]
    except Exception:
        pass
    for r in rows:
        if r['job'] in live:
            r['state'], r['elapsed'] = 'running', live[r['job']]
        elif r['state'] == 'running':
            # STATUS.md says running but no process holds it: it finished or
            # died since the scheduler last wrote. Say so rather than lie.
            r['state'] = 'running?'
    for r in rows:
        if r['job'] in RETIRED_JOBS and r['state'] == 'pending':
            r['state'] = 'retired'
            r['note'] = RETIRED_JOBS[r['job']]
    for name, el in live.items():
        if not any(r['job'] == name for r in rows):
            rows.append({'job': name, 'state': 'running', 'elapsed': el,
                         'minutes': None, 'note': 'not in the scheduler plan'})
    if not any(r['job'] == 'v10_lin_marg_s2' for r in rows):
        done = (RESULTS / 'result_v10_lin_marg_s2.json').exists()
        rows.append({'job': 'v10_lin_marg_s2',
                     'state': 'done' if done else 'waiting',
                     'minutes': None,
                     'note': 'noise-floor replicate — identical to '
                             'v10_lin_marg but for the seed; launched by '
                             'scripts/run_noise_floor.sh when the cores free'})
    return {'updated': updated, 'jobs': rows,
            'n_live': len(live),
            'scheduler_up': bool(subprocess.run(
                ['pgrep', '-f', 'run_overnight.py'],
                capture_output=True).stdout.strip())}


def main():
    runs = collect_runs()
    status = read_status()

    by_campaign = {}
    for r in runs:
        by_campaign.setdefault(r['campaign'], []).append(r)

    campaigns = []
    for c in CAMPAIGNS:
        rs = by_campaign.get(c['prefix'], [])
        conv = [r for r in rs if r['rhat_ok'] and r['ess_ok']]
        campaigns.append({**c, 'n_runs': len(rs), 'n_converged': len(conv),
                          'minutes': round(sum(r['minutes'] or 0 for r in rs))})

    known = {c['prefix'] for c in CAMPAIGNS}
    other = [r for r in runs if r['campaign'] not in known]

    payload = {
        'built_at': time.strftime('%Y-%m-%d %H:%M'),
        'status': status,
        'next': NEXT,
        'campaigns': campaigns,
        'measurements': MEASUREMENTS,
        'runs': runs,
        'n_other': len(other),
        'totals': {
            'runs': len(runs),
            'hours': round(sum(r['minutes'] or 0 for r in runs) / 60, 1),
            'traces': sum(1 for r in runs if r['trace_path']),
            'trace_gb': round(sum(r['trace_mb'] or 0 for r in runs) / 1000, 2),
            'converged': sum(1 for r in runs if r['rhat_ok'] and r['ess_ok']),
        },
        'bars': {'rhat': RHAT_BAR, 'ess': ESS_FLOOR},
    }
    OUT.write_text(json.dumps(payload, indent=1, default=float))
    t = payload['totals']
    print(f"{t['runs']} runs, {t['converged']} converged, {t['hours']} h of "
          f"compute, {t['traces']} traces ({t['trace_gb']} GB)")
    print(f"live: {status['n_live']} fit(s) running, "
          f"scheduler {'up' if status['scheduler_up'] else 'down'}")
    print(f'wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KB)')


if __name__ == '__main__':
    main()
