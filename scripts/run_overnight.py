"""Unattended overnight chain: Q2, Q3, and whatever Q3's answer makes worth running.

Why this exists rather than a shell script of `&&`s
---------------------------------------------------
The three questions in docs/run-plan.md do not form a straight line. Q2 (does
a long warm-up fix R-hat?) and Q3 (does an orthogonal design fix it?) are the
two levers of one 2x2 and are independent, so they should run side by side.
Q4 (the seven-form sweep) is downstream of Q3 only -- and Q3 finishes in ~85
minutes while Q2 runs for ~5.5 hours. A serial script would leave half the
machine idle for four hours; a plain `--batch` would not know that Q4's jobs
become runnable the moment Q3 lands rather than when the whole batch does.

So: a dependency-aware scheduler over a fixed core budget, which fills a slot
the instant one frees and something is eligible for it.

The core budget is the hard constraint. PyMC runs one chain per process,
single-threaded, so a 4-chain fit occupies 4 cores for its whole life. Ten
cores means two fits. Three fits was measured on this machine and it is not a
mild penalty: v4_lin_b_marg, v4_lin_c_marg and v4_linxg_marg took 370 minutes
each against ~85 for the same model run two at a time.

The gate
--------
Q4 is seven fits and most of a night, and it only makes sense on a
parameterisation that has been shown to help. Q3's result decides:

  * **pass** -> the orthogonal seven-form sweep. Every form gets re-fitted on
    the same basis, which is what makes their elpds comparable to each other.
  * **fail** -> the noise floor instead: three refits of the current primary,
    raw basis, differing only in seed. Non-optional work that is worth a night
    on its own, and it has to be measured in whichever parameterisation the
    fits it calibrates were run in -- which is exactly what Q3 decides. That
    is why it cannot simply be run alongside.

Either branch fills the night, so the machine is never left idle on a verdict.

Q4 runs at the BASELINE settings (tune 600 / draws 500), not Q2's long ones,
even though Q2 may be about to say the long ones are better. Three reasons:
every fit on disk is at baseline so the comparison stays like-for-like, seven
long fits is ~35 hours rather than a night, and if Q2 does come back saying
warm-up matters then this sweep tells us which forms are worth paying that
for. It is a screen, not the final measurement.

    python scripts/run_overnight.py --dry-run     # print the plan, run nothing
    python scripts/run_overnight.py               # wait for emcee, then go
    python scripts/run_overnight.py --wait-pid 0  # start immediately

Progress goes to STATUS.md in the log directory, rewritten after every state
change, so the morning check is one `cat` rather than a hunt through logs.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

from run_batch import CORES_PER_FIT, SINGLE_THREAD_BLAS  # noqa: E402

RUNS = ROOT / 'runs'
# Module level so the scheduler can be exercised against a stub. The thing
# worth testing here is the dependency and gate logic, and it must not cost
# eleven real fits to test it.
RUN_FIT = ROOT / 'scripts' / 'run_fit.py'

# Base seed for the sweep; each form gets SWEEP_SEED + i. A top-up run must use
# a DIFFERENT base -- see build_jobs and scripts/merge_chains.py. Recorded here
# rather than passed in so a re-run of the same plan is reproducible.
SWEEP_SEED = 20260806
# A DIFFERENT base for the 2026-08-06 night, so none of its fits can collide
# with a v7 seed -- a repeated seed is the same chains twice.
NIGHT_SEED = 20260807

# Baseline to beat, from v3_conf_marg: quadratic_x_gender, marginalized, raw
# basis, tune 600 / draws 500. Both numbers are on the raw-basis coefficient
# names, which is why run_fit.py reports the orthogonal fit under those names
# too -- the comparison would be meaningless otherwise.
BASELINE = {'fit': 'v3_conf_marg', 'max_rhat': 1.069, 'min_ess': 48}

# Chain count is per-job now, not shared: 8-chain fits need the whole machine
# and 4-chain fits pair up, so it has to travel with the job's core budget.
COMMON = ['--network', 'net50', '--name-filter', 'confident',
          '--fixed-sigma-link', '--marginalize-singles']


@dataclass
class Job:
    name: str
    args: list
    after: str = ''          # must finish before this one is eligible
    gate: str = ''           # name of a gate function in GATES
    note: str = ''
    # One core per chain. Admission is by core budget rather than a fixed slot
    # count, so an 8-chain fit takes the machine alone while 4-chain fits still
    # pair up -- measured at ~85 min each for two, versus 370 for three.
    cores: int = CORES_PER_FIT
    state: str = 'pending'   # pending | running | done | failed | skipped
    rc: int | None = None
    minutes: float = 0.0
    started: float = 0.0
    proc: subprocess.Popen | None = field(default=None, repr=False)

    @property
    def cmd(self):
        return [sys.executable, str(RUN_FIT), '--name', self.name] + self.args


def fit(name, height_form, *, extra=(), tune=600, draws=500,
        chains=CORES_PER_FIT, seed=None, **kw):
    args = COMMON + ['--height-form', height_form, '--tune', str(tune),
                     '--draws', str(draws), '--chains', str(chains)]
    if seed is not None:
        args += ['--seed', str(seed)]
    return Job(name=name, args=args + list(extra), cores=chains, **kw)


# ---- the gate ------------------------------------------------------------

def orthogonal_helped(_):
    """Did v6_conf_orth do at least as well as the raw-basis baseline?

    Deliberately an OR rather than an AND. R-hat and effective sample size
    are both noisy at 500 draws, and demanding an improvement in both would
    fail a real improvement about as often as it would catch a real
    regression. Requiring neither to have got worse is the weaker claim the
    evidence can actually support.
    """
    p = RUNS / 'results' / 'result_v6_conf_orth.json'
    if not p.exists():
        return False, 'no result file -- the fit did not get far enough'
    r = json.loads(p.read_text())
    # A result file left by an earlier night would gate this one, and it would
    # do so silently: the name is the same and the JSON is well formed. Check
    # it describes the fit this plan actually asked for.
    a = r.get('args', {})
    if a and not a.get('orthogonal_design'):
        return False, ('result_v6_conf_orth.json is from a RAW-basis run -- '
                       'stale file from an earlier night, refusing to gate on it')
    rhat, ess = float(r['max_rhat']), float(r['min_ess'])
    o = r.get('orth', {})
    detail = (f"raw-basis R-hat {rhat:.3f} vs baseline "
              f"{BASELINE['max_rhat']:.3f}, ESS {ess:.0f} vs "
              f"{BASELINE['min_ess']:.0f}")
    if o:
        detail += (f"; sampled basis R-hat {o['max_rhat']:.3f}, "
                   f"ESS {o['min_ess']:.0f}")
    ok = rhat <= BASELINE['max_rhat'] or ess >= BASELINE['min_ess']
    return ok, detail


def _tree_depth_from_trace(name, limit):
    """Recover the tree-depth summary from the trace when the result lacks it.

    run_fit only started recording this on 2026-08-06, and a probe launched
    minutes earlier does not have it. The trace does -- it is the same numbers
    from the same run -- so read them rather than discarding a finished fit and
    idling the machine, which is exactly what happened that night.
    """
    p = RUNS / 'traces' / f'idata_{name}.nc'
    if not p.exists():
        return None
    import arviz as az
    import numpy as np
    ss = az.from_netcdf(str(p)).sample_stats
    if 'tree_depth' not in ss:
        return None
    td = np.asarray(ss['tree_depth'].values)
    return {'mean': float(td.mean()), 'max': int(td.max()), 'limit': limit,
            'frac_at_limit': float((td >= limit).mean()),
            'step_size': float(np.asarray(ss['step_size'].values).mean())}


def _result(name):
    p = RUNS / 'results' / f'result_{name}.json'
    return json.loads(p.read_text()) if p.exists() else None


# The v7 linear fit, which is the form this is trying to rescue. Both numbers
# come from runs/results/result_v7_v3_lin.json.
LINEAR_BASELINE = {'max_rhat': 1.040, 'min_ess': 151.0}


def centering_helped(_):
    """Did centering the climber offsets improve the worst form?

    Measured before this was queued: the likelihood dominates the prior 21-64x
    for every one of the 4,201 multi-row climbers, which is the regime where
    the centered parameterisation samples better. corr(log sigma_user, spread
    of epsilon_raw) = -0.847 is what the mismatch looked like.

    An OR, not an AND, for the same reason the old orthogonal gate was: R-hat
    and ESS are both noisy at 1,000 draws, and requiring both to improve would
    reject a real improvement about as often as it caught a real regression.
    """
    r = _result('v8_lin_centered')
    if r is None:
        return False, 'no result file -- the centered probe did not finish'
    if not r.get('args', {}).get('center_user_offsets'):
        return False, ('result_v8_lin_centered.json is from a NON-centered '
                       'run -- stale file, refusing to gate on it')
    rhat, ess = float(r['max_rhat']), float(r['min_ess'])
    td = r.get('tree_depth', {})
    detail = (f"R-hat {rhat:.3f} vs {LINEAR_BASELINE['max_rhat']:.3f}, "
              f"ESS {ess:.0f} vs {LINEAR_BASELINE['min_ess']:.0f}, "
              f"{100 * td.get('frac_at_limit', 1.0):.0f}% at tree-depth limit "
              f"(was 100%)")
    ok = (rhat <= LINEAR_BASELINE['max_rhat']
          or ess >= LINEAR_BASELINE['min_ess'])
    return ok, detail


def quadrature_viable(_):
    """Is the fully-marginalized model fast enough to be worth a night?

    It is 28x slower per gradient (53 ms at 31 nodes vs 1.9 ms with sampled
    offsets), so it only wins if the better geometry lets NUTS finish
    trajectories early instead of burning its whole step budget. The probe runs
    with max_treedepth 7, so "not saturating 7" means it is U-turning inside
    127 steps -- against 1,023 for the sampled-offset model.
    """
    r = _result('v9_probe_margall')
    if r is None:
        return False, 'no result file -- the quadrature probe did not finish'
    if not r.get('args', {}).get('marginalize_all'):
        return False, ('result_v9_probe_margall.json is not from a '
                       'marginalize-all run -- stale file, refusing to gate')
    td = r.get('tree_depth') or _tree_depth_from_trace(r['name'],
                                                       r['args']['max_treedepth'])
    if not td:
        return False, 'no tree-depth record -- cannot tell if it saturated'
    frac = float(td['frac_at_limit'])
    detail = (f"{100 * frac:.0f}% of iterations at the depth-{td['limit']} "
              f"limit, mean depth {td['mean']:.1f}, "
              f"step size {td['step_size']:.4f}")
    # Under half the iterations saturating means most trajectories complete,
    # which is the whole bet. At 100% it is strictly worse than what we have.
    return frac < 0.5, detail


GATES = {'orthogonal_helped': orthogonal_helped,
         'centering_helped': centering_helped,
         'quadrature_viable': quadrature_viable}


# ---- the plan ------------------------------------------------------------

def build_jobs():
    """2026-08-07, rebuilt after the probes came back and reversed the ranking.

    Both geometry fixes were probed on the linear form, matched settings
    (4 chains x 1,000 draws, 2,000 warm-up) against the v7 baseline:

    | fit                | R-hat | ESS | min | step size | at depth limit |
    | ------------------ | ----- | --- | --- | --------- | -------------- |
    | v7 (non-centered)  | 1.040 | 151 | 199 | 0.00307   | 100%           |
    | v8 (centered)      | 1.030 | 194 | 125 | 0.00287   | 100%           |
    | v9 probe (quad)    |   --  |  -- |  49 | **0.141** | 42% (of 7)     |

    **Centering barely moved anything.** R-hat 1.040 -> 1.030 and ESS 151 ->
    194 are inside the noise on these diagnostics, the step size did not budge,
    and it still burned its whole step budget on 100% of iterations. The
    likelihood:prior ratio argument said centered should win; measured, it
    does not. Whatever is stiff here is not the sigma_user/epsilon coupling
    alone. The 37% speedup is real but it is speed, not mixing.

    **The quadrature fixed the geometry outright.** Step size 0.00307 ->
    0.141, a **46x** increase, and trajectories that finish: mean depth 6.4
    against an artificial cap of 7, versus 1,023 steps every single iteration.
    Removing the 4,201 offsets removed the problem rather than re-coordinatising
    it.

    So the order is reversed from last night's plan: the quadrature arm leads
    and gets the breadth, the centered arm follows as the fallback that is
    known to at least fit. Per-iteration the quadrature is ~2x slower (90 steps
    x 40 ms against 1,023 x 1.9 ms) but each draw is worth far more, and that
    trade is what these fits measure.

    Ordering matters more than usual: the night of 2026-08-06 was lost to a
    gate that could not be satisfied, so the highest-value fits go first and
    nothing waits on anything that has not already been measured.
    """
    MARGALL = ['--marginalize-all', '--n-quad', '21']
    jobs = []

    # 1. The quadrature arm, leading with the two forms brute force could not
    #    reach and then the page primary.
    for i, (prefix, form) in enumerate([('lin', 'linear'),
                                        ('linxg', 'linear_x_gender'),
                                        ('conf', 'quadratic_x_gender'),
                                        ('quad', 'quadratic'),
                                        ('zero', 'zero'),
                                        ('sat', 'saturating'),
                                        ('vtx', 'vertex_quadratic')]):
        jobs.append(fit(f'v10_{prefix}_marg', form, extra=MARGALL,
                        tune=1500, draws=1500, chains=4,
                        seed=NIGHT_SEED + 100 + i, gate='quadrature_viable',
                        note=f'{form}, all offsets integrated out — 40 sampled '
                             'parameters, step size 46x the v7 fits'))

    # 2. The noise floor, on the geometry the sweep above actually ran on.
    #
    #    This replaced a centered fallback arm on 2026-08-07. The centered arm
    #    was queued as insurance in case the quadrature proved slow; it did
    #    not, and v10_lin_marg came back R-hat 1.0000, ESS 762, 0 divergences,
    #    0% at the tree-depth limit against the centered probe's 1.030 / 194 /
    #    100%. Spending ~10 core-hours reproducing a strictly worse
    #    parameterisation to insure against an outcome that already resolved is
    #    not insurance.
    #
    #    What that budget buys instead is the measurement without which the
    #    seven-form sweep cannot be READ. Seven elpds sort into a leaderboard
    #    whether or not the ordering means anything, and the v7 sweep already
    #    demonstrated what happens when nobody checks: a 32.7-elpd spread
    #    against a 31.1-elpd floor of pure seed-to-seed noise, i.e. no ranking
    #    at all. That floor does not carry over here -- it was measured per
    #    OBSERVATION on chains that never mixed, and these fits are per CLIMBER
    #    on chains that did. Neither the unit nor the geometry transfers, so it
    #    has to be measured again.
    #
    #    Identical to v10_lin_marg in every respect except the seed. That is
    #    the point: the gap between the two IS the noise floor.
    jobs.append(fit('v10_lin_marg_s2', 'linear', extra=MARGALL,
                    tune=1500, draws=1500, chains=4,
                    seed=NIGHT_SEED + 200, gate='quadrature_viable',
                    note='linear, same settings as v10_lin_marg, different '
                         'seed — the elpd gap between the two is the noise '
                         'floor the sweep must clear to be a ranking'))
    return jobs


# ---- scheduling ----------------------------------------------------------

def write_status(jobs, log_dir, header):
    lines = [f'# Overnight run — {header}', '',
             f'_updated {time.strftime("%Y-%m-%d %H:%M:%S")}_', '',
             '| job | state | min | note |', '| --- | --- | --- | --- |']
    for j in jobs:
        mins = f'{j.minutes:.0f}' if j.minutes else ''
        state = j.state + (f' (rc {j.rc})' if j.state == 'failed' else '')
        lines.append(f'| `{j.name}` | {state} | {mins} | {j.note} |')
    (log_dir / 'STATUS.md').write_text('\n'.join(lines) + '\n')


def eligible(job, jobs, gate_cache, log):
    """Is this job runnable now? Resolves its dependency and its gate."""
    if job.state != 'pending':
        return False
    dep_ok = True
    if job.after:
        dep = next(j for j in jobs if j.name == job.after)
        if dep.state in ('pending', 'running'):
            return False
        dep_ok = dep.state == 'done'
    if not job.gate:
        if not dep_ok:
            job.state = 'skipped'
            return False
        return True
    key = job.gate.lstrip('!')
    if key not in gate_cache:
        if dep_ok:
            ok, why = GATES[key](jobs)
        else:
            # A gate whose evidence never got written cannot pass. Decided
            # here rather than inside the gate so it holds for every gate,
            # and so a leftover result file from an earlier night cannot
            # answer for a fit that crashed tonight.
            ok, why = False, f'{job.after} did not finish -- treating as FAIL'
        gate_cache[key] = ok
        log(f'[gate] {key}: {"PASS" if ok else "FAIL"} — {why}')
    if gate_cache[key] != (not job.gate.startswith('!')):
        job.state = 'skipped'
        return False
    return True


def run(jobs, cores, log_dir, poll=15):
    log_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src'), **SINGLE_THREAD_BLAS}
    gate_cache = {}
    logfile = open(log_dir / 'overnight.log', 'a', buffering=1)

    def log(msg):
        line = f'[{time.strftime("%H:%M:%S")}] {msg}'
        print(line, flush=True)
        logfile.write(line + '\n')

    log(f'{len(jobs)} jobs, {cores} cores; '
        f'{sorted({j.cores for j in jobs})} core(s) per fit')
    while True:
        running = [j for j in jobs if j.state == 'running']
        for j in list(running):
            if j.proc.poll() is None:
                continue
            j.rc, j.minutes = j.proc.returncode, (time.time() - j.started) / 60
            j.state = 'done' if j.rc == 0 else 'failed'
            log(f'{j.state.upper():6s} {j.name}  {j.minutes:.0f} min')
            running.remove(j)
            write_status(jobs, log_dir, 'in progress')
        while True:
            free = cores - sum(j.cores for j in running)
            # A job wanting more cores than the machine has would never start
            # and would spin here forever; give it the whole machine instead.
            nxt = next((j for j in jobs
                        if (j.cores <= free or (not running and j.cores >= cores))
                        and eligible(j, jobs, gate_cache, log)), None)
            if nxt is None:
                break
            fh = open(log_dir / f'{nxt.name}.log', 'w')
            nxt.proc = subprocess.Popen(nxt.cmd, stdout=fh,
                                        stderr=subprocess.STDOUT,
                                        env=env, cwd=ROOT)
            nxt.state, nxt.started = 'running', time.time()
            running.append(nxt)
            log(f'START  {nxt.name}  ({nxt.note})')
            write_status(jobs, log_dir, 'in progress')
        if not running and all(j.state != 'pending' for j in jobs):
            break
        # A skipped job can unblock nothing, but a pending one whose gate has
        # just been decided can, so re-scan rather than sleeping on a future.
        time.sleep(poll)

    done = [j for j in jobs if j.state == 'done']
    failed = [j for j in jobs if j.state == 'failed']
    write_status(jobs, log_dir, 'finished')
    log(f'finished: {len(done)} ok, {len(failed)} failed, '
        f'{sum(1 for j in jobs if j.state == "skipped")} skipped')
    for j in failed:
        log(f'  FAILED {j.name} — see {log_dir / (j.name + ".log")}')
    logfile.close()
    return 1 if failed else 0


def wait_for_pid(pid, log_dir):
    """Block until `pid` exits. Both jobs want the whole machine."""
    if not pid:
        return
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        print(f'pid {pid} is not running; starting now', flush=True)
        return
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / 'STATUS.md').write_text(
        f'# Overnight run — waiting\n\nWaiting for pid {pid} to exit '
        f'before starting; it holds the cores.\n')
    print(f'waiting for pid {pid} to exit...', flush=True)
    while True:
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            print(f'pid {pid} exited at {time.strftime("%H:%M")}', flush=True)
            return
        time.sleep(30)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--wait-pid', type=int, default=0,
                    help='block until this pid exits before starting '
                         '(0 = start now)')
    ap.add_argument('--cores', type=int,
                    default=max(1, (os.cpu_count() or 4) - 2),
                    help='core budget; one fit occupies 4')
    ap.add_argument('--log-dir', default=str(ROOT / 'runs' / 'logs' / 'overnight'))
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    jobs = build_jobs()
    log_dir = Path(args.log_dir)

    if args.dry_run:
        widest = max(j.cores for j in jobs)
        concurrent = max(1, args.cores // widest)
        est = -(-len(jobs) // concurrent)
        print(f'{len(jobs)} jobs, {args.cores} cores, '
              f'{widest} core(s) per fit -> {concurrent} at a time '
              f'({est} wave(s))\n')
        for j in jobs:
            dep = f'  after {j.after}' if j.after else ''
            gate = f'  gate {j.gate}' if j.gate else ''
            print(f'  {j.name:22s}{dep}{gate}')
            print(f'    {" ".join(j.cmd[2:])}')
        print(f'\nlogs -> {log_dir}')
        return 0

    wait_for_pid(args.wait_pid, log_dir)
    return run(jobs, args.cores, log_dir)


if __name__ == '__main__':
    raise SystemExit(main())
