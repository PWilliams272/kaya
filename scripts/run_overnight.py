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

from run_batch import CORES_PER_FIT, HEIGHT_FORMS, SINGLE_THREAD_BLAS  # noqa: E402

RUNS = ROOT / 'runs'
# Module level so the scheduler can be exercised against a stub. The thing
# worth testing here is the dependency and gate logic, and it must not cost
# eleven real fits to test it.
RUN_FIT = ROOT / 'scripts' / 'run_fit.py'

# Baseline to beat, from v3_conf_marg: quadratic_x_gender, marginalized, raw
# basis, tune 600 / draws 500. Both numbers are on the raw-basis coefficient
# names, which is why run_fit.py reports the orthogonal fit under those names
# too -- the comparison would be meaningless otherwise.
BASELINE = {'fit': 'v3_conf_marg', 'max_rhat': 1.069, 'min_ess': 48}

COMMON = ['--network', 'net50', '--name-filter', 'confident',
          '--fixed-sigma-link', '--marginalize-singles', '--chains', '4']


@dataclass
class Job:
    name: str
    args: list
    after: str = ''          # must finish before this one is eligible
    gate: str = ''           # name of a gate function in GATES
    note: str = ''
    state: str = 'pending'   # pending | running | done | failed | skipped
    rc: int | None = None
    minutes: float = 0.0
    started: float = 0.0
    proc: subprocess.Popen | None = field(default=None, repr=False)

    @property
    def cmd(self):
        return [sys.executable, str(RUN_FIT), '--name', self.name] + self.args


def fit(name, height_form, *, extra=(), tune=600, draws=500, **kw):
    return Job(name=name,
               args=COMMON + ['--height-form', height_form,
                              '--tune', str(tune), '--draws', str(draws)]
                    + list(extra),
               **kw)


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


GATES = {'orthogonal_helped': orthogonal_helped}


# ---- the plan ------------------------------------------------------------

def build_jobs():
    jobs = [
        # Q2 -- the long-warm-up cell. Raw basis on purpose: each cell of the
        # 2x2 changes one thing, and its baseline is a raw-basis fit.
        fit('v5_conf_marg_long', 'quadratic_x_gender', tune=2000, draws=2000,
            note='Q2: does a long warm-up alone fix R-hat?'),
        # Q3 -- the orthogonal cell, at baseline settings so it is directly
        # comparable to v3_conf_marg.
        fit('v6_conf_orth', 'quadratic_x_gender', extra=['--orthogonal-design'],
            note='Q3: does an orthogonal design fix the height block?'),
    ]
    # Q4 -- the sweep, if Q3 earned it. quadratic_x_gender is already covered
    # by v6_conf_orth itself, so it is not refitted here.
    for prefix, form in HEIGHT_FORMS.items():
        if form == 'quadratic_x_gender':
            continue
        jobs.append(fit(f'{prefix}_orth', form, extra=['--orthogonal-design'],
                        after='v6_conf_orth', gate='orthogonal_helped',
                        note=f'Q4: {form}, orthogonal basis'))
    # The fallback branch. Same gate, opposite sense -- see the module
    # docstring for why this cannot just run alongside.
    for tag in 'fgh':
        jobs.append(fit(f'v5_conf_marg_{tag}', 'quadratic_x_gender',
                        after='v6_conf_orth', gate='!orthogonal_helped',
                        note='Phase 3: noise floor, raw basis'))
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
    slots = max(1, cores // CORES_PER_FIT)
    log_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src'), **SINGLE_THREAD_BLAS}
    gate_cache = {}
    logfile = open(log_dir / 'overnight.log', 'a', buffering=1)

    def log(msg):
        line = f'[{time.strftime("%H:%M:%S")}] {msg}'
        print(line, flush=True)
        logfile.write(line + '\n')

    log(f'{len(jobs)} jobs, {slots} slots ({cores} cores / {CORES_PER_FIT} per fit)')
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
        while len(running) < slots:
            nxt = next((j for j in jobs if eligible(j, jobs, gate_cache, log)),
                       None)
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
    slots = max(1, args.cores // CORES_PER_FIT)

    if args.dry_run:
        print(f'{len(jobs)} jobs, {slots} slots '
              f'({args.cores} cores / {CORES_PER_FIT} per fit)\n')
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
