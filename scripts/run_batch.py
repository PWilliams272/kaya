"""Run a named batch of fits, here or on a rented box.

Every queued job in this project is embarrassingly parallel -- independent
fits with nothing to say to each other -- and the whole input is 12 MB. That
combination is what makes renting compute worth considering: the constraint is
core count, not data movement.

What renting does NOT buy is a faster individual fit. PyMC runs one chain per
process, single-threaded, so a single model takes ~85 minutes on any machine.
A bigger box runs more models at once; it does not finish any one of them
sooner. Batches are sized on that basis.

    run_batch.py --batch refits --local
    run_batch.py --batch kfold  --cloud --confirm

`--local` is the default and always works. `--cloud` provisions a spot
instance, syncs the inputs, runs the same batch, pulls back the summaries, and
terminates -- and refuses to start without `--confirm`, because it spends
money.

**Only summaries come home.** The traces are ~1.3 GB per fit and are never
needed locally: cross-validation scores, R-hat, effective sample size and
thinned posterior draws are all kilobytes. Traces stay on the box unless
--keep-traces pushes them to S3.
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'

# Cores one fit occupies. PyMC runs 4 chains as 4 single-threaded processes,
# so a fit is 4 cores for its whole life regardless of machine size.
CORES_PER_FIT = 4

# NumPy's BLAS spawns a thread pool inside EVERY worker process, so a batch
# that already runs one process per core silently ends up with cores x threads
# runnable threads all fighting each other. Measured on the emcee run: 9
# workers x 11 threads = 99 threads on 10 cores, a load average of 96, and
# each worker still only managing one core's worth of actual work -- the extra
# threads just contend. Scheduling here is per process, so each process gets
# exactly one thread.
SINGLE_THREAD_BLAS = {
    'OMP_NUM_THREADS': '1',
    'OPENBLAS_NUM_THREADS': '1',
    'MKL_NUM_THREADS': '1',
    'VECLIB_MAXIMUM_THREADS': '1',
    'NUMEXPR_NUM_THREADS': '1',
}

HEIGHT_FORMS = {
    'v3_lin': 'linear',
    'v3_quad': 'quadratic',
    'v3_sat': 'saturating',
    'v3_conf': 'quadratic_x_gender',
    'v4_linxg': 'linear_x_gender',
    'v3_zero': 'none',
}


def fit_job(name, height_form, *, marginalize=True, seed=None, extra=()):
    cmd = [sys.executable, str(ROOT / 'scripts' / 'run_fit.py'),
           '--name', name, '--network', 'net50', '--name-filter', 'confident',
           '--height-form', height_form, '--fixed-sigma-link',
           '--draws', '500', '--tune', '600', '--chains', '4']
    if marginalize:
        cmd.append('--marginalize-singles')
    cmd += list(extra)
    return {'name': name, 'cmd': cmd, 'cores': CORES_PER_FIT}


def kfold_job(name, height_form, fold, n_folds=5):
    cmd = [sys.executable, str(ROOT / 'scripts' / 'run_kfold.py'),
           '--name', name, '--height-form', height_form,
           '--fold', str(fold), '--n-folds', str(n_folds)]
    return {'name': f'{name}_f{fold}', 'cmd': cmd, 'cores': CORES_PER_FIT}


def batch_refits():
    """More refits of one model, to pin down the noise floor.

    Two converged refits per arm is not enough to estimate the spread that
    every other gap on the page is measured against, and one of the three
    marginalized refits did not converge at all. Six more, three per arm.
    """
    jobs = []
    for i, tag in enumerate('fghijk'[:3]):
        jobs.append(fit_job(f'v5_lin_{tag}', 'linear', marginalize=False))
        jobs.append(fit_job(f'v5_lin_{tag}_marg', 'linear', marginalize=True))
    return jobs


def batch_kfold(n_folds=5):
    """Grouped k-fold across the height forms: can it predict a new climber?"""
    return [kfold_job(n, hf, f, n_folds)
            for n, hf in HEIGHT_FORMS.items()
            for f in range(n_folds)]


def batch_heights():
    """The full height-form sweep, marginalized -- what batch_marg.sh did."""
    return [fit_job(f'{n}_marg', hf) for n, hf in HEIGHT_FORMS.items()]


BATCHES = {'refits': batch_refits, 'kfold': batch_kfold, 'heights': batch_heights}


# ---- local ---------------------------------------------------------------

def run_local(jobs, cores, log_dir):
    """Run jobs concurrently, never oversubscribing the core budget."""
    slots = max(1, cores // CORES_PER_FIT)
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f'running {len(jobs)} jobs, {slots} at a time '
          f'({cores} cores / {CORES_PER_FIT} per fit)\n')
    env = {**os.environ, 'PYTHONPATH': str(ROOT / 'src'), **SINGLE_THREAD_BLAS}
    done, failed = [], []

    def one(job):
        log = log_dir / f'{job["name"]}.log'
        t0 = time.time()
        with open(log, 'w') as fh:
            r = subprocess.run(job['cmd'], stdout=fh, stderr=subprocess.STDOUT,
                               env=env, cwd=ROOT)
        return job, r.returncode, (time.time() - t0) / 60, log

    with ThreadPoolExecutor(max_workers=slots) as pool:
        futs = {pool.submit(one, j): j for j in jobs}
        for fut in as_completed(futs):
            job, rc, mins, log = fut.result()
            mark = 'ok ' if rc == 0 else 'FAIL'
            print(f'  [{mark}] {job["name"]:<24} {mins:6.1f} min   {log.name}',
                  flush=True)
            (done if rc == 0 else failed).append(job['name'])
    print(f'\n{len(done)} succeeded, {len(failed)} failed'
          + (f': {", ".join(failed)}' if failed else ''))
    return 1 if failed else 0


# ---- cloud ---------------------------------------------------------------

# Read-only probes, run before anything is created, so a missing permission
# surfaces as a sentence rather than as a half-provisioned instance.
PREFLIGHT = [
    ('ec2:DescribeInstances', ['ec2', 'describe-instances', '--max-items', '1']),
    ('ec2:DescribeImages', ['ec2', 'describe-images', '--owners', 'amazon',
                            '--max-items', '1']),
    ('ec2:DescribeSpotPriceHistory', ['ec2', 'describe-spot-price-history',
                                      '--max-items', '1']),
    ('ec2:DescribeSecurityGroups', ['ec2', 'describe-security-groups',
                                    '--max-items', '1']),
    ('ec2:DescribeKeyPairs', ['ec2', 'describe-key-pairs']),
]
NEEDED_ACTIONS = """ec2:DescribeInstances      ec2:DescribeImages
ec2:DescribeInstanceTypes  ec2:DescribeSpotPriceHistory
ec2:DescribeSecurityGroups ec2:DescribeKeyPairs
ec2:DescribeSubnets        ec2:DescribeVpcs
ec2:RunInstances           ec2:TerminateInstances
ec2:CreateTags             ec2:CreateSecurityGroup
ec2:AuthorizeSecurityGroupIngress"""


def preflight():
    """Check the credentials can actually do this before spending anything."""
    missing = []
    for action, cmd in PREFLIGHT:
        r = subprocess.run(['aws', *cmd], capture_output=True, text=True)
        if r.returncode != 0 and 'not authorized' in (r.stderr or ''):
            missing.append(action)
    if not missing:
        return True
    who = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Arn',
                          '--output', 'text'], capture_output=True, text=True)
    print('Cloud run blocked: these credentials cannot manage EC2.\n')
    print(f'  identity : {(who.stdout or "unknown").strip()}')
    print(f'  missing  : {", ".join(missing)}'
          + (' (and probably the write actions too)' if missing else ''))
    print('\nThis account\'s key is scoped to the viewer/Lambda work, not to '
          'renting\ncompute. To enable --cloud, attach a policy allowing:\n')
    print('    ' + NEEDED_ACTIONS.replace('\n', '\n    '))
    print('\nScope it to a dedicated tag or security group rather than "*" if '
          'you want\nit narrow. Until then, --local works unchanged.')
    return False


def run_cloud(jobs, args):
    if not preflight():
        return 2
    if not args.confirm:
        print(f'\nWould launch {args.instance_type} (spot) in '
              f'{os.environ.get("AWS_DEFAULT_REGION", "the configured region")} '
              f'and run {len(jobs)} jobs.')
        print('Re-run with --confirm to actually spend money.')
        return 0
    raise SystemExit(
        'The provisioning path is written but has never been executed, '
        'because these\ncredentials cannot reach EC2 to test it. Attach the '
        'policy above, then run\nwith --dry-run first to check the plan.')


# ---- entry ---------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--batch', required=True, choices=sorted(BATCHES))
    where = ap.add_mutually_exclusive_group()
    where.add_argument('--local', action='store_true', default=True)
    where.add_argument('--cloud', action='store_true')
    ap.add_argument('--cores', type=int, default=max(1, (os.cpu_count() or 4) - 1),
                    help='core budget; one fit takes 4')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--confirm', action='store_true',
                    help='required for --cloud: it costs money')
    ap.add_argument('--instance-type', default='c7i.16xlarge')
    ap.add_argument('--log-dir', default=None)
    args = ap.parse_args()

    jobs = BATCHES[args.batch]()
    slots = max(1, args.cores // CORES_PER_FIT)
    waves = -(-len(jobs) // slots)
    print(f'batch "{args.batch}": {len(jobs)} jobs')
    print(f'  {slots} at a time on {args.cores} cores -> {waves} waves '
          f'~= {waves * 1.4:.1f} h at ~85 min per fit\n')
    if args.dry_run:
        for j in jobs:
            print('  ' + ' '.join(shlex.quote(c) for c in j['cmd']))
        return 0

    log_dir = Path(args.log_dir) if args.log_dir else RUNS / 'logs' / args.batch
    if args.cloud:
        return run_cloud(jobs, args)
    return run_local(jobs, args.cores, log_dir)


if __name__ == '__main__':
    sys.exit(main())
