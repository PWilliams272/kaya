"""The overnight scheduler's dependency and gate logic.

This runs unattended for eight hours with nobody watching, so the failure
mode that matters is not a crash -- a crash leaves a log. It is a night spent
running the wrong branch, or a slot left idle, or a job silently never
becoming eligible. Those are all decided by `eligible()` and the scheduler
loop, neither of which needs a real fit to exercise.

`RUN_FIT` is swapped for a stub that exits immediately, so a whole plan runs in
about a second.

The gate and dependency machinery is exercised against SYNTHETIC jobs rather
than whatever `build_jobs()` currently returns. The plan changes whenever a
question is answered -- it has already gone from a gated 2x2 to a flat sweep --
and tests that reach into it turn every planning decision into a test edit
while quietly losing coverage of the scheduler itself.
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

import run_overnight as ov  # noqa: E402

STUB = '''import sys, time
# Accepts and ignores run_fit.py's arguments; exits with the code encoded in
# the fit name so a test can force a failure.
name = sys.argv[sys.argv.index('--name') + 1]
time.sleep(0.05)
sys.exit(7 if 'BOOM' in name else 0)
'''


@pytest.fixture
def stub(tmp_path, monkeypatch):
    p = tmp_path / 'stub_fit.py'
    p.write_text(STUB)
    monkeypatch.setattr(ov, 'RUN_FIT', p)
    return p


def gate_returning(value):
    return lambda jobs: (value, 'stubbed')


def synthetic_plan():
    """A lead job, a gated branch, and its mutually exclusive fallback.

    The same shape any gated plan has, independent of which questions are
    live this week.
    """
    return [
        ov.Job(name='lead', args=['--x'], note='the gating fit'),
        ov.Job(name='branch_a', args=['--x'], after='lead',
               gate='orthogonal_helped', note='runs if the gate passes'),
        ov.Job(name='branch_b', args=['--x'], after='lead',
               gate='!orthogonal_helped', note='runs if it fails'),
        ov.Job(name='independent', args=['--x'], note='no dependency'),
    ]


def test_the_gated_branch_runs_and_its_opposite_is_skipped(stub, tmp_path, monkeypatch):
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(True))
    jobs = synthetic_plan()
    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 0
    by_name = {j.name: j for j in jobs}
    assert by_name['branch_a'].state == 'done'
    assert by_name['branch_b'].state == 'skipped'
    assert by_name['independent'].state == 'done'


def test_the_opposite_branch_runs_when_the_gate_fails(stub, tmp_path, monkeypatch):
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(False))
    jobs = synthetic_plan()
    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 0
    by_name = {j.name: j for j in jobs}
    assert by_name['branch_a'].state == 'skipped'
    assert by_name['branch_b'].state == 'done'
    # An ungated job must run whichever way the gate went.
    assert by_name['independent'].state == 'done'


def test_a_failed_lead_falls_through_rather_than_gating_on_a_stale_file(
        stub, tmp_path, monkeypatch):
    """A crashed gating fit must not let yesterday's result answer for it."""
    monkeypatch.setattr(ov, 'RUNS', tmp_path)
    (tmp_path / 'results').mkdir(parents=True)
    (tmp_path / 'results' / 'result_v6_conf_orth.json').write_text(
        json.dumps({'max_rhat': 1.00, 'min_ess': 900,
                    'args': {'orthogonal_design': True}}))
    jobs = synthetic_plan()
    jobs[0].name = 'lead_BOOM'
    for j in jobs:
        if j.after == 'lead':
            j.after = 'lead_BOOM'

    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 1
    by_name = {j.name: j for j in jobs}
    assert by_name['lead_BOOM'].state == 'failed'
    assert by_name['branch_a'].state == 'skipped'
    assert by_name['branch_b'].state == 'done'
    assert 'did not finish' in (tmp_path / 'overnight.log').read_text()


# --- the plan itself: only what must be true of ANY plan --------------------

def test_the_quadrature_arm_sweeps_every_height_form_at_matched_settings():
    """Forms fitted at different settings are not comparable to each other.

    The quadrature arm produces the height-form comparison -- it is the
    parameterisation that measurably fixed the geometry (step size 0.00307 ->
    0.141) -- so its settings must match across forms. The noise-floor
    replicate also carries --marginalize-all and deliberately repeats the
    linear form, so it is excluded from the form set and checked separately by
    test_the_noise_floor_replicate_differs_only_in_its_seed. The advancement
    arm repeats forms for the same reason -- it is a paired comparison, not a
    sweep entry -- and is checked by
    test_the_advancement_arm_is_paired_against_its_twin.
    """
    import run_batch
    marg = [j for j in ov.build_jobs()
            if '--marginalize-all' in j.args and not j.name.endswith('_s2')
            and '--advancement' not in j.args]
    forms = [j.args[j.args.index('--height-form') + 1] for j in marg]
    assert set(forms) == set(run_batch.HEIGHT_FORMS.values())
    assert len(forms) == len(set(forms)), 'a form is fitted twice'
    for flag in ('--tune', '--draws', '--chains', '--n-quad'):
        values = {j.args[j.args.index(flag) + 1] for j in marg}
        assert len(values) == 1, f'{flag} differs between forms: {values}'


def test_the_arm_that_fixed_the_geometry_leads():
    """Ordering is the plan's only defence against running out of time.

    Measured 2026-08-07 on matched linear fits: centering moved R-hat
    1.040 -> 1.030 and left the step size and the 100%-at-tree-depth-limit
    untouched, while the quadrature took the step size to 0.141 and let
    trajectories finish. The centered arm was retired on the strength of that
    -- see build_jobs -- so the whole plan is now quadrature.
    """
    jobs = ov.build_jobs()
    assert '--marginalize-all' in jobs[0].args
    assert jobs[0].name == 'v10_lin_marg', 'the worst form should go first'
    assert all('--marginalize-all' in j.args for j in jobs), (
        'every job should be on the parameterisation that worked')
    assert not any('--center-user-offsets' in j.args for j in jobs), (
        'the centered arm was retired; re-adding it needs a new measurement')


def test_the_noise_floor_replicate_differs_only_in_its_seed():
    """Without it the sweep is a leaderboard nobody can read.

    The v7 sweep spread 32.7 elpd against a 31.1-elpd floor of pure
    seed-to-seed noise -- no ranking at all. That floor does not transfer to
    these fits: it was measured per OBSERVATION on chains that never mixed,
    and these are per CLIMBER on chains that did. So it is measured again
    here, and the only way that measurement means anything is if the replicate
    is identical to its twin apart from the seed.
    """
    jobs = {j.name: j for j in ov.build_jobs()}
    base, rep = jobs['v10_lin_marg'], jobs['v10_lin_marg_s2']

    def without_seed(args):
        a = list(args)
        i = a.index('--seed')
        return a[:i] + a[i + 2:]

    assert without_seed(base.args) == without_seed(rep.args), (
        'the replicate must differ from its twin in the seed and nothing else')
    assert base.args[base.args.index('--seed') + 1] != \
        rep.args[rep.args.index('--seed') + 1], 'a shared seed measures nothing'


def test_the_advancement_arm_is_paired_against_its_twin():
    """A paired difference only cancels shared variation if it IS paired.

    v11_lin_adv exists to be subtracted from v10_lin_marg. Any settings
    difference beyond the offset itself -- a different tune, a different
    n_quad, a different network -- turns that subtraction into a comparison of
    two unrelated things, and the paired standard error (0.63 against a raw
    284.8 on the height sweep) stops being the right yardstick.
    """
    jobs = {j.name: j for j in ov.build_jobs()}
    for prefix in ('lin', 'quad'):
        base, adv = jobs[f'v10_{prefix}_marg'], jobs[f'v11_{prefix}_adv']

        def strip(args):
            a = [x for x in args if x != '--advancement']
            i = a.index('--seed')
            return a[:i] + a[i + 2:]

        assert strip(base.args) == strip(adv.args), (
            f'v11_{prefix}_adv differs from its twin in more than the offset')
        assert '--advancement' in adv.args
        assert '--advancement' not in base.args
        assert base.args[base.args.index('--seed') + 1] != \
            adv.args[adv.args.index('--seed') + 1], (
                'a shared seed makes the two the same chains twice')


def test_every_job_is_gated_on_the_quadrature():
    """One arm now, so one gate -- but the gate must still be there.

    An ungated plan runs whatever it is given, including on evidence that the
    quadrature never finished its trajectories.
    """
    jobs = ov.build_jobs()
    assert {j.gate for j in jobs} == {'quadrature_viable'}
    assert 'quadrature_viable' in ov.GATES


def test_a_gate_survives_a_result_file_that_predates_tree_depth_recording():
    """The bug that cost the night of 2026-08-06.

    run_fit only began recording tree depth partway through that evening, so
    the probe launched minutes earlier wrote a result without it. The gate
    read the missing field as "cannot tell", failed, and skipped all eight
    jobs -- with the answer sitting in the trace on disk the whole time.
    """
    import inspect
    src = inspect.getsource(ov.quadrature_viable)
    assert '_tree_depth_from_trace' in src, (
        'the gate must fall back to the trace rather than idling the machine')


def test_the_plan_is_off_the_orthogonal_basis():
    """Dropped 2026-08-06: paired ESS ratio 1.01x over seven forms, 4 wins of 7.

    It is also not prior-preserving and it costs a stored transform that
    cross-validation has to apply per fold. If this flag comes back it should
    be because a measurement changed, not because a default drifted.
    """
    assert not any('--orthogonal-design' in j.args for j in ov.build_jobs())


def test_every_job_carries_a_distinct_seed():
    """The plan is incremental, which only works if runs are distinguishable.

    Without --seed PyMC draws a fresh one per run: fits are irreproducible, and
    a top-up cannot be told apart from a repeat. Two runs that shared a seed
    are the SAME chains -- merging them makes between-chain variance zero, so
    R-hat reads 1.000 no matter how badly the sampler mixed.
    """
    jobs = ov.build_jobs()
    seeds = [j.args[j.args.index('--seed') + 1] for j in jobs]
    assert len(seeds) == len(jobs), 'a job has no --seed'
    assert len(set(seeds)) == len(seeds), f'duplicate seeds in the plan: {seeds}'


def test_the_plan_stays_at_four_chains_and_tops_up_later():
    """4 chains x 7 forms fits two at a time; 8 would spend the compute up front.

    More chains IS the fix for R-hat's noise at m=4, but chains merge exactly
    (see tests/test_merge_chains.py), so it can be bought per-form afterwards
    for only the forms that need it.
    """
    for j in ov.build_jobs():
        assert int(j.args[j.args.index('--chains') + 1]) == 4


def test_core_budget_matches_the_chain_count():
    """Admission is by cores; a job claiming fewer than it spawns oversubscribes."""
    for j in ov.build_jobs():
        assert j.cores == int(j.args[j.args.index('--chains') + 1])


def test_a_stale_raw_basis_result_cannot_gate(tmp_path, monkeypatch):
    """Yesterday's v3_conf_marg-shaped result must not authorise a sweep."""
    monkeypatch.setattr(ov, 'RUNS', tmp_path)
    (tmp_path / 'results').mkdir(parents=True)
    (tmp_path / 'results' / 'result_v6_conf_orth.json').write_text(
        json.dumps({'max_rhat': 1.00, 'min_ess': 900,
                    'args': {'orthogonal_design': False}}))
    ok, why = ov.orthogonal_helped([])
    assert not ok and 'stale file' in why


def test_never_exceeds_the_core_budget(stub, tmp_path, monkeypatch):
    """Three concurrent 4-chain fits was measured at 370 min each against ~85.

    Driven with 4-core jobs so the budget actually binds: the live plan is
    8-core, which admits one at a time and would pass trivially.
    """
    jobs = [ov.Job(name=f'j{i}', args=['--x'], cores=4) for i in range(5)]
    peak = {'n': 0}
    real_popen = ov.subprocess.Popen

    def counting_popen(*a, **k):
        live = sum(1 for j in jobs
                   if j.state == 'running' and j.proc and j.proc.poll() is None)
        peak['n'] = max(peak['n'], live + 1)
        return real_popen(*a, **k)

    monkeypatch.setattr(ov.subprocess, 'Popen', counting_popen)
    ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01)
    assert peak['n'] <= 2


def test_status_file_is_written_for_the_morning(stub, tmp_path, monkeypatch):
    jobs = ov.build_jobs()
    ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01)
    status = (tmp_path / 'STATUS.md').read_text()
    assert 'finished' in status
    assert 'pending' not in status
    for j in jobs:
        assert j.name in status, f'{j.name} missing from the morning summary'


def test_gate_reads_the_real_result_shape(tmp_path, monkeypatch):
    """The gate must survive the JSON run_fit.py actually writes."""
    monkeypatch.setattr(ov, 'RUNS', tmp_path)
    (tmp_path / 'results').mkdir(parents=True)
    p = tmp_path / 'results' / 'result_v6_conf_orth.json'

    p.write_text(json.dumps({'max_rhat': 1.02, 'min_ess': 200,
                             'orth': {'max_rhat': 1.01, 'min_ess': 260}}))
    ok, why = ov.orthogonal_helped([])
    assert ok and 'sampled basis' in why

    # Worse on both headline numbers than v3_conf_marg -> do not spend a night.
    p.write_text(json.dumps({'max_rhat': 1.20, 'min_ess': 12}))
    ok, _ = ov.orthogonal_helped([])
    assert not ok

    # Better on only one of the two is enough; both are noisy at 500 draws.
    p.write_text(json.dumps({'max_rhat': 1.20, 'min_ess': 60}))
    assert ov.orthogonal_helped([])[0]
    p.write_text(json.dumps({'max_rhat': 1.01, 'min_ess': 12}))
    assert ov.orthogonal_helped([])[0]
