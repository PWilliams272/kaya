"""The overnight scheduler's dependency and gate logic.

This runs unattended for eight hours with nobody watching, so the failure
mode that matters is not a crash -- a crash leaves a log. It is a night spent
running the wrong branch, or a slot left idle, or a job silently never
becoming eligible. Those are all decided by `eligible()` and the scheduler
loop, neither of which needs a real fit to exercise.

`RUN_FIT` is swapped for a stub that exits immediately, so the whole eleven-job
plan runs in about a second.
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


def test_plan_covers_every_height_form():
    """Q4 must sweep all seven forms, with quad x gender supplied by Q3."""
    jobs = {j.name: j for j in ov.build_jobs()}
    sweep = [n for n, j in jobs.items() if j.gate == 'orthogonal_helped']
    forms = {tuple(j.args)[j.args.index('--height-form') + 1]
             for n, j in jobs.items() if n in sweep}
    assert forms | {'quadratic_x_gender'} == set(ov.HEIGHT_FORMS.values())
    assert len(sweep) == 6           # seventh form IS v6_conf_orth
    assert all('--orthogonal-design' in jobs[n].args for n in sweep)


def test_q2_stays_on_the_raw_basis():
    """Each cell of the 2x2 changes one thing; Q2's baseline is raw-basis."""
    q2 = next(j for j in ov.build_jobs() if j.name == 'v5_conf_marg_long')
    assert '--orthogonal-design' not in q2.args
    assert q2.args[q2.args.index('--tune') + 1] == '2000'
    assert not q2.after and not q2.gate


def test_q4_runs_when_the_gate_passes(stub, tmp_path, monkeypatch):
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(True))
    jobs = ov.build_jobs()
    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 0
    by_name = {j.name: j for j in jobs}
    assert by_name['v3_lin_orth'].state == 'done'
    assert by_name['v5_conf_marg_f'].state == 'skipped'


def test_noise_floor_runs_when_the_gate_fails(stub, tmp_path, monkeypatch):
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(False))
    jobs = ov.build_jobs()
    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 0
    by_name = {j.name: j for j in jobs}
    assert by_name['v3_lin_orth'].state == 'skipped'
    assert by_name['v5_conf_marg_f'].state == 'done'
    # Q2 is independent of the gate and must run on either branch.
    assert by_name['v5_conf_marg_long'].state == 'done'


def test_a_failed_q3_still_leaves_the_night_useful(stub, tmp_path, monkeypatch):
    """If Q3 crashes, the night must fall through to the noise floor.

    And it must do so even with a passing result file sitting on disk from an
    earlier night -- that file describes a different run.
    """
    monkeypatch.setattr(ov, 'RUNS', tmp_path)
    (tmp_path / 'results').mkdir(parents=True)
    (tmp_path / 'results' / 'result_v6_conf_orth.json').write_text(
        json.dumps({'max_rhat': 1.00, 'min_ess': 900,
                    'args': {'orthogonal_design': True}}))
    jobs = ov.build_jobs()
    next(j for j in jobs if j.name == 'v6_conf_orth').name = 'v6_conf_orth_BOOM'
    for j in jobs:
        if j.after == 'v6_conf_orth':
            j.after = 'v6_conf_orth_BOOM'

    assert ov.run(jobs, cores=8, log_dir=tmp_path, poll=0.01) == 1
    by_name = {j.name: j for j in jobs}
    assert by_name['v6_conf_orth_BOOM'].state == 'failed'
    assert by_name['v3_lin_orth'].state == 'skipped'
    assert by_name['v5_conf_marg_f'].state == 'done'
    assert 'did not finish' in (tmp_path / 'overnight.log').read_text()


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
    """Three concurrent fits was measured at 370 min each against ~85."""
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(True))
    jobs = ov.build_jobs()
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
    monkeypatch.setitem(ov.GATES, 'orthogonal_helped', gate_returning(True))
    ov.run(ov.build_jobs(), cores=8, log_dir=tmp_path, poll=0.01)
    status = (tmp_path / 'STATUS.md').read_text()
    assert 'v6_conf_orth' in status and 'finished' in status
    assert 'pending' not in status


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
