"""Tests for the shared convergence gate.

The rule being enforced: a fit whose chains did not agree is not a measurement,
and must not silently become a number on a page. These tests pin the two-level
verdict — `converged` (safe to quote) vs `usable` (reportable as a diagnostic,
but never as the yardstick) — because the Grading Model v2 page deliberately
presents a failed refit as a finding, so a single boolean would be wrong.
"""
from kaya.convergence import (
    ESS_MIN,
    RHAT_CONVERGED,
    RHAT_GATE,
    assess,
    assess_result,
)


def test_clean_fit_converges() -> None:
    verdict = assess(max_rhat=1.00, min_ess=2000, divergences=0)
    assert verdict.converged
    assert verdict.usable
    assert verdict.reasons == []
    assert verdict.describe() == 'converged'


def test_thresholds_are_ordered_as_documented() -> None:
    assert RHAT_CONVERGED < RHAT_GATE


def test_rhat_between_the_thresholds_is_usable_but_not_converged() -> None:
    """The band the page reports as a diagnostic without quoting it."""
    verdict = assess(max_rhat=1.09, min_ess=2000, divergences=0)
    assert not verdict.converged
    assert verdict.usable
    assert any('1.09' in reason for reason in verdict.reasons)


def test_rhat_above_the_gate_is_not_usable() -> None:
    """R-hat 1.44 with ESS 8 is the real failed refit this page discusses."""
    verdict = assess(max_rhat=1.44, min_ess=8, divergences=0)
    assert not verdict.converged
    assert not verdict.usable
    assert verdict.describe().startswith('NOT usable')


def test_gate_boundary_is_inclusive() -> None:
    assert assess(max_rhat=RHAT_GATE, min_ess=2000, divergences=0).usable
    assert not assess(max_rhat=RHAT_GATE + 0.001, min_ess=2000, divergences=0).usable


def test_low_effective_sample_size_fails_convergence() -> None:
    verdict = assess(max_rhat=1.00, min_ess=ESS_MIN - 1, divergences=0)
    assert not verdict.converged
    assert verdict.usable, 'a thin but agreeing chain is still usable, just not clean'
    assert any('effective sample size' in reason for reason in verdict.reasons)


def test_divergences_fail_convergence() -> None:
    verdict = assess(max_rhat=1.00, min_ess=2000, divergences=32)
    assert not verdict.converged
    assert any('32 divergent' in reason for reason in verdict.reasons)


def test_missing_diagnostics_are_failures_not_passes() -> None:
    """An unknown R-hat is exactly what this gate exists to catch."""
    verdict = assess()
    assert not verdict.converged
    assert not verdict.usable
    assert any('no R-hat' in reason for reason in verdict.reasons)


def test_assess_result_reads_a_fit_results_dict() -> None:
    """The shape scripts/run_fit.py writes."""
    result = {
        'name': 'v3_conf',
        'max_rhat': 1.44,
        'min_ess': 8.0,
        'divergences': 0,
    }
    verdict = assess_result(result)
    assert not verdict.usable
    assert verdict.max_rhat == 1.44

    payload = verdict.as_dict()
    assert payload['converged'] is False
    assert payload['usable'] is False
    assert payload['max_rhat'] == 1.44
    assert payload['reasons']


def test_as_dict_is_json_serializable() -> None:
    """It is written straight into viewer payloads."""
    import json

    json.dumps(assess(max_rhat=1.0, min_ess=1000, divergences=0).as_dict())
