"""Tests for the shared convergence gate.

The rule being enforced: a fit whose chains did not agree is not a measurement,
and must not silently become a number on a page. These tests pin the two-level
verdict — `converged` (safe to quote) vs `usable` (reportable as a diagnostic,
but never as the yardstick) — because the Grading Model v2 page deliberately
presents a failed refit as a finding, so a single boolean would be wrong.
"""
from kaya import convergence as conv
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


# --- frozen chains -----------------------------------------------------------
#
# Added 2026-08-07. Two v10 fits sharing no height parameters both reported
# R-hat 1.53 / ESS 7 / 1,500 divergences -- identical to three significant
# figures, which is not something two different models do. Chain 3 of each had
# adapted its step size to exactly 0.0 and never left its initial point. The
# diagnostics were describing the dead chain. Dropping it, quadratic_x_gender
# went from apparent total failure to the best-scoring form in the sweep.

class _Stub:
    """Minimal stand-in for an InferenceData, to keep these tests fast."""

    def __init__(self, sample_stats=None, posterior=None):
        if sample_stats is not None:
            self.sample_stats = sample_stats
        if posterior is not None:
            self.posterior = posterior


class _Vars(dict):
    """A mapping that also exposes `data_vars`, like an xarray Dataset."""

    def __contains__(self, k):
        return dict.__contains__(self, k)

    @property
    def data_vars(self):
        return list(self.keys())

    def __getitem__(self, k):
        return dict.__getitem__(self, k)


def test_a_chain_whose_step_size_hit_zero_is_reported_frozen():
    import numpy as np
    stats = _Vars({'step_size': np.array([[0.13] * 5, [0.09] * 5,
                                          [0.08] * 5, [0.0] * 5])})
    assert conv.frozen_chains(_Stub(sample_stats=stats)) == [3]


def test_healthy_chains_report_none_frozen():
    import numpy as np
    stats = _Vars({'step_size': np.array([[0.13] * 5, [0.09] * 5])})
    assert conv.frozen_chains(_Stub(sample_stats=stats)) == []


def test_a_nonfinite_step_size_counts_as_frozen():
    import numpy as np
    stats = _Vars({'step_size': np.array([[0.13] * 5, [np.nan] * 5])})
    assert conv.frozen_chains(_Stub(sample_stats=stats)) == [1]


def test_without_a_step_size_a_zero_variance_chain_is_still_caught():
    """Not every sampler records a step size; a chain that never moved is
    still detectable from the draws themselves."""
    import numpy as np
    post = _Vars({'beta0': np.array([[1.0, 1.2, 0.9], [2.0, 2.0, 2.0]])})
    assert conv.frozen_chains(_Stub(posterior=post)) == [1]


def test_a_frozen_chain_makes_the_fit_unusable_and_says_why_first():
    v = conv.assess(max_rhat=1.53, min_ess=7.0, divergences=1500, frozen=[3])
    assert not v.usable, 'a fit with a dead chain is not a measurement'
    assert v.frozen_chains == [3]
    # Ordering matters: every other reason is downstream of the dead chain and
    # reads as a model fault when it is a sampler fault.
    assert 'never moved' in v.reasons[0]
    assert 're-run at a different seed' in v.reasons[0]


def test_the_frozen_list_survives_the_round_trip_through_a_result_dict():
    v = conv.assess_result({'max_rhat': 1.53, 'min_ess': 7.0,
                            'divergences': 1500, 'frozen_chains': [3]})
    assert v.frozen_chains == [3]
    assert v.as_dict()['frozen_chains'] == [3]


def test_a_healthy_fit_is_unaffected_by_the_new_field():
    v = conv.assess_result({'max_rhat': 1.0, 'min_ess': 762.0,
                            'divergences': 0})
    assert v.converged and v.usable and v.frozen_chains == []
