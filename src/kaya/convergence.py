"""Shared convergence thresholds and verdicts for the grading-model fits.

Convergence is a gate, not a report. A fit whose chains did not agree is not a
measurement, and it must not silently become a number on the viewer's pages.

Two thresholds, because this repo needs both:

* ``RHAT_CONVERGED`` — the bar a healthy fit clears. Below it the numbers are
  trustworthy on their own.
* ``RHAT_GATE`` — the bar below which a fit is not evidence at all. Between the
  two, a fit is reportable *as a diagnostic* but must not feed an aggregate,
  and must not be the headline.

That middle band is deliberate. The Grading Model v2 page presents a refit that
reached R-hat 1.44 as a finding in its own right — "this model does not fit
dependably on every seed" is a real result — so blanket-excluding failed fits
would delete content the writeup conventions specifically want kept. What must
never happen is a failed fit being *quoted as a measurement*.

``RHAT_GATE`` was already in use at 1.2 in ``scripts/build_v2_reliability.py``;
it lives here now so every builder agrees on one number.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Chains agree this well on a healthy fit. arviz's own guidance, and what the
# viewer's diagnostics table tells the reader to want.
RHAT_CONVERGED = 1.01

# Above this, between-chain disagreement is large enough that the posterior
# summary describes the sampler, not the model.
RHAT_GATE = 1.2

# Draws are autocorrelated, so this is what they are actually worth.
ESS_MIN = 400


@dataclass
class ConvergenceVerdict:
    """What the diagnostics say about one fit.

    Attributes:
        converged: Clears every threshold — safe to quote as a measurement.
        usable: Clears ``RHAT_GATE``. Below this the fit is a diagnostic only
            and must be kept out of aggregates, rankings, and headline numbers.
        max_rhat: Worst Gelman-Rubin statistic across the reported parameters.
        min_ess: Smallest bulk effective sample size across those parameters.
        divergences: Count of divergent transitions.
        reasons: Human-readable failures, empty when ``converged``.
    """

    converged: bool
    usable: bool
    max_rhat: Optional[float]
    min_ess: Optional[float]
    divergences: Optional[int]
    reasons: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        """Shape written into fit results and viewer payloads."""
        return {
            'converged': self.converged,
            'usable': self.usable,
            'max_rhat': self.max_rhat,
            'min_ess': self.min_ess,
            'divergences': self.divergences,
            'reasons': list(self.reasons),
        }

    def describe(self) -> str:
        """One line for a log or a console warning."""
        if self.converged:
            return 'converged'
        return ('NOT usable: ' if not self.usable else 'not converged: ') + '; '.join(self.reasons)


def assess(
    max_rhat: Optional[float] = None,
    min_ess: Optional[float] = None,
    divergences: Optional[int] = None,
) -> ConvergenceVerdict:
    """Judge one fit's diagnostics against the shared thresholds.

    Missing diagnostics are treated as failures, not as passes — an unknown
    R-hat is exactly the case this gate exists to catch.

    Args:
        max_rhat: Worst R-hat across reported parameters.
        min_ess: Smallest bulk effective sample size.
        divergences: Number of divergent transitions.

    Returns:
        The verdict, with `reasons` listing every threshold that failed.
    """
    reasons: List[str] = []

    if max_rhat is None:
        reasons.append('no R-hat reported')
    elif max_rhat > RHAT_GATE:
        reasons.append(f'R-hat {max_rhat:.3f} exceeds the gate of {RHAT_GATE}')
    elif max_rhat > RHAT_CONVERGED:
        reasons.append(f'R-hat {max_rhat:.3f} above {RHAT_CONVERGED}')

    if min_ess is None:
        reasons.append('no effective sample size reported')
    elif min_ess < ESS_MIN:
        reasons.append(f'effective sample size {min_ess:.0f} below {ESS_MIN}')

    if divergences:
        reasons.append(f'{divergences} divergent transitions')

    usable = max_rhat is not None and max_rhat <= RHAT_GATE
    return ConvergenceVerdict(
        converged=not reasons,
        usable=usable,
        max_rhat=max_rhat,
        min_ess=min_ess,
        divergences=divergences,
        reasons=reasons,
    )


def assess_result(result: Dict[str, Any]) -> ConvergenceVerdict:
    """Assess a fit-results dict as written by `scripts/run_fit.py`."""
    return assess(
        max_rhat=result.get('max_rhat'),
        min_ess=result.get('min_ess'),
        divergences=result.get('divergences'),
    )
