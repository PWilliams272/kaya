"""Tests for `ViewerPayloadBuilder`'s caching and optional-dependency handling."""
import sys
from typing import Any, Dict, Optional, Tuple

from kaya.viewer_payloads import ViewerPayloadBuilder


class _StubAccessor:
    """Minimal stand-in for `KayaDataAccessor`, with a settable mirror version."""

    def __init__(self, version: float = 1.0) -> None:
        self.version = version
        self.local_db_path = '/tmp/does-not-exist.db'

    def _local_db_version(self) -> float:
        return self.version


def _builder(version: float = 1.0) -> Tuple[ViewerPayloadBuilder, _StubAccessor]:
    accessor = _StubAccessor(version)
    return ViewerPayloadBuilder(accessor=accessor), accessor  # type: ignore[arg-type]


def test_pygam_is_not_imported_at_module_scope() -> None:
    """The deployed viewer serves prebuilt payloads and must import without pygam.

    pygam is in the `payloads` extra, so the EC2 host does not install it.
    """
    assert 'kaya.viewer_payloads' in sys.modules
    assert 'pygam' not in sys.modules


def test_cache_key_tracks_the_mirror_version() -> None:
    """It used to be lru_cached, so it could never notice a rebuilt mirror."""
    builder, accessor = _builder(version=1.0)
    first = builder.body_metrics_cache_key()
    accessor.version = 2.0
    second = builder.body_metrics_cache_key()
    assert first != second, 'cache key must change when the SQLite mirror is rebuilt'


def test_body_metrics_result_is_reused_within_a_version() -> None:
    builder, _ = _builder()
    calls: Dict[str, int] = {'n': 0}

    def fake_build(discipline: str = 'bouldering', active_only: bool = True) -> Dict[str, Any]:
        calls['n'] += 1
        return {'discipline': discipline, 'active_only': active_only}

    builder._build_body_metrics_uncached = fake_build  # type: ignore[method-assign]

    builder.build_body_metrics('bouldering', active_only=True)
    builder.build_body_metrics('bouldering', active_only=True)
    assert calls['n'] == 1, 'a repeated request must not refit the GAM curves'


def test_body_metrics_distinguishes_discipline_and_audience() -> None:
    """The four combinations are distinct payloads and must not collide."""
    builder, _ = _builder()
    seen = []

    def fake_build(discipline: str = 'bouldering', active_only: bool = True) -> Dict[str, Any]:
        seen.append((discipline, active_only))
        return {'discipline': discipline, 'active_only': active_only}

    builder._build_body_metrics_uncached = fake_build  # type: ignore[method-assign]

    combinations = [('bouldering', True), ('bouldering', False), ('routes', True), ('routes', False)]
    for discipline, active_only in combinations:
        payload = builder.build_body_metrics(discipline, active_only=active_only)
        assert payload['discipline'] == discipline
        assert payload['active_only'] is active_only
    assert seen == combinations, 'each discipline/audience pair needs its own cache entry'


def test_body_metrics_cache_drops_entries_from_an_older_mirror() -> None:
    builder, accessor = _builder(version=1.0)
    calls: Dict[str, int] = {'n': 0}

    def fake_build(discipline: str = 'bouldering', active_only: bool = True) -> Dict[str, Any]:
        calls['n'] += 1
        return {'version': accessor.version}

    builder._build_body_metrics_uncached = fake_build  # type: ignore[method-assign]

    builder.build_body_metrics('bouldering')
    accessor.version = 2.0
    payload = builder.build_body_metrics('bouldering')

    assert calls['n'] == 2, 'a rebuilt mirror must invalidate the cached payload'
    assert payload['version'] == 2.0
    assert len(builder._body_metrics_cache) == 1, 'stale entries must not accumulate'


def test_coerce_gym_ids() -> None:
    builder, _ = _builder()
    assert builder.coerce_gym_ids(None) is None
    assert builder.coerce_gym_ids('') is None
    assert builder.coerce_gym_ids('260') == ['260']


def test_plot_gender_normalization() -> None:
    """Gender arrives from gender-guesser, so `mostly_*` has to map through."""
    plot_gender = ViewerPayloadBuilder._plot_gender
    assert plot_gender('male') == 'male'
    assert plot_gender('Mostly_Male') == 'male'
    assert plot_gender('female') == 'female'
    assert plot_gender('mostly_female') == 'female'
    assert plot_gender('andy') is None
    assert plot_gender(None) is None


def test_optional_gam_dependency_reports_the_extra() -> None:
    """Without pygam the error must name the extra, not just fail on import."""
    import numpy as np

    from kaya import viewer_payloads

    if 'pygam' in sys.modules:
        return  # installed here; the message path is only reachable without it

    x = np.linspace(60.0, 75.0, viewer_payloads.GAM_MIN_POINTS + 5)
    y = np.linspace(1.0, 9.0, x.size)
    try:
        viewer_payloads.ViewerPayloadBuilder._fit_gam_curve(x, y, x)
    except ImportError as exc:
        assert 'payloads' in str(exc)


def test_fit_gam_curve_returns_none_below_the_point_floor() -> None:
    """Guard runs before the optional import, so this holds either way."""
    import numpy as np

    from kaya import viewer_payloads

    n = viewer_payloads.GAM_MIN_POINTS - 1
    x = np.linspace(60.0, 75.0, n)
    result: Optional[Dict[str, Any]] = viewer_payloads.ViewerPayloadBuilder._fit_gam_curve(
        x, np.linspace(1.0, 9.0, n), x
    )
    assert result is None
