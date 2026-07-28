import json
from functools import lru_cache
from math import isnan
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from pygam import LinearGAM, s as gam_spline

from kaya.data_access import (
    BOULDER_GRADE_TO_NUM,
    KayaDataAccessor,
    boulder_grade_ticks,
    normalize_climb_discipline,
    route_grade_to_num,
    route_grade_ticks,
)
from kaya.s3_storage import get_s3_bucket, get_s3_client, get_s3_prefix, has_s3_storage_config

VIEWER_CACHE_S3_SUBPREFIX = 'viewer-cache'
VIEWER_ARTIFACTS_DIR = Path(__file__).resolve().parents[2] / 'data' / 'viewer_payloads' / 'latest'
ACTIVE_SEGMENT_RULES: Dict[str, float] = {
    'n_sends': 3.0,
    'n_sesh': 2.5,
    'n_sends_per_sesh': 2.5,
}
GAM_MIN_POINTS = 15
GAM_GRID_POINTS = 60
GAM_CI_WIDTH = 0.68  # matches the ~1-SD convention already used elsewhere in the viewer

SEGMENT_DIMENSIONS: List[Dict[str, str]] = [
    {'key': 'n_sends', 'label': '# Sends'},
    {'key': 'n_sesh', 'label': '# Sessions'},
    {'key': 'n_sends_per_sesh', 'label': 'Sends / Session'},
]


class ViewerPayloadBuilder:
    """Build chart-ready payloads for the local Kaya viewer."""

    def __init__(self, accessor: Optional[KayaDataAccessor] = None) -> None:
        self.accessor = accessor or KayaDataAccessor()
        self._cached_gym_comparison_base: Optional[Dict[str, Any]] = None
        self._cached_gym_comparison_base_version: Optional[float] = None

    def coerce_gym_ids(self, gym_id: Optional[str]) -> Optional[List[str]]:
        if not gym_id:
            return None
        return [gym_id]

    @staticmethod
    def _segment_users(users_df: pd.DataFrame) -> pd.DataFrame:
        segmented = users_df.copy()
        segmented['segment'] = segmented.apply(
            lambda row: (
                'Active'
                if all((row.get(metric) or 0) >= threshold for metric, threshold in ACTIVE_SEGMENT_RULES.items())
                else 'Inactive'
            ),
            axis=1,
        )
        return segmented

    @staticmethod
    def _plot_gender(gender: Any) -> Optional[str]:
        if gender is None or pd.isna(gender):
            return None
        normalized = str(gender).strip().lower()
        if normalized in {'male', 'mostly_male'}:
            return 'male'
        if normalized in {'female', 'mostly_female'}:
            return 'female'
        return None

    @lru_cache(maxsize=1)
    def body_metrics_cache_key(self) -> tuple[Optional[float], str]:
        return self.accessor._local_db_version(), str(self.accessor.local_db_path)

    def build_summary(
        self,
        gym_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        summary = self.accessor.summarize_sends(
            source='local_db',
            gym_ids=self.coerce_gym_ids(gym_id),
            start_date=start_date,
            end_date=end_date,
        )
        summary['local_db_path'] = str(self.accessor.local_db_path)
        summary['state_count'] = (
            int(len(self.accessor.read_state())) if has_s3_storage_config() else 0
        )
        return summary

    def build_gyms(self) -> List[Dict[str, Any]]:
        gyms_df = self.accessor.list_gyms(source='local_db')
        return gyms_df.to_dict(orient='records')

    def build_time_series(
        self,
        gym_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = 'D',
    ) -> List[Dict[str, Any]]:
        series_df = self.accessor.sends_time_series(
            source='local_db',
            gym_ids=self.coerce_gym_ids(gym_id),
            start_date=start_date,
            end_date=end_date,
            freq=freq,
        )
        return series_df.to_dict(orient='records')

    def build_grade_distribution(
        self,
        discipline: str,
        gym_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        grades_df = self.accessor.grade_distribution(
            source='local_db',
            gym_ids=self.coerce_gym_ids(gym_id),
            start_date=start_date,
            end_date=end_date,
            discipline=discipline,
        )
        return grades_df.to_dict(orient='records')

    def build_top_gyms(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        gyms_df = self.accessor.list_gyms(source='local_db', limit=limit)
        return gyms_df.to_dict(orient='records')

    def build_state_preview(self, limit: int = 20) -> List[Dict[str, Any]]:
        if not has_s3_storage_config():
            return []
        states_df = self.accessor.read_state()
        if states_df.empty:
            return []
        if 'last_successful_run_at' in states_df.columns:
            states_df = states_df.sort_values('last_successful_run_at', ascending=False)
        return states_df.head(limit).to_dict(orient='records')

    def build_body_metrics(
        self,
        discipline: str = 'bouldering',
        active_only: bool = True,
    ) -> Dict[str, Any]:
        self.body_metrics_cache_key()
        users_df = self._segment_users(self.accessor.read_user_profiles(source='local_db'))
        normalized_discipline = normalize_climb_discipline(discipline) or 'bouldering'
        grade_num_column = (
            'max_route_grade_num' if normalized_discipline == 'routes' else 'max_boulder_grade_num'
        )
        grade_label_column = (
            'max_route_grade' if normalized_discipline == 'routes' else 'max_boulder_grade'
        )
        if users_df.empty:
            return {
                'discipline': normalized_discipline,
                'active_only': active_only,
                'height_histogram': [],
                'ape_index_histogram': [],
                'height_vs_grade': [],
                'ape_vs_grade': [],
                'grade_ticks': [],
            }

        clipped_df = users_df.copy()
        clipped_df.loc[
            (clipped_df['height'] < 52) | (clipped_df['height'] > 80),
            'height',
        ] = pd.NA
        clipped_df.loc[
            (clipped_df['ape_index'] < -10) | (clipped_df['ape_index'] > 10),
            'ape_index',
        ] = pd.NA

        filtered_df = clipped_df.copy()
        if active_only:
            filtered_df = filtered_df[filtered_df['segment'] == 'Active'].copy()
        histogram_df = clipped_df

        if filtered_df.empty:
            return {
                'discipline': normalized_discipline,
                'active_only': active_only,
                'height_histogram': self._build_morphology_histograms(histogram_df, 'height'),
                'ape_index_histogram': self._build_morphology_histograms(histogram_df, 'ape_index'),
                'height_vs_grade_by_gender': {'male': [], 'female': []},
                'ape_vs_grade_by_gender': {'male': [], 'female': []},
                'grade_ticks': [],
                'segment_rules': ACTIVE_SEGMENT_RULES,
            }

        filtered_df['plot_gender'] = filtered_df['gender'].apply(self._plot_gender)
        histogram_df['plot_gender'] = histogram_df['gender'].apply(self._plot_gender)

        active_df = filtered_df.copy()
        active_df['height_rounded'] = active_df['height'].round()
        active_df['ape_index_rounded'] = active_df['ape_index'].round()
        active_df = active_df[active_df[grade_num_column].notna()]

        def grouped_points(axis_column: str) -> Dict[str, List[Dict[str, Any]]]:
            grouped = (
                active_df.dropna(subset=[axis_column, grade_num_column, 'plot_gender'])
                .groupby([axis_column, grade_num_column, grade_label_column, 'plot_gender'], as_index=False)
                .agg(n_users=('user_id', 'count'))
                .sort_values([grade_num_column, axis_column])
            )
            return {
                'male': grouped[grouped['plot_gender'] == 'male'].to_dict(orient='records'),
                'female': grouped[grouped['plot_gender'] == 'female'].to_dict(orient='records'),
            }

        return {
            'discipline': normalized_discipline,
            'active_only': active_only,
            'height_histogram': self._build_morphology_histograms(histogram_df, 'height'),
            'ape_index_histogram': self._build_morphology_histograms(histogram_df, 'ape_index'),
            'height_vs_grade_by_gender': grouped_points('height_rounded'),
            'ape_vs_grade_by_gender': grouped_points('ape_index_rounded'),
            'grade_num_column': grade_num_column,
            'grade_label_column': grade_label_column,
            'grade_ticks': self._build_grade_ticks(active_df[grade_num_column], normalized_discipline),
            'segment_rules': ACTIVE_SEGMENT_RULES,
            'gam_curves': {
                'height': self._build_gam_curves(active_df, 'height', grade_num_column, 52.0, 80.0),
                'ape_index': self._build_gam_curves(active_df, 'ape_index', grade_num_column, -10.0, 10.0),
            },
        }

    @staticmethod
    def _fit_gam_curve(
        x_values: np.ndarray,
        y_values: np.ndarray,
        x_grid: np.ndarray,
    ) -> Optional[Dict[str, List[float]]]:
        """Fit grade ~ s(x) with pygam and predict a mean curve + 68% CI on a grid.

        Deliberately fit on raw (unrounded) per-user x/y pairs, not the
        rounded buckets used for the scatter/heatmap, and deliberately a
        smooth spline term rather than a straight line or fixed-degree
        polynomial — the whole point is to let a non-monotonic shape (e.g.
        height helping up to a point, then hurting) emerge from the data
        instead of assuming one.

        Plain .fit() uses a fixed, fairly permissive smoothing penalty
        (lam=0.6) with no cross-validation, which lets the curve chase
        individual points in sparse regions — e.g. route ape-index has zero
        observations between -10 and -4, and an under-penalized fit swung
        to wildly implausible predictions there (grade -25, then +24) just
        from a couple of points near the edge of support. gridsearch over a
        higher lam floor picks a properly cross-validated penalty instead
        of a fixed guess, and fewer spline knots (10 vs 15) further limits
        how much local wiggle the model can express in the first place —
        together these keep a real signal (e.g. the height sweet-spot
        curve) while no longer letting one or two sparse/unusual points
        swing the whole shape.
        """
        if len(x_values) < GAM_MIN_POINTS:
            return None
        gam = LinearGAM(gam_spline(0, n_splines=10)).gridsearch(
            x_values.reshape(-1, 1),
            y_values,
            lam=np.logspace(0, 5, 15),
            progress=False,
        )
        grid = x_grid.reshape(-1, 1)
        mean = gam.predict(grid)
        lower, upper = gam.confidence_intervals(grid, width=GAM_CI_WIDTH).T
        return {
            'x': x_grid.tolist(),
            'mean': mean.tolist(),
            'lower': lower.tolist(),
            'upper': upper.tolist(),
        }

    def _build_gam_curves(
        self,
        active_df: pd.DataFrame,
        x_column: str,
        grade_num_column: str,
        x_min: float,
        x_max: float,
    ) -> Dict[str, Optional[Dict[str, List[float]]]]:
        x_grid = np.linspace(x_min, x_max, GAM_GRID_POINTS)
        curves: Dict[str, Optional[Dict[str, List[float]]]] = {}
        for gender in ('male', 'female'):
            subset = active_df[
                (active_df['plot_gender'] == gender)
                & active_df[x_column].notna()
                & active_df[grade_num_column].notna()
            ]
            curves[gender] = self._fit_gam_curve(
                subset[x_column].to_numpy(dtype=float),
                subset[grade_num_column].to_numpy(dtype=float),
                x_grid,
            )
        return curves

    @staticmethod
    def _build_morphology_histograms(users_df: pd.DataFrame, metric: str) -> List[Dict[str, Any]]:
        histograms: List[Dict[str, Any]] = []
        for gender, label in [('all', 'All Users'), ('male', 'Male'), ('female', 'Female')]:
            subset = users_df if gender == 'all' else users_df[users_df['plot_gender'] == gender]
            histograms.append(
                {
                    'series': label,
                    'values': subset[metric].dropna().round(2).tolist(),
                }
            )
        return histograms

    def build_user_segmentation(self, sample_size: Optional[int] = None) -> Dict[str, Any]:
        users_df = self._segment_users(self.accessor.read_user_profiles(source='local_db'))
        if users_df.empty:
            return {
                'criteria': ACTIVE_SEGMENT_RULES,
                'criteria_text': 'Active if n_sends >= 3, n_sesh >= 2.5, and n_sends_per_sesh >= 2.5.',
                'segment_counts': [],
                'height_histogram': [],
                'ape_index_histogram': [],
                'corner_dimensions': SEGMENT_DIMENSIONS,
                'corner_thresholds': ACTIVE_SEGMENT_RULES,
                'corner_points': [],
            }

        segment_counts = (
            users_df.groupby('segment', as_index=False)
            .agg(user_count=('user_id', 'count'))
            .sort_values('segment')
        )

        filtered_df = users_df.copy()
        filtered_df.loc[
            (filtered_df['height'] < 52) | (filtered_df['height'] > 80),
            'height',
        ] = pd.NA
        filtered_df.loc[
            (filtered_df['ape_index'] < -10) | (filtered_df['ape_index'] > 10),
            'ape_index',
        ] = pd.NA

        filtered_df['plot_gender'] = filtered_df['gender'].apply(self._plot_gender)

        height_histogram = self._build_morphology_histograms(filtered_df, 'height')
        ape_index_histogram = self._build_morphology_histograms(filtered_df, 'ape_index')

        corner_metric_keys = [item['key'] for item in SEGMENT_DIMENSIONS]
        corner_columns = corner_metric_keys + ['segment']
        corner_df = users_df[corner_columns].dropna()
        for metric in corner_metric_keys:
            corner_df = corner_df[corner_df[metric] > 0]
        if sample_size is not None and len(corner_df) > sample_size:
            corner_df = corner_df.sample(sample_size, random_state=42)

        corner_df = corner_df.copy()
        for metric in corner_metric_keys:
            corner_df[f'{metric}_log10'] = corner_df[metric].apply(lambda value: None if pd.isna(value) or value <= 0 else float(__import__('math').log10(value)))

        return {
            'criteria': ACTIVE_SEGMENT_RULES,
            'criteria_text': 'Active if n_sends >= 3, n_sesh >= 2.5, and n_sends_per_sesh >= 2.5.',
            'segment_counts': segment_counts.to_dict(orient='records'),
            'height_histogram': height_histogram,
            'ape_index_histogram': ape_index_histogram,
            'corner_dimensions': SEGMENT_DIMENSIONS,
            'corner_thresholds': {
                key: float(value)
                for key, value in ACTIVE_SEGMENT_RULES.items()
            },
            'corner_points': corner_df.to_dict(orient='records'),
            'sample_size': int(len(corner_df)),
        }

    def build_gym_comparison_base(self) -> Dict[str, Any]:
        """Build the full per-user/gym/discipline max-grade table used by the
        gym comparison tab's client-side filtering.

        Cached at the instance level, matching KayaDataAccessor.read_user_profiles
        — this result doesn't depend on which reference/comparison gyms the
        user has selected (that filtering happens client-side against the
        full record set already in the browser), so there's nothing "live"
        about recomputing it from 1.8M+ raw send rows on every request. It
        was previously uncached, which combined with a row-by-row
        DataFrame.apply() over the full sends table made every gym
        comparison load take ~20s regardless of how many times the
        underlying data had actually changed.
        """
        version = self.accessor._local_db_version()
        if (
            self._cached_gym_comparison_base is not None
            and self._cached_gym_comparison_base_version == version
        ):
            return self._cached_gym_comparison_base

        result = self._build_gym_comparison_base()
        self._cached_gym_comparison_base = result
        self._cached_gym_comparison_base_version = version
        return result

    def _build_gym_comparison_base(self) -> Dict[str, Any]:
        sends_df = self.accessor.read_sends(
            source='local_db',
            columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type'],
            order_by=False,
        )
        if sends_df.empty:
            return {'records': []}

        # Vectorized discipline classification + grade lookup instead of a
        # row-by-row .apply(axis=1) — over ~1.8M send rows that apply() was
        # the dominant cost (tens of seconds). Bouldering is a plain dict
        # lookup (fully vectorizable via .map()); only the much smaller
        # routes subset needs the regex-based route_grade_to_num, applied to
        # just that subset rather than the whole table.
        climb_type_lower = sends_df['climb_type'].fillna('').astype(str).str.lower()
        is_boulder = climb_type_lower.str.contains('boulder')
        is_route = climb_type_lower.str.contains('route') | climb_type_lower.str.contains('rope')
        sends_df['discipline'] = np.select(
            [is_boulder, is_route],
            ['bouldering', 'routes'],
            default=climb_type_lower,
        )

        grade_num = pd.Series(np.nan, index=sends_df.index, dtype=float)
        grade_num.loc[is_boulder] = sends_df.loc[is_boulder, 'grade'].map(BOULDER_GRADE_TO_NUM)
        grade_num.loc[is_route] = sends_df.loc[is_route, 'grade'].apply(route_grade_to_num)
        sends_df['grade_num'] = grade_num

        sends_df = sends_df[
            sends_df['discipline'].isin(['bouldering', 'routes'])
            & sends_df['grade_num'].notna()
            & sends_df['user_id'].notna()
            & sends_df['gym_id'].notna()
        ].copy()
        if sends_df.empty:
            return {'records': []}

        sends_df['gym_id'] = sends_df['gym_id'].astype(str)
        group_columns = ['user_id', 'gym_id', 'discipline']
        grouped = sends_df.groupby(group_columns, as_index=False).agg(
            n_days=('date', 'nunique'),
        )
        max_grade_labels = (
            sends_df.sort_values(group_columns + ['grade_num', 'grade'])
            .groupby(group_columns, as_index=False)
            .tail(1)[group_columns + ['grade', 'grade_num']]
            .rename(columns={'grade': 'max_grade_label', 'grade_num': 'max_grade_num'})
        )
        comparison_df = grouped.merge(max_grade_labels, on=group_columns, how='left')
        return {
            'records': comparison_df.to_dict(orient='records'),
        }

    def build_static_artifacts(self) -> Dict[str, Any]:
        gyms = self.build_gyms()
        gym_ids = [str(gym['gym_id']) for gym in gyms if gym.get('gym_id') is not None]
        time_grains = ['D', 'W', 'M']
        disciplines = ['bouldering', 'routes']

        return {
            'manifest': {
                'mode': 'static',
                'supports': {
                    'time_grains': time_grains,
                    'disciplines': disciplines,
                    'gym_filter': True,
                    'date_filter': False,
                },
            },
            'summary': self.build_summary(),
            'gyms': gyms,
            'top_gyms': self.build_top_gyms(limit=None),
            'state_preview': self.build_state_preview(),
            'time_series_daily': self.build_time_series(freq='D'),
            'time_series_weekly': self.build_time_series(freq='W'),
            'time_series_monthly': self.build_time_series(freq='M'),
            'grade_distribution_bouldering': self.build_grade_distribution('bouldering'),
            'grade_distribution_routes': self.build_grade_distribution('routes'),
            # Two variants each so the Audience toggle (active vs all users)
            # works fully from precomputed data — previously only
            # active_only=True was precomputed, so switching to "All users"
            # silently fell back to a live (uncached) computation.
            'body_metrics_bouldering': self.build_body_metrics('bouldering', active_only=True),
            'body_metrics_bouldering_all': self.build_body_metrics('bouldering', active_only=False),
            'body_metrics_routes': self.build_body_metrics('routes', active_only=True),
            'body_metrics_routes_all': self.build_body_metrics('routes', active_only=False),
            'user_segmentation': self.build_user_segmentation(),
            'gym_comparison_base': self.build_gym_comparison_base(),
            'time_series_by_gym': {
                freq: {
                    gym_id: self.build_time_series(gym_id=gym_id, freq=freq)
                    for gym_id in gym_ids
                }
                for freq in time_grains
            },
            'grade_distribution_by_gym': {
                discipline: {
                    gym_id: self.build_grade_distribution(discipline, gym_id=gym_id)
                    for gym_id in gym_ids
                }
                for discipline in disciplines
            },
        }

    def write_static_artifacts(
        self,
        output_dir: Path = VIEWER_ARTIFACTS_DIR,
    ) -> Dict[str, Any]:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifacts = self.build_static_artifacts()
        file_map = {
            'manifest': 'manifest.json',
            'summary': 'summary.json',
            'gyms': 'gyms.json',
            'top_gyms': 'top-gyms.json',
            'state_preview': 'state-preview.json',
            'time_series_daily': 'time-series-daily.json',
            'time_series_weekly': 'time-series-weekly.json',
            'time_series_monthly': 'time-series-monthly.json',
            'grade_distribution_bouldering': 'grade-distribution-bouldering.json',
            'grade_distribution_routes': 'grade-distribution-routes.json',
            'body_metrics_bouldering': 'body-metrics-bouldering.json',
            'body_metrics_bouldering_all': 'body-metrics-bouldering-all.json',
            'body_metrics_routes': 'body-metrics-routes.json',
            'body_metrics_routes_all': 'body-metrics-routes-all.json',
            'user_segmentation': 'user-segmentation.json',
            'gym_comparison_base': 'gym-comparison-base.json',
        }
        for key, file_name in file_map.items():
            payload = self._sanitize_for_json(artifacts[key])
            (output_dir / file_name).write_text(
                json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
                encoding='utf-8',
            )

        for freq, by_gym in artifacts['time_series_by_gym'].items():
            freq_dir = output_dir / 'time-series' / freq
            freq_dir.mkdir(parents=True, exist_ok=True)
            for gym_id, payload in by_gym.items():
                (freq_dir / f'{gym_id}.json').write_text(
                    json.dumps(self._sanitize_for_json(payload), indent=2, sort_keys=True, allow_nan=False),
                    encoding='utf-8',
                )

        for discipline, by_gym in artifacts['grade_distribution_by_gym'].items():
            discipline_dir = output_dir / 'grade-distribution' / discipline
            discipline_dir.mkdir(parents=True, exist_ok=True)
            for gym_id, payload in by_gym.items():
                (discipline_dir / f'{gym_id}.json').write_text(
                    json.dumps(self._sanitize_for_json(payload), indent=2, sort_keys=True, allow_nan=False),
                    encoding='utf-8',
                )

        return {
            'output_dir': str(output_dir),
            'files_written': sorted(file_map.values()),
            'gym_artifact_count': len(artifacts['gyms']),
        }

    def upload_static_artifacts_to_s3(
        self,
        output_dir: Path = VIEWER_ARTIFACTS_DIR,
        s3_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload a previously written static-artifacts directory to S3.

        Walks `output_dir` for JSON files (including the nested
        time-series/grade-distribution per-gym files) and uploads each one
        under `s3_prefix`, preserving the local relative path as the S3 key
        suffix so the layout on S3 mirrors the local `viewer_payloads/latest`
        directory.
        """
        if not has_s3_storage_config():
            raise RuntimeError('KAYA_S3_BUCKET is not set; cannot upload viewer artifacts to S3.')
        if not output_dir.exists():
            raise FileNotFoundError(f'{output_dir} does not exist; run write_static_artifacts() first.')

        bucket = get_s3_bucket()
        prefix = s3_prefix if s3_prefix is not None else f'{get_s3_prefix()}/{VIEWER_CACHE_S3_SUBPREFIX}'
        client = get_s3_client()

        uploaded_keys = []
        for file_path in sorted(output_dir.rglob('*.json')):
            relative_key = file_path.relative_to(output_dir).as_posix()
            key = f'{prefix}/{relative_key}'
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=file_path.read_bytes(),
                ContentType='application/json',
            )
            uploaded_keys.append(key)

        return {
            'bucket': bucket,
            'prefix': prefix,
            'files_uploaded': len(uploaded_keys),
            'keys': uploaded_keys,
        }

    @staticmethod
    def _build_grade_ticks(
        grade_num_values: pd.Series,
        discipline: str,
    ) -> List[Dict[str, Any]]:
        values = grade_num_values.dropna()
        if values.empty:
            return []
        min_value = float(values.min())
        max_value = float(values.max())
        if discipline == 'routes':
            return route_grade_ticks(min_value, max_value)
        return boulder_grade_ticks(min_value, max_value)

    @staticmethod
    def _sanitize_for_json(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: ViewerPayloadBuilder._sanitize_for_json(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [ViewerPayloadBuilder._sanitize_for_json(item) for item in value]
        if value is pd.NA:
            return None
        if isinstance(value, float) and isnan(value):
            return None
        if pd.isna(value) and not isinstance(value, (str, bytes)):
            return None
        return value