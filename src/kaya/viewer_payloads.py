import json
from functools import lru_cache
from math import isnan
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from kaya.data_access import (
    KayaDataAccessor,
    boulder_grade_ticks,
    grade_to_num,
    normalize_climb_discipline,
    route_grade_ticks,
)
from kaya.s3_storage import has_s3_storage_config


VIEWER_ARTIFACTS_DIR = Path(__file__).resolve().parents[2] / 'data' / 'viewer_payloads' / 'latest'
ACTIVE_SEGMENT_RULES: Dict[str, float] = {
    'n_sends': 3.0,
    'n_sesh': 2.5,
    'n_sends_per_sesh': 2.5,
}

SEGMENT_DIMENSIONS: List[Dict[str, str]] = [
    {'key': 'n_sends', 'label': '# Sends'},
    {'key': 'n_sesh', 'label': '# Sessions'},
    {'key': 'n_sends_per_sesh', 'label': 'Sends / Session'},
]


class ViewerPayloadBuilder:
    """Build chart-ready payloads for the local Kaya viewer."""

    def __init__(self, accessor: Optional[KayaDataAccessor] = None) -> None:
        self.accessor = accessor or KayaDataAccessor()

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
        }

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
        sends_df = self.accessor.read_sends(
            source='local_db',
            columns=['user_id', 'gym_id', 'date', 'grade', 'climb_type'],
            order_by=False,
        )
        if sends_df.empty:
            return {'records': []}

        sends_df['discipline'] = sends_df['climb_type'].apply(normalize_climb_discipline)
        sends_df['grade_num'] = sends_df.apply(
            lambda row: grade_to_num(row.get('grade'), row.get('climb_type')),
            axis=1,
        )
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
            'body_metrics_bouldering': self.build_body_metrics('bouldering', active_only=True),
            'body_metrics_routes': self.build_body_metrics('routes', active_only=True),
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
            'body_metrics_routes': 'body-metrics-routes.json',
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