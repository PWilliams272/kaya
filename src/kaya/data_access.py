import gzip
import json
import math
import os
import re
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import bindparam, inspect, text
from gender_guesser.detector import Detector

from kaya.db_manager import BASE_DIR, get_engine, write_dataframe
from kaya.s3_storage import (
    get_s3_bucket,
    get_s3_client,
    get_s3_prefix,
    has_s3_storage_config,
    list_recent_send_states,
)


load_dotenv(override=False)


SendSource = Literal['auto', 'local_db', 'aws_db', 's3_raw', 's3_backfill']


BOULDER_GRADE_TO_NUM: Dict[str, Optional[float]] = {
    **{f'v{i}': float(i) for i in range(18)},
    'vB': -1.0,
    'vIntro': -1.0,
    'v?': None,
}

BOULDER_GRADE_MAX = 17

_BOULDER_GRADE_LADDER: List[tuple] = [(-1.0, 'vB')] + [
    (float(i), f'v{i}') for i in range(0, BOULDER_GRADE_MAX + 1)
]


def _build_route_grade_ladder(max_major: int = 15) -> List[tuple]:
    ladder = [(float(major), f'5.{major}') for major in range(3, 10)]
    for major in range(10, max_major + 1):
        base = 10 + ((major - 10) * 4)
        for suffix, offset in (('a', 0.0), ('b', 1.0), ('c', 2.0), ('d', 3.0)):
            ladder.append((float(base) + offset, f'5.{major}{suffix}'))
    return ladder


_ROUTE_GRADE_LADDER: List[tuple] = _build_route_grade_ladder()


def boulder_grade_ticks(
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return the canonical vB..v17 tick ladder, optionally bounded to a value range."""
    lo = _BOULDER_GRADE_LADDER[0][0] if min_value is None else math.floor(min_value)
    hi = _BOULDER_GRADE_LADDER[-1][0] if max_value is None else math.ceil(max_value)
    return [
        {'value': value, 'label': label}
        for value, label in _BOULDER_GRADE_LADDER
        if lo <= value <= hi
    ]


def route_grade_ticks(
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return the canonical 5.3..5.15d tick ladder, optionally bounded to a value range."""
    lo = _ROUTE_GRADE_LADDER[0][0] if min_value is None else math.floor(min_value)
    hi = _ROUTE_GRADE_LADDER[-1][0] if max_value is None else math.ceil(max_value)
    return [
        {'value': value, 'label': label}
        for value, label in _ROUTE_GRADE_LADDER
        if lo <= value <= hi
    ]

ROUTE_SPECIAL_GRADE_TO_NUM: Dict[str, Optional[float]] = {
    '5.Intro': 4.0,
    '5.?': None,
    '5.Hard': None,
}

ROUTE_GRADE_PATTERN = re.compile(r'^5\.(\d+)([abcd+-]?)$')


def boulder_grade_num_to_str(grade: Optional[float]) -> Optional[str]:
    if grade is None or pd.isna(grade):
        return None
    if float(grade) == -1.0:
        return 'vB'
    return f"v{int(grade)}"


def normalize_climb_discipline(climb_type: Optional[str]) -> Optional[str]:
    if climb_type is None or pd.isna(climb_type):
        return None
    normalized = str(climb_type).strip().lower()
    if 'boulder' in normalized:
        return 'bouldering'
    if 'route' in normalized or 'rope' in normalized:
        return 'routes'
    return normalized


def route_grade_to_num(grade: Optional[str]) -> Optional[float]:
    if grade is None or pd.isna(grade):
        return None

    label = str(grade).strip()
    if label in ROUTE_SPECIAL_GRADE_TO_NUM:
        return ROUTE_SPECIAL_GRADE_TO_NUM[label]

    match = ROUTE_GRADE_PATTERN.match(label)
    if not match:
        return None

    major_grade = int(match.group(1))
    suffix = match.group(2)
    if major_grade < 10:
        return float(major_grade)

    base_value = 10 + ((major_grade - 10) * 4)
    suffix_offsets = {
        '-': 0.0,
        'a': 0.0,
        '': 0.5,
        'b': 1.0,
        '+': 1.5,
        'c': 2.0,
        'd': 3.0,
    }
    return float(base_value) + suffix_offsets.get(suffix, 0.0)


def grade_to_num(
    grade: Optional[str],
    climb_type: Optional[str] = None,
) -> Optional[float]:
    if grade is None or pd.isna(grade):
        return None

    discipline = normalize_climb_discipline(climb_type)
    if discipline == 'bouldering':
        return BOULDER_GRADE_TO_NUM.get(str(grade))
    if discipline == 'routes':
        return route_grade_to_num(str(grade))

    return BOULDER_GRADE_TO_NUM.get(str(grade), route_grade_to_num(str(grade)))


def inches_to_ft_inches(inches: float) -> Optional[str]:
    if pd.isna(inches):
        return None
    return f"{int(inches // 12)}' {int(inches % 12)}\""


@dataclass
class KayaDataAccessor:
    """Unified read access to Kaya data across S3 and database backends."""

    local_db_path: Path = BASE_DIR / 'kaya_data.db'
    _cached_user_profiles: Optional[pd.DataFrame] = field(
        default=None,
        init=False,
        repr=False,
    )
    _cached_user_profiles_version: Optional[float] = field(
        default=None,
        init=False,
        repr=False,
    )

    @cached_property
    def _gender_detector(self) -> Detector:
        return Detector(case_sensitive=False)

    def read_sends(
        self,
        source: SendSource = 'auto',
        gym_ids: Optional[Sequence[Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        run_dates: Optional[Sequence[str]] = None,
        run_ids: Optional[Sequence[str]] = None,
        max_objects: Optional[int] = None,
        export_name: str = 'rds-backfill',
        normalize: bool = True,
        parse_dates: bool = True,
        order_by: bool = True,
    ) -> pd.DataFrame:
        """Read sends from local DB, AWS DB, or S3 raw storage."""
        resolved_source = self._resolve_source(source)
        if resolved_source == 'local_db':
            df = self._read_db_sends(
                use_aws=False,
                gym_ids=gym_ids,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                columns=columns,
                order_by=order_by,
            )
        elif resolved_source == 'aws_db':
            df = self._read_db_sends(
                use_aws=True,
                gym_ids=gym_ids,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                columns=columns,
                order_by=order_by,
            )
        elif resolved_source == 's3_raw':
            df = self._read_s3_sends(
                gym_ids=gym_ids,
                run_dates=run_dates,
                run_ids=run_ids,
                max_objects=max_objects,
                columns=columns,
            )
        else:
            df = self._read_s3_sends(
                gym_ids=gym_ids,
                max_objects=max_objects,
                columns=columns,
                export_name=export_name,
            )

        if normalize:
            df = self.normalize_sends_frame(df, parse_dates=parse_dates)
        return df

    def read_state(
        self,
        gym_ids: Optional[Sequence[Any]] = None,
    ) -> pd.DataFrame:
        """Read per-gym incremental frontier state from S3."""
        normalized_gym_ids = self._normalize_gym_ids(gym_ids)
        states = list_recent_send_states(gym_ids=normalized_gym_ids)
        if not states:
            return pd.DataFrame()
        return pd.DataFrame(states)

    def summarize_sends(
        self,
        source: SendSource = 'local_db',
        gym_ids: Optional[Sequence[Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a compact summary for the current send corpus."""
        resolved_source = self._resolve_source(source)
        if resolved_source not in {'local_db', 'aws_db'}:
            sends_df = self.read_sends(
                source=resolved_source,
                gym_ids=gym_ids,
                start_date=start_date,
                end_date=end_date,
            )
            if sends_df.empty:
                return {
                    'total_sends': 0,
                    'unique_users': 0,
                    'unique_gyms': 0,
                    'first_date': None,
                    'last_date': None,
                }
            return {
                'total_sends': int(len(sends_df)),
                'unique_users': int(sends_df['user_id'].nunique()) if 'user_id' in sends_df.columns else 0,
                'unique_gyms': int(sends_df['gym_id'].nunique()) if 'gym_id' in sends_df.columns else 0,
                'first_date': sends_df['date'].min().isoformat() if 'date' in sends_df.columns and pd.notna(sends_df['date'].min()) else None,
                'last_date': sends_df['date'].max().isoformat() if 'date' in sends_df.columns and pd.notna(sends_df['date'].max()) else None,
            }

        use_aws = resolved_source == 'aws_db'
        table_name = self._table_name(use_aws=use_aws)
        where_sql, params = self._build_db_filters(
            gym_ids=gym_ids,
            start_date=start_date,
            end_date=end_date,
        )
        query = (
            'SELECT '
            'COUNT(*) AS total_sends, '
            'COUNT(DISTINCT user_id) AS unique_users, '
            'COUNT(DISTINCT gym_id) AS unique_gyms, '
            'MIN(date) AS first_date, '
            'MAX(date) AS last_date '
            f'FROM {table_name}'
        )
        if where_sql:
            query += f' WHERE {where_sql}'

        statement = text(query)
        if params.get('gym_ids'):
            statement = statement.bindparams(bindparam('gym_ids', expanding=True))

        summary_df = pd.read_sql_query(statement, get_engine(use_aws=use_aws), params=params)
        record = summary_df.iloc[0].to_dict()
        return {
            'total_sends': int(record['total_sends'] or 0),
            'unique_users': int(record['unique_users'] or 0),
            'unique_gyms': int(record['unique_gyms'] or 0),
            'first_date': record['first_date'],
            'last_date': record['last_date'],
        }

    def list_gyms(
        self,
        source: SendSource = 'local_db',
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """List gyms with send counts."""
        resolved_source = self._resolve_source(source)
        if resolved_source not in {'local_db', 'aws_db'}:
            sends_df = self.read_sends(source=resolved_source)
            if sends_df.empty:
                return pd.DataFrame(columns=['gym_id', 'gym_name', 'send_count'])
            gym_column = 'gym_name' if 'gym_name' in sends_df.columns else 'gym'
            grouped = (
                sends_df.groupby(['gym_id', gym_column], dropna=False)
                .size()
                .rename('send_count')
                .reset_index()
                .rename(columns={gym_column: 'gym_name'})
                .sort_values('send_count', ascending=False)
            )
            return grouped.head(limit) if limit else grouped

        use_aws = resolved_source == 'aws_db'
        send_columns = self._send_columns(use_aws=use_aws)
        gym_name_column = 'gym_name' if 'gym_name' in send_columns else 'gym' if 'gym' in send_columns else None
        gym_name_sql = gym_name_column if gym_name_column else 'CAST(gym_id AS TEXT)'
        query = (
            'SELECT '
            'CAST(gym_id AS TEXT) AS gym_id, '
            f'{gym_name_sql} AS gym_name, '
            'COUNT(*) AS send_count '
            f'FROM {self._table_name(use_aws=use_aws)} '
            'GROUP BY gym_id, gym_name '
            'ORDER BY send_count DESC'
        )
        if limit is not None:
            query += ' LIMIT :limit'
            params = {'limit': limit}
        else:
            params = None
        return pd.read_sql_query(text(query), get_engine(use_aws=use_aws), params=params)

    def sends_time_series(
        self,
        source: SendSource = 'local_db',
        gym_ids: Optional[Sequence[Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        freq: str = 'D',
    ) -> pd.DataFrame:
        """Aggregate sends over time."""
        sends_df = self.read_sends(
            source=source,
            gym_ids=gym_ids,
            start_date=start_date,
            end_date=end_date,
            columns=['date', 'send_id'],
            order_by=False,
        )
        if sends_df.empty:
            return pd.DataFrame(columns=['period', 'send_count'])

        freq_map = {'D': 'D', 'W': 'W', 'M': 'ME'}
        normalized_freq = freq_map.get(freq.upper(), 'D')
        grouped = (
            sends_df.dropna(subset=['date'])
            .set_index('date')
            .resample(normalized_freq)
            .size()
            .rename('send_count')
            .reset_index()
            .rename(columns={'date': 'period'})
        )
        grouped['period'] = grouped['period'].dt.strftime('%Y-%m-%d')
        return grouped

    def grade_distribution(
        self,
        source: SendSource = 'local_db',
        gym_ids: Optional[Sequence[Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        climb_type: Optional[str] = None,
        discipline: Optional[str] = None,
    ) -> pd.DataFrame:
        """Aggregate distinct climbs by grade — one count per unique climb_id,
        not one per logged send, so a climb that got sent 50 times doesn't
        outweigh 50 different climbs sent once each."""
        sends_df = self.read_sends(
            source=source,
            gym_ids=gym_ids,
            start_date=start_date,
            end_date=end_date,
            columns=['grade', 'climb_type', 'climb_id'],
            parse_dates=False,
            order_by=False,
        )
        if 'climb_type' in sends_df.columns:
            sends_df['discipline'] = sends_df['climb_type'].apply(
                normalize_climb_discipline
            )
        if climb_type and 'climb_type' in sends_df.columns:
            sends_df = sends_df[sends_df['climb_type'] == climb_type]
        if discipline:
            sends_df = sends_df[
                sends_df['discipline'] == normalize_climb_discipline(discipline)
            ]
        if sends_df.empty:
            return pd.DataFrame(
                columns=['grade', 'climb_count', 'grade_num', 'discipline']
            )

        grouped = (
            sends_df.groupby(['discipline', 'grade'], dropna=False)['climb_id']
            .nunique()
            .rename('climb_count')
            .reset_index()
        )
        grouped['grade_num'] = grouped.apply(
            lambda row: grade_to_num(
                row['grade'],
                row.get('discipline'),
            ),
            axis=1,
        )
        grouped = grouped.sort_values(
            ['discipline', 'grade_num', 'grade'],
            na_position='last',
        )
        return grouped

    def read_user_profiles(
        self,
        source: SendSource = 'local_db',
        refresh: bool = False,
    ) -> pd.DataFrame:
        """Build and cache a user-level profile table from send history."""
        resolved_source = self._resolve_source(source)
        if resolved_source == 'local_db':
            version = self._local_db_version()
            if (
                not refresh
                and self._cached_user_profiles is not None
                and self._cached_user_profiles_version == version
            ):
                return self._cached_user_profiles.copy()

            profiles = self._build_user_profiles(source='local_db')
            self._cached_user_profiles = profiles
            self._cached_user_profiles_version = version
            return profiles.copy()

        return self._build_user_profiles(source=resolved_source)

    def list_run_dates(
        self,
        export_name: Optional[str] = None,
        max_dates: Optional[int] = None,
    ) -> List[str]:
        """Return available run dates from the S3 raw object layout."""
        objects = self.list_raw_objects(max_objects=None, export_name=export_name)
        run_dates = sorted(
            {item['run_date'] for item in objects if item.get('run_date')},
            reverse=True,
        )
        if max_dates is not None:
            return run_dates[:max_dates]
        return run_dates

    def list_raw_objects(
        self,
        gym_ids: Optional[Sequence[Any]] = None,
        run_dates: Optional[Sequence[str]] = None,
        run_ids: Optional[Sequence[str]] = None,
        max_objects: Optional[int] = None,
        export_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List raw S3 objects that match the requested filters."""
        normalized_gym_ids = self._normalize_gym_ids(gym_ids)
        normalized_run_dates = set(run_dates or [])
        normalized_run_ids = set(run_ids or [])

        base_prefix = f"{get_s3_prefix()}/raw/sends/"
        if export_name:
            base_prefix = f"{base_prefix}source={export_name}/"

        client = get_s3_client()
        bucket = get_s3_bucket()
        continuation_token: Optional[str] = None
        matches: List[Dict[str, Any]] = []

        while True:
            request: Dict[str, Any] = {
                'Bucket': bucket,
                'Prefix': base_prefix,
            }
            if continuation_token is not None:
                request['ContinuationToken'] = continuation_token

            response = client.list_objects_v2(**request)
            for item in response.get('Contents', []):
                key = item['Key']
                if not key.endswith('.jsonl.gz'):
                    continue
                metadata = self._parse_raw_key(key)
                if normalized_gym_ids and metadata.get('gym_id') not in normalized_gym_ids:
                    continue
                if normalized_run_dates and metadata.get('run_date') not in normalized_run_dates:
                    continue
                if normalized_run_ids and metadata.get('run_id') not in normalized_run_ids:
                    continue
                matches.append(
                    {
                        'key': key,
                        'size': item['Size'],
                        **metadata,
                    }
                )
                if max_objects is not None and len(matches) >= max_objects:
                    return matches

            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

        return matches

    def sync_s3_sends_to_local_db(
        self,
        gym_ids: Optional[Sequence[Any]] = None,
        run_dates: Optional[Sequence[str]] = None,
        run_ids: Optional[Sequence[str]] = None,
        max_objects: Optional[int] = None,
        export_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upsert selected S3 raw sends into the local SQLite database."""
        objects = self.list_raw_objects(
            gym_ids=gym_ids,
            run_dates=run_dates,
            run_ids=run_ids,
            max_objects=max_objects,
            export_name=export_name,
        )

        rows_written = 0
        dropped_columns: set[str] = set()
        months_touched: set[str] = set()
        for item in objects:
            batch_df = self._read_s3_object(item['key'])
            if batch_df.empty:
                continue
            local_batch_df, removed_columns = self._align_batch_to_local_sends_schema(
                batch_df
            )
            dropped_columns.update(removed_columns)
            write_dataframe(
                local_batch_df,
                'sends',
                use_aws=False,
                if_exists='upsert',
            )
            rows_written += len(local_batch_df)
            if 'date' in local_batch_df.columns:
                # 'date' is stored/queried as an ISO 8601 string (see
                # _build_db_filters), so the first 7 characters are always
                # YYYY-MM without needing to parse it — used to know which
                # curated Parquet month-partitions need rebuilding.
                months_touched.update(
                    local_batch_df['date'].dropna().astype(str).str[:7].unique()
                )

        return {
            'objects_processed': len(objects),
            'rows_written': rows_written,
            'local_db_path': str(self.local_db_path),
            'dropped_columns': sorted(dropped_columns),
            'months_touched': sorted(months_touched),
        }

    def sync_latest_s3_to_local_db(
        self,
        latest_run_dates: int = 1,
        gym_ids: Optional[Sequence[Any]] = None,
        max_objects: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Sync one or more of the most recent live S3 run dates into local DB."""
        run_dates = self.list_run_dates(max_dates=latest_run_dates)
        if not run_dates:
            return {
                'run_dates': [],
                'objects_processed': 0,
                'rows_written': 0,
                'local_db_path': str(self.local_db_path),
            }
        result = self.sync_s3_sends_to_local_db(
            gym_ids=gym_ids,
            run_dates=run_dates,
            max_objects=max_objects,
        )
        result['run_dates'] = run_dates
        return result

    def write_curated_month_parquet(self, year_month: str) -> Dict[str, Any]:
        """Rebuild one curated Parquet month-partition from the local DB.

        Per DATA_STORAGE_NOTES.md's suggested layout
        (kaya/curated/sends/year=YYYY/month=MM/...), and reads the whole
        month fresh each time rather than appending — Parquet files aren't
        appendable in place, and a full-month reread is fast now that `date`
        is indexed (see write_dataframe's index-ensuring step).
        """
        year_str, month_str = year_month.split('-')
        year, month = int(year_str), int(month_str)
        start = f'{year:04d}-{month:02d}-01'
        end = f'{year + 1:04d}-01-01' if month == 12 else f'{year:04d}-{month + 1:02d}-01'

        month_df = pd.read_sql_query(
            text('SELECT * FROM sends WHERE date >= :start AND date < :end ORDER BY date'),
            get_engine(use_aws=False),
            params={'start': start, 'end': end},
        )

        local_path = Path(f'/tmp/curated_sends_{year_month}.parquet') if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ \
            else self.local_db_path.parent / f'curated_sends_{year_month}.parquet'
        local_path.parent.mkdir(parents=True, exist_ok=True)
        month_df.to_parquet(local_path, engine='pyarrow', index=False)

        key = f'{get_s3_prefix()}/curated/sends/year={year:04d}/month={month:02d}/data.parquet'
        get_s3_client().upload_file(str(local_path), get_s3_bucket(), key)
        local_path.unlink()

        return {'year_month': year_month, 'rows': len(month_df), 's3_key': key}

    def normalize_sends_frame(
        self,
        sends_df: pd.DataFrame,
        parse_dates: bool = True,
    ) -> pd.DataFrame:
        """Standardize common send columns across storage backends."""
        if sends_df.empty:
            return sends_df.copy()

        normalized = sends_df.copy()
        if 'gym_name' not in normalized.columns and 'gym' in normalized.columns:
            normalized['gym_name'] = normalized['gym']
        if 'gym' not in normalized.columns and 'gym_name' in normalized.columns:
            normalized['gym'] = normalized['gym_name']

        if 'date' in normalized.columns and parse_dates:
            normalized['date'] = pd.to_datetime(
                normalized['date'],
                utc=True,
                errors='coerce',
            )

        if 'grade' in normalized.columns and 'grade_num' not in normalized.columns:
            if 'climb_type' in normalized.columns:
                normalized['grade_num'] = normalized.apply(
                    lambda row: grade_to_num(
                        row.get('grade'),
                        row.get('climb_type'),
                    ),
                    axis=1,
                )
                normalized['discipline'] = normalized['climb_type'].apply(
                    normalize_climb_discipline
                )
            else:
                normalized['grade_num'] = normalized['grade'].apply(grade_to_num)

        return normalized

    def _resolve_source(self, source: SendSource) -> SendSource:
        if source != 'auto':
            return source

        if self._local_sends_table_exists():
            return 'local_db'
        if has_s3_storage_config():
            return 's3_raw'
        if os.getenv('AWS_DB_URL'):
            return 'aws_db'

        raise ValueError(
            'Could not resolve a default data source. Use an explicit source.'
        )

    def _local_sends_table_exists(self) -> bool:
        engine = get_engine(use_aws=False)
        inspector = inspect(engine)
        return inspector.has_table('sends')

    def _local_db_version(self) -> Optional[float]:
        if not self.local_db_path.exists():
            return None
        return self.local_db_path.stat().st_mtime

    def _align_batch_to_local_sends_schema(
        self,
        batch_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, List[str]]:
        if not self._local_sends_table_exists():
            return batch_df, []

        local_columns = self._send_columns(use_aws=False)
        compatible_columns = [
            column_name for column_name in batch_df.columns if column_name in local_columns
        ]
        removed_columns = [
            column_name for column_name in batch_df.columns if column_name not in local_columns
        ]
        return batch_df[compatible_columns].copy(), removed_columns

    def _send_columns(self, use_aws: bool) -> List[str]:
        inspector = inspect(get_engine(use_aws=use_aws))
        schema = os.getenv('AWS_DB_SCHEMA') if use_aws else None
        return [
            column['name']
            for column in inspector.get_columns('sends', schema=schema)
        ]

    def _read_db_sends(
        self,
        use_aws: bool,
        gym_ids: Optional[Sequence[Any]],
        start_date: Optional[str],
        end_date: Optional[str],
        limit: Optional[int],
        columns: Optional[Sequence[str]],
        order_by: bool,
    ) -> pd.DataFrame:
        engine = get_engine(use_aws=use_aws)
        available_columns = self._send_columns(use_aws=use_aws)
        statement, params = self._build_db_query(
            use_aws=use_aws,
            gym_ids=gym_ids,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            columns=columns,
            available_columns=available_columns,
            order_by=order_by,
        )
        return pd.read_sql_query(statement, engine, params=params)

    def _build_db_query(
        self,
        use_aws: bool,
        gym_ids: Optional[Sequence[Any]],
        start_date: Optional[str],
        end_date: Optional[str],
        limit: Optional[int],
        columns: Optional[Sequence[str]],
        available_columns: Optional[Sequence[str]] = None,
        order_by: bool = True,
    ) -> tuple[Any, Dict[str, Any]]:
        requested_columns = [
            column for column in (columns or []) if not available_columns or column in available_columns
        ]
        column_sql = ', '.join(requested_columns) if requested_columns else '*'
        table_name = self._table_name(use_aws=use_aws)
        where_sql, params = self._build_db_filters(
            gym_ids=gym_ids,
            start_date=start_date,
            end_date=end_date,
        )

        query = f'SELECT {column_sql} FROM {table_name}'
        if where_sql:
            query += ' WHERE ' + where_sql
        if order_by:
            query += ' ORDER BY date DESC, send_id DESC'
        if limit is not None:
            query += ' LIMIT :limit'
            params['limit'] = limit

        statement = text(query)
        if params.get('gym_ids'):
            statement = statement.bindparams(bindparam('gym_ids', expanding=True))
        return statement, params

    def _build_db_filters(
        self,
        gym_ids: Optional[Sequence[Any]],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> tuple[str, Dict[str, Any]]:
        where_clauses: List[str] = []
        params: Dict[str, Any] = {}
        normalized_gym_ids = self._normalize_gym_ids(gym_ids)

        if normalized_gym_ids:
            where_clauses.append('CAST(gym_id AS TEXT) IN :gym_ids')
            params['gym_ids'] = normalized_gym_ids
        if start_date:
            where_clauses.append('date >= :start_date')
            params['start_date'] = start_date
        if end_date:
            where_clauses.append('date <= :end_date')
            params['end_date'] = end_date

        return ' AND '.join(where_clauses), params

    def _read_s3_sends(
        self,
        gym_ids: Optional[Sequence[Any]],
        run_dates: Optional[Sequence[str]] = None,
        run_ids: Optional[Sequence[str]] = None,
        max_objects: Optional[int] = None,
        columns: Optional[Sequence[str]] = None,
        export_name: Optional[str] = None,
    ) -> pd.DataFrame:
        objects = self.list_raw_objects(
            gym_ids=gym_ids,
            run_dates=run_dates,
            run_ids=run_ids,
            max_objects=max_objects,
            export_name=export_name,
        )
        frames = [self._read_s3_object(item['key']) for item in objects]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return pd.DataFrame(columns=list(columns or []))

        df = pd.concat(frames, ignore_index=True)
        if columns:
            available_columns = [column for column in columns if column in df.columns]
            return df[available_columns]
        return df

    def _build_user_profiles(self, source: SendSource) -> pd.DataFrame:
        sends_df = self.read_sends(
            source=source,
            columns=[
                'user_id',
                'height',
                'ape_index',
                'first_name',
                'date',
                'send_id',
                'grade',
                'climb_type',
            ],
            order_by=False,
        )
        if sends_df.empty:
            return pd.DataFrame()

        # local_db enforces send_id as a primary key, so duplicates can't
        # reach this point via that source, but s3_raw/aws_db offer no such
        # guarantee (e.g. overlapping incremental partitions near the pull
        # frontier). Drop defensively regardless of source.
        sends_df = sends_df.drop_duplicates(subset='send_id')

        sends_df['date'] = pd.to_datetime(sends_df['date'], errors='coerce').dt.normalize()
        sends_df['discipline'] = sends_df['climb_type'].apply(
            normalize_climb_discipline
        )
        sends_df['grade_num'] = sends_df.apply(
            lambda row: grade_to_num(row.get('grade'), row.get('climb_type')),
            axis=1,
        )
        bouldering_mask = sends_df['climb_type'].fillna('').str.contains(
            'boulder',
            case=False,
        )
        routes_mask = sends_df['climb_type'].fillna('').str.contains(
            'route',
            case=False,
        )
        sends_df['boulder_grade_num'] = sends_df['grade_num'].where(bouldering_mask)
        sends_df['route_grade_num'] = sends_df['grade_num'].where(routes_mask)

        users_df = sends_df.groupby('user_id', as_index=False).agg(
            height=('height', 'first'),
            ape_index=('ape_index', 'first'),
            first_name=('first_name', 'first'),
            n_sends=('send_id', 'count'),
            n_dates=('date', 'nunique'),
            n_sesh=('date', 'nunique'),
            first_send=('date', 'min'),
            last_send=('date', 'max'),
            max_boulder_grade_num=('boulder_grade_num', 'max'),
            max_route_grade_num=('route_grade_num', 'max'),
        )

        max_boulder_labels = (
            sends_df[
                bouldering_mask & sends_df['boulder_grade_num'].notna()
            ]
            .sort_values(['user_id', 'boulder_grade_num', 'grade'])
            .groupby('user_id', as_index=False)
            .tail(1)[['user_id', 'grade']]
            .rename(columns={'grade': 'max_boulder_grade'})
        )
        max_route_labels = (
            sends_df[
                routes_mask & sends_df['route_grade_num'].notna()
            ]
            .sort_values(['user_id', 'route_grade_num', 'grade'])
            .groupby('user_id', as_index=False)
            .tail(1)[['user_id', 'grade']]
            .rename(columns={'grade': 'max_route_grade'})
        )

        users_df['gender'] = users_df['first_name'].fillna('').apply(
            self._gender_detector.get_gender
        )
        users_df = users_df.drop(columns=['first_name'])
        users_df = users_df.merge(max_boulder_labels, on='user_id', how='left')
        users_df = users_df.merge(max_route_labels, on='user_id', how='left')

        users_df['height'] = users_df['height'] / 2.54
        users_df['ape_index'] = users_df['ape_index'] / 2.54
        users_df['height_hr'] = users_df['height'].apply(inches_to_ft_inches)

        timeframe_days = (
            (users_df['last_send'] - users_df['first_send']).dt.days.fillna(0) + 1
        ).clip(lower=1)
        users_df['sends_per_date'] = users_df['n_sends'] / users_df['n_dates'].clip(lower=1)
        users_df['n_sends_per_sesh'] = users_df['n_sends'] / users_df['n_sesh'].clip(lower=1)
        users_df['sends_per_month'] = users_df['n_sends'] / timeframe_days / 30.0
        users_df['max_boulder_grade'] = users_df['max_boulder_grade'].combine_first(
            users_df['max_boulder_grade_num'].apply(boulder_grade_num_to_str)
        )
        return users_df

    def _read_s3_object(self, key: str) -> pd.DataFrame:
        response = get_s3_client().get_object(Bucket=get_s3_bucket(), Key=key)
        payload = gzip.decompress(response['Body'].read()).decode('utf-8')
        rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
        if not rows:
            return pd.DataFrame()

        metadata = self._parse_raw_key(key)
        df = pd.DataFrame(rows)
        for column_name, value in metadata.items():
            if column_name not in df.columns:
                df[column_name] = value
        return df

    def _parse_raw_key(self, key: str) -> Dict[str, str]:
        metadata: Dict[str, str] = {'s3_key': key}
        for part in key.split('/'):
            if '=' not in part:
                continue
            field_name, value = part.split('=', 1)
            metadata[field_name] = value
        if 'source' not in metadata:
            metadata['source'] = 'live'
        return metadata

    def _table_name(self, use_aws: bool) -> str:
        schema = os.getenv('AWS_DB_SCHEMA') if use_aws else None
        if schema:
            return f'{schema}.sends'
        return 'sends'

    def _normalize_gym_ids(
        self,
        gym_ids: Optional[Sequence[Any]],
    ) -> Optional[List[str]]:
        if not gym_ids:
            return None
        return [str(gym_id) for gym_id in gym_ids]