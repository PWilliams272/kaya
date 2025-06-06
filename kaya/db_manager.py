import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from sqlalchemy import Table, MetaData, inspect, text, Column, String
from sqlalchemy.types import Boolean, Integer, Float
import logging

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DB_URL_DEFAULT = f"sqlite:///{os.path.join(BASE_DIR, 'data', 'kaya_data.db')}"

LOCAL_DB_URL = os.getenv('LOCAL_DB_URL', LOCAL_DB_URL_DEFAULT)
AWS_DB_URL = os.getenv('AWS_DB_URL')

logger = logging.getLogger(__name__)

def get_engine(use_aws=False) -> Engine:
    """
    Returns a SQLAlchemy engine for the local or AWS database.
    """
    from sqlalchemy import text  # Ensure text is imported
    db_url = AWS_DB_URL if use_aws else LOCAL_DB_URL
    if db_url is None:
        raise ValueError("Database URL not set. Please set LOCAL_DB_URL or AWS_DB_URL in your .env file.")
    engine = create_engine(db_url)
    if use_aws:
        schema = os.getenv('AWS_DB_SCHEMA', 'public')
        # Set the PostgreSQL search_path to the desired schema
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {schema}"))
    return engine

def write_dataframe(df: pd.DataFrame, table_name: str, use_aws: bool = False, if_exists: str = 'append'):
    """
    Write a DataFrame to the specified table in the selected database.
    - Always creates the table if it doesn't exist, with send_id as unique.
    - Upserts on send_id for both SQLite and Postgres.
    """
    engine = get_engine(use_aws=use_aws)
    schema = os.getenv('AWS_DB_SCHEMA') if use_aws else None

    # Ensure boolean columns are correct dtype for DB
    for col in ['is_private', 'is_premium']:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    metadata = MetaData()

    with engine.begin() as conn:
        inspector = inspect(conn)
        logger.debug(f"Checking if table '{table_name}' exists in schema '{schema}'...")
        logger.debug(f"Tables found: {inspector.get_table_names(schema=schema)}")
        if table_name not in inspector.get_table_names(schema=schema):
            logger.info(f"Table '{table_name}' does not exist. Creating...")
            # Build columns from DataFrame dtypes
            columns = []
            for col in df.columns:
                if col == 'send_id':
                    columns.append(Column('send_id', String, primary_key=True))
                elif df[col].dtype in [int, 'int64', 'int32']:
                    columns.append(Column(col, Integer))
                elif df[col].dtype in [float, 'float64', 'float32']:
                    columns.append(Column(col, Float))
                elif df[col].dtype in [bool, 'bool']:
                    columns.append(Column(col, Boolean))
                else:
                    columns.append(Column(col, String))
            table = Table(table_name, metadata, *columns, schema=schema)
            metadata.create_all(bind=conn)
            logger.info(f"Table '{table_name}' created.")
        else:
            logger.debug(f"Table '{table_name}' already exists.")
            table = Table(table_name, metadata, autoload_with=conn, schema=schema)

        # Upsert logic
        records = df.to_dict(orient='records')
        logger.debug(f"Beginning upsert/insert for {len(records)} records into '{table_name}'...")
        if if_exists == 'replace':
            # Drop and recreate the table with PRIMARY KEY constraint
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            # Build columns from DataFrame dtypes
            columns = []
            for col in df.columns:
                if col == 'send_id':
                    columns.append(Column('send_id', String, primary_key=True))
                elif df[col].dtype in [int, 'int64', 'int32']:
                    columns.append(Column(col, Integer))
                elif df[col].dtype in [float, 'float64', 'float32']:
                    columns.append(Column(col, Float))
                elif df[col].dtype in [bool, 'bool']:
                    columns.append(Column(col, Boolean))
                else:
                    columns.append(Column(col, String))
            new_table = Table(table_name, MetaData(), *columns, schema=schema)
            new_table.create(bind=conn)
            # Insert all records
            conn.execute(new_table.insert(), records)
        elif if_exists == 'upsert':
            if use_aws:
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                for row in records:
                    stmt = pg_insert(table).values(**row)
                    update_dict = {c: stmt.excluded[c] for c in df.columns if c != 'send_id'}
                    stmt = stmt.on_conflict_do_update(index_elements=['send_id'], set_=update_dict)
                    conn.execute(stmt)
            else:
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert
                for row in records:
                    stmt = sqlite_insert(table).values(**row)
                    update_dict = {c: row[c] for c in df.columns if c != 'send_id'}
                    stmt = stmt.on_conflict_do_update(index_elements=['send_id'], set_=update_dict)
                    conn.execute(stmt)
        else:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False, schema=schema)

def read_table(table_name: str, use_aws: bool = False) -> pd.DataFrame:
    """
    Read a table from the selected database into a DataFrame.
    Args:
        table_name: Table name
        use_aws: If True, read from AWS RDS; else, read from local DB
    Returns:
        DataFrame
    """
    engine = get_engine(use_aws=use_aws)
    schema = os.getenv('AWS_DB_SCHEMA') if use_aws else None
    return pd.read_sql_table(table_name, engine, schema=schema)