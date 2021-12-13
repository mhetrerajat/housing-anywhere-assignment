import os
from pathlib import Path
from typing import Generator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pypika import Query, Table

from etl.config import get_config
from etl.db import DBManager
from etl.utils import ETLStage, get_export_filename

__all__ = ["load"]


def _get_files_by_etl_stage(etl_stage: ETLStage) -> Generator[str, None, None]:
    """Returns absolute file path for all data chunks related to particular etl stage"""
    config = get_config()
    return Path(config.data_dir).glob(f"{etl_stage.name}__*.parquet")


def load(etl_stage: ETLStage) -> Generator[pd.DataFrame, None, None]:
    """Loads data chunks based on etl stage"""
    files = _get_files_by_etl_stage(etl_stage=etl_stage)
    for filepath in files:
        yield pd.read_parquet(filepath)


def export_as_file(data: pd.DataFrame, etl_stage: ETLStage, execution_id: str) -> str:
    """Export data in parquet format to intermediate data storage zone"""
    config = get_config()
    schema = get_data_schema(etl_stage)
    table = pa.Table.from_pandas(data, schema=schema)
    filename = get_export_filename(
        etl_stage=ETLStage.raw,
        execution_id=execution_id,
    )
    export_path = config.data_dir / f"{filename}.parquet"
    pq.write_table(table, where=export_path)
    return export_path


def get_data_schema(etl_stage: ETLStage) -> pa.schema:

    schema_base_fields = [
        ("event", pa.string()),
        ("time", pa.timestamp("ns")),
        ("unique_visitor_id", pa.string()),
        ("ha_user_id", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
    ]

    schema_store = {
        etl_stage.raw: schema_base_fields + [("country_code", pa.string())],
        etl_stage.preprocess: schema_base_fields
        + [("country", pa.string()), ("device_type", pa.string())],
    }
    schema = schema_store[etl_stage]
    return pa.schema(schema)


def export_to_db(data: pd.DataFrame):
    """Export data into database by performing bulk insert operation"""
    db_manager = DBManager()
    cursor = db_manager.get_cursor()

    data_cols = data.columns.tolist()

    events_table = Table("events")

    sql_statements = ["BEGIN TRANSACTION"]
    for _, row in data.iterrows():
        q = Query.into(events_table).columns(*data_cols).insert(row.tolist())
        sql_statements.append(q.get_sql())
    sql_statements.append("COMMIT;")

    with open("/tmp/query.txt", "w") as f:
        f.write(";/n".join(sql_statements))

    cursor.executescript(";\n".join(sql_statements))


def flush(etl_stage: ETLStage):
    """Delete all files of particular etl stage from intermediate data store"""
    files = _get_files_by_etl_stage(etl_stage=etl_stage)
    for filepath in files:
        os.remove(filepath)
