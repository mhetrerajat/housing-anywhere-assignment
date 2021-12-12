from pathlib import Path
from typing import Generator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from etl.config import get_config
from etl.utils import ETLStage, get_export_filename

__all__ = ["load"]


def _get_files_by_etl_stage(etl_stage: ETLStage) -> Generator[str, None, None]:
    """Returns absolute file path for all data chunks related to particular etl stage"""
    config = get_config()
    return Path(config.data_dir).glob(f"{etl_stage.name}__*")


def load(etl_stage: ETLStage) -> Generator[pd.DataFrame, None, None]:
    """Loads data chunks based on etl stage"""
    files = _get_files_by_etl_stage(etl_stage=etl_stage)
    for filepath in files:
        yield pd.read_csv(filepath)


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
