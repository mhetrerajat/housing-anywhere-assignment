from pathlib import Path
from typing import Generator

import pandas as pd

from etl.config import get_config
from etl.utils import ETLStage

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
