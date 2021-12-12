import glob
from pathlib import Path
from typing import Generator

from config import get_config
from utils import ETLStage


def get_files_by_etl_stage(etl_stage: ETLStage) -> Generator[str]:
    """Returns absolute file path for all data chunks related to particular etl stage"""
    config = get_config()
    return Path(config.data_dir).glob(f"{etl_stage.name}__*")
