import dataclasses
import os
import re
from pathlib import Path

__all__ = ["get_config"]


@dataclasses.dataclass
class Config:
    events_api_host: str = "127.0.0.1"
    events_api_port: str = "5000"

    data_dir: Path = Path("/tmp/housinganywhere_data/")

    events_timeperiod_date_format = "%Y-%m-%d %H:%M:%S"

    ha_user_id_regex = re.compile("(\d+)")

    etl_root_dir = os.path.dirname(os.path.abspath(__file__))

    @property
    def events_api_url(self) -> str:
        return f"http://{self.events_api_host}:{self.events_api_port}"

    @property
    def database_uri(self) -> str:
        return os.path.join(self.etl_root_dir, "analytics.sqlite")

    @property
    def analytics_schema_script_path(self) -> str:
        return os.path.join(self.etl_root_dir, "schema.sql")


def get_config() -> Config:
    config = Config()

    # Create directory if missing
    os.makedirs(config.data_dir, exist_ok=True)

    return config
