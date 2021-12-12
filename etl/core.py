from datetime import datetime

import pandas as pd
import pycountry
import requests

from etl.config import get_config
from etl.utils import ETLStage, build_api_fetch_events_url, get_export_filename

__all__ = ["fetch_events", "preprocess"]


def fetch_events(start_time: datetime, end_time: datetime) -> str:
    """Fetch events data from the HTTP Server"""
    api_url = build_api_fetch_events_url(start_time, end_time)
    r = requests.get(api_url)

    data = r.json()

    events_df = pd.DataFrame(data.get("data", []))

    export_path = None
    if not events_df.empty:
        properties_df = pd.json_normalize(events_df["properties"])
        events_df = events_df[["event"]].join(properties_df)

        config = get_config()
        filename = get_export_filename(
            etl_stage=ETLStage.raw,
            execution_id=f"{start_time.isoformat()}_{end_time.isoformat()}",
        )
        export_path = config.data_dir / f"{filename}.csv"
        events_df.to_csv(export_path, index=False)

    return export_path


def build_datalake(raw_data: pd.DataFrame) -> str:
    config = get_config()

    # Cleanup country
    raw_data["country"] = raw_data["country_code"].map(
        lambda x: pycountry.countries.lookup(x).name
    )
    del raw_data["country_code"]

    # Cleanup user id
    # ha_user_id is integer
    raw_data["ha_user_id"] = raw_data["ha_user_id"].str.extract(config.ha_user_id_regex)

    filename = (
        f"{get_export_filename(etl_stage=ETLStage.preprocess, execution_id='test')}.csv"
    )
    export_path = config.data_dir / filename
    raw_data.to_csv(export_path)

    return export_path
