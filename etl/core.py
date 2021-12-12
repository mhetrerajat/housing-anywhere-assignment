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

    # Fill ha_user_id if we already know it
    df = raw_data[raw_data["ha_user_id"].notnull()]
    user_df = df.groupby(by=["unique_visitor_id"], as_index=False).agg(
        {"ha_user_id": "first"}
    )
    user_df = user_df[["unique_visitor_id", "ha_user_id"]].rename(
        columns={"ha_user_id": "valid_user_id"}
    )
    raw_data = pd.merge(raw_data, user_df, on=["unique_visitor_id"], how="left")
    raw_data["ha_user_id"] = raw_data["ha_user_id"].fillna(raw_data["valid_user_id"])
    del raw_data["valid_user_id"]
    del df
    del user_df

    # Validate one-to-one relation between unique_visitor_id and ha_user_id
    df = raw_data[raw_data["ha_user_id"].notnull()]
    df = df.groupby(by=["unique_visitor_id"], as_index=False).filter(
        lambda g: (g["ha_user_id"].nunique() > 1)
    )
    if not df.empty:
        print(f"Found inconsistency in {len(df)} rows")
        print(
            "Will only keep the latest unique pair of unique_visitor_id and ha_user_id"
        )

        # Find the most recent pair of unique_visitor_id and ha_user_id
        latest_df = (
            df.sort_values(by=["unique_visitor_id", "time"], ascending=[True, False])
            .groupby(by=["unique_visitor_id"], as_index=False)
            .agg({"ha_user_id": "first"})
        )

        # Delete unwanted rows
        # Old pairs of inconsistent unique_visitor_id and ha_user_id
        row_to_keep_df = pd.merge(
            df, latest_df, on=["unique_visitor_id", "ha_user_id"], how="inner"
        )
        rows_to_delete_df = pd.concat([df, row_to_keep_df]).drop_duplicates(keep=False)
        print(f"Deleting {len(rows_to_delete_df)} rows because of inconsistency")

        raw_data = raw_data.loc[~raw_data.index.isin(rows_to_delete_df.index)]

    filename = (
        f"{get_export_filename(etl_stage=ETLStage.preprocess, execution_id='test')}.csv"
    )
    export_path = config.data_dir / filename
    raw_data.to_csv(export_path)

    return export_path
