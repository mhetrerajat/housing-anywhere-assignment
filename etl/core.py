from datetime import datetime

import numpy as np
import pandas as pd
import pycountry
import requests
from pandas.tseries.holiday import USFederalHolidayCalendar as HolidayCalendar
from pypika import CustomFunction, Query, Table
from pypika import functions as fn
from pypika.enums import Order
from pypika.terms import PseudoColumn

from etl.config import get_config
from etl.db import DBManager
from etl.io import export_as_file, export_report, export_to_db, load
from etl.utils import (
    ETLStage,
    build_api_fetch_events_url,
    country_to_continent,
    get_device_type,
)

__all__ = [
    "fetch_events",
    "clean_and_preprocess_data",
    "build_report",
    "import_preprocess_data",
]


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

        # Replace `nan` with empty string
        events_df = events_df.fillna(
            value={col: "" for col in ["browser", "os", "ha_user_id"]}
        )

        events_df = events_df.astype(
            dtype={
                "ha_user_id": str,
                "browser": str,
                "os": str,
                "country_code": str,
                "time": "datetime64[ns]",
            }
        )

        execution_id = f"{start_time.isoformat()}_{end_time.isoformat()}".replace(
            "-", ""
        ).replace(":", "")
        export_path = export_as_file(
            data=events_df, etl_stage=ETLStage.raw, execution_id=execution_id
        )

    return export_path


def clean_and_preprocess_data(raw_data: pd.DataFrame) -> str:
    config = get_config()

    raw_data = raw_data.drop_duplicates()

    # Cleanup country
    raw_data = _preprocess_country_column(df=raw_data)

    # Fill `browser` and `os` if already known
    raw_data = _fill_known_user_device_details(raw_df=raw_data)

    # `ha_user_id` should be numeric
    raw_data["ha_user_id"] = raw_data["ha_user_id"].str.extract(config.ha_user_id_regex)

    # Fill `ha_user_id` if already known
    raw_data = _fill_known_ha_user_id(raw_df=raw_data)

    # Validate many-to-one relation between unique_visitor_id and ha_user_id
    # Make sure each `unique_visitor_id` should have single ha_user_id
    raw_data = _remove_inconsistent_user_pairs(raw_df=raw_data)

    # Replace `nan` with empty string
    raw_data = raw_data.fillna(
        value={col: "" for col in ["browser", "os", "ha_user_id"]}
    )

    export_path = export_as_file(
        data=raw_data, etl_stage=ETLStage.preprocess, execution_id="ha"
    )
    return export_path


def import_preprocess_data():
    # Fetch preprocess data
    pdf = pd.concat([x for x in load(etl_stage=ETLStage.preprocess)])

    pdf = _import_device_details(pdf)

    pdf = _import_users(pdf)

    pdf = _import_locations(pdf)

    pdf = _import_event_date_dimensions(pdf)

    pdf = pdf.fillna(value="")

    table = Table("events")
    export_to_db(pdf, table)


def build_report() -> str:
    export_path = export_report(
        report_name="ha_sample_report",
        reports_data={
            "Events Per Country": _get_events_per_country(),
            "Events by User Type": _get_events_by_user_type(),
        },
    )
    return export_path


#########################################################################
# Local Helper Functions
#########################################################################


def _preprocess_country_column(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and cleanup country column. Replaces country_code to have consistent
    country names across rows"""
    df["country"] = df["country_code"].map(lambda x: pycountry.countries.lookup(x).name)
    del df["country_code"]
    return df


def _fill_known_ha_user_id(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Fill `ha_user_id` if we already know it based on `unique_visitor_id`"""
    df = raw_df[raw_df["ha_user_id"].notnull()]
    user_df = df.groupby(by=["unique_visitor_id"], as_index=False).agg(
        {"ha_user_id": "first"}
    )
    user_df = user_df[["unique_visitor_id", "ha_user_id"]].rename(
        columns={"ha_user_id": "valid_user_id"}
    )
    raw_df = pd.merge(raw_df, user_df, on=["unique_visitor_id"], how="left")
    raw_df["ha_user_id"] = raw_df["ha_user_id"].fillna(raw_df["valid_user_id"])
    del raw_df["valid_user_id"]

    return raw_df


def _remove_inconsistent_user_pairs(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Validate many-to-one relation between unique_visitor_id and ha_user_id.
    Remove the old inconsistent `unique_visitor_id` and `ha_user_id` pairs.
    Make sure each `unique_visitor_id` should have single ha_user_id"""
    df = raw_df[raw_df["ha_user_id"].notnull()]
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

        raw_df = raw_df.loc[~raw_df.index.isin(rows_to_delete_df.index)]
    return raw_df


def _fill_known_user_device_details(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Fill `browser` and `os` columns if we already know it based on `unique_visitor_id`.
    The `unique_visitor_id` is assigned by browser so there must be one-to-one relation
    between device details and `unique_visitor_id`"""

    # `browser` and `os` exists as a pair
    # i.e either both columns will have values else both will be empty
    known_df = raw_df[(raw_df["browser"].notnull()) & (raw_df["os"].notnull())]
    known_df = known_df.groupby(by=["unique_visitor_id"], as_index=False).agg(
        {"browser": "first", "os": "first"}
    )
    known_df = known_df[["unique_visitor_id", "browser", "os"]]
    known_df = known_df.rename(columns={"browser": "known_browser", "os": "known_os"})

    raw_df = pd.merge(raw_df, known_df, on=["unique_visitor_id"], how="left")

    raw_df["browser"] = raw_df["browser"].fillna(raw_df["known_browser"])
    raw_df["os"] = raw_df["os"].fillna(raw_df["known_os"])

    del raw_df["known_browser"]
    del raw_df["known_os"]

    return raw_df


def _add_device_type(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Add device type based on the `browser` and `os`. The device_type column
    can only have three values i.e `mobile`, `desktop` and `unknown`"""

    raw_df.loc[:, "device_type"] = np.vectorize(get_device_type)(
        raw_df["browser"], raw_df["os"]
    )
    raw_df = raw_df.astype(dtype={"device_type": "category"})
    return raw_df


def _get_events_per_country() -> pd.DataFrame:
    """Compute number of events per country"""

    events_table = Table("events")
    locations_table = Table("locations")

    nevents = PseudoColumn("nevents")

    query = (
        Query.from_(events_table)
        .left_join(locations_table)
        .on(events_table.location_key == locations_table.id)
        .select(locations_table.country, fn.Count("*").as_("nevents"))
        .groupby(locations_table.country)
        .orderby(nevents, order=Order.desc)
        .orderby(locations_table.country, order=Order.asc)
    )
    query = query.get_sql()

    db_manager = DBManager()
    data = db_manager.fetch(query)
    return pd.DataFrame(
        data,
        columns=["country", "nevents"],
    )


def _get_events_by_user_type() -> pd.DataFrame:
    """Compute number events for authenticated and unauthenticated users"""
    events_table = Table("events")
    users_table = Table("users")

    nevents = PseudoColumn("nevents")
    user_type = PseudoColumn("user_type")

    iif = CustomFunction(
        name="iif", params=["condition_column", "true_value", "false_value"]
    )

    query = (
        Query.from_(events_table)
        .left_join(users_table)
        .on(events_table.ha_user_key == users_table.id)
        .select(
            iif(users_table.ha_user_id, "Authenticated", "Unauthenticated").as_(
                user_type
            ),
            fn.Count("*").as_(nevents),
        )
        .groupby(user_type)
        .orderby(nevents, order=Order.desc)
    )

    query = query.get_sql()

    db_manager = DBManager()
    data = db_manager.fetch(query)
    return pd.DataFrame(
        data,
        columns=["user_type", "nevents"],
    )


def _import_device_details(preprocess_data: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_data[["browser", "os"]]

    # Add `device_type` column
    df = _add_device_type(raw_df=df)

    # Treat empty strings as NaN
    df = df.replace(r"^\s*$", np.nan, regex=True)

    df = df.drop_duplicates().dropna(subset=["browser", "os"]).reset_index(drop=True)

    table = Table("device_details")
    export_to_db(df, table)

    df = df.reset_index().rename(columns={"index": "device_key"})
    df["device_key"] += 1
    pdf = pd.merge(
        preprocess_data,
        df[["device_key", "browser", "os"]],
        on=["browser", "os"],
        how="left",
    )
    del pdf["browser"]
    del pdf["os"]

    return pdf


def _import_users(preprocess_data: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_data[["ha_user_id"]]

    df = (
        df.replace(r"^\s*$", np.nan, regex=True)
        .drop_duplicates()
        .dropna(subset=["ha_user_id"])
        .reset_index(drop=True)
    )

    table = Table("users")
    export_to_db(df, table)

    df = df.reset_index().rename(columns={"index": "ha_user_key"})
    df["ha_user_key"] += 1

    pdf = pd.merge(preprocess_data, df, on=["ha_user_id"], how="left")
    del pdf["ha_user_id"]

    return pdf


def _import_locations(preprocess_data: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_data[["country"]]

    df = df.drop_duplicates().reset_index(drop=True)

    df["official_country_name"] = df["country"].map(
        lambda x: pycountry.countries.lookup(x).official_name
    )

    df["continent"] = np.vectorize(country_to_continent)(df["country"])

    table = Table("locations")
    export_to_db(df, table)

    df = df.reset_index().rename(columns={"index": "location_key"})
    df["location_key"] += 1

    pdf = pd.merge(
        preprocess_data, df[["location_key", "country"]], on=["country"], how="left"
    )
    del pdf["country"]

    return pdf


def _import_event_date_dimensions(preprocess_data: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_data[["time"]]

    df = df.drop_duplicates().reset_index(drop=True)

    df.loc[:, "month"] = df["time"].dt.month_name()
    df.loc[:, "year"] = df["time"].dt.year
    df.loc[:, "date"] = df["time"].dt.date
    df.loc[:, "day"] = df["time"].dt.day_name()
    df.loc[:, "quarter"] = df["time"].dt.to_period("Q").astype(str)

    calendar = HolidayCalendar()
    holidays = calendar.holidays(start=df["date"].min(), end=df["date"].max())
    df.loc[:, "is_holiday"] = df["date"].isin(holidays)

    table = Table("event_date")
    export_to_db(df, table)

    df = df.reset_index().rename(columns={"index": "event_date_key"})
    df["event_date_key"] += 1

    pdf = pd.merge(
        preprocess_data, df[["event_date_key", "time"]], on=["time"], how="left"
    )
    del pdf["time"]

    return pdf
