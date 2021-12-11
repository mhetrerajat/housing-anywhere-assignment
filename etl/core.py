from datetime import datetime, timedelta
from urllib.parse import ParseResult, urlencode, urljoin, urlparse

import pandas as pd
import requests

from etl.config import get_config

__all__ = ["fetch_events"]


def _build_api_url() -> str:
    config = get_config()

    base_url = urljoin(config.events_api_url, "/v1/events/")
    parsed_base_url = urlparse(base_url)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=5)
    params = {"timeperiod": f"{start_time}:{end_time}"}
    encoded_params = urlencode(params)

    api_url = ParseResult(
        parsed_base_url.scheme,
        parsed_base_url.netloc,
        parsed_base_url.path,
        parsed_base_url.params,
        encoded_params,
        parsed_base_url.fragment,
    ).geturl()

    return api_url


def fetch_events():
    """Fetch events data from the HTTP Server"""
    api_url = _build_api_url()
    r = requests.get(api_url)
    data = r.json()

    events_df = pd.DataFrame(data)

    properties_df = pd.json_normalize(events_df["properties"])
    events_df = events_df[["event"]].join(properties_df)

    config = get_config()
    export_path = config.data_dir / f"fetched_events__randomid.csv"
    events_df.to_csv(export_path, index=False)
