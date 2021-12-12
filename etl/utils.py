from urllib.parse import ParseResult, urlencode, urljoin, urlparse
from datetime import datetime

from config import get_config

from enum import Enum, unique


@unique
class ETLStage(Enum):
    raw = 1
    preprocess = 2


def build_api_fetch_events_url(start_time: datetime, end_time: datetime) -> str:
    config = get_config()

    base_url = urljoin(config.events_api_url, "/v1/events/")
    parsed_base_url = urlparse(base_url)

    params = {
        "timeperiod": f"{start_time.strftime(config.events_timeperiod_date_format)}::{end_time.strftime(config.events_timeperiod_date_format)}"
    }
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


def get_export_filename(etl_stage: ETLStage, execution_id: str) -> str:
    return f"{etl_stage.name}__{execution_id}"
