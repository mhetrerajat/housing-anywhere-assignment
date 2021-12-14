from datetime import datetime
from enum import Enum, unique
from urllib.parse import ParseResult, urlencode, urljoin, urlparse

import pyarrow as pa
import pycountry_convert as pc

from etl.config import get_config


@unique
class ETLStage(Enum):
    raw = "raw"
    preprocess = "preprocess"


@unique
class DeviceType(Enum):
    mobile = "mobile"
    desktop = "desktop"
    unknown = "unknown"


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


def get_device_type(browser: str, os: str) -> str:
    device_type = DeviceType.unknown
    mobile_os = set(["android"])

    if not isinstance(browser, str):
        browser = ""

    if not isinstance(os, str):
        os = ""

    browser = browser.lower().strip()
    os = os.lower().strip()

    if ("mobile" in browser and "windows" not in os) or (os in mobile_os):
        device_type = DeviceType.mobile
    elif browser and os:
        device_type = DeviceType.desktop

    # Mark inconsistent data as `unknown`
    # Cannot have `mobile safari` browser on `windows` device
    if "mobile" in browser and "windows" in os:
        device_type = DeviceType.unknown

    return device_type.value


def country_to_continent(country_name: str) -> str:
    country_alpha2 = pc.country_name_to_country_alpha2(country_name)
    country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
    return pc.convert_continent_code_to_continent_name(country_continent_code)


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
        etl_stage.preprocess: schema_base_fields + [("country", pa.string())],
    }
    schema = schema_store[etl_stage]
    return pa.schema(schema)
