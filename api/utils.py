from datetime import datetime


def is_valid_datetime(datetime_str: str) -> bool:
    date_format = "%Y-%m-%d %H:%M:%S"
    try:
        datetime.strptime(datetime_str, date_format)
    except ValueError as _:
        return False

    return True
