from flask import Blueprint, request, current_app
from markupsafe import escape

from api.db import get_db
from api.utils import is_valid_datetime

bp = Blueprint("events", __name__, url_prefix="/v1/events/")


@bp.route("/", methods=["GET"])
def fetch_events():
    event_id = escape(request.args.get("event_id", ""))
    timeperiod = escape(request.args.get("timeperiod", ""))

    db = get_db()
    cur = db.cursor()

    filter_statements = ""
    if event_id:
        filter_statements += f"WHERE event='{event_id}'"

    if timeperiod:
        start, end = timeperiod.split("::")

        if not is_valid_datetime(start):
            current_app.logger.error(
                f"Invalid start date in timeperiod parameter : {timeperiod}"
            )

        if not is_valid_datetime(end):
            current_app.logger.error(
                f"Invalid end date in timeperiod parameter : {timeperiod}"
            )

        if event_id:
            filter_statements += " AND"
        else:
            filter_statements += "WHERE"

        filter_statements += f" time BETWEEN '{start}' AND '{end}'"

    query = f"SELECT * FROM raw_events {filter_statements}"

    data = cur.execute(query).fetchall()

    formatted_response = []
    for row in data:
        formatted_row = dict(row)
        event = formatted_row.pop("event")
        formatted_response.append({"event": event, "properties": formatted_row})

    return {"data": formatted_response}
