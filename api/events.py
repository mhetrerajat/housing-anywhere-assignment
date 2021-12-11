import pandas as pd
from flask import Blueprint, request

bp = Blueprint("events", __name__, url_prefix="/v1/events/")


@bp.route("/", methods=["GET"])
def fetch_events():
    # TODO: Fetch data from DB instead of json file

    event_id = request.args.get("event_id")
    timeperiod = request.args.get("timeperiod")

    events_df = pd.read_json("api/events_data.json")

    # Reformat `properties` column
    properties_df = pd.json_normalize(events_df["properties"])
    events_df = events_df[["event"]].join(properties_df)

    if event_id:
        events_df = events_df[events_df["event"] == event_id]

    if timeperiod:
        start, end = timeperiod.split(":")
        events_df = events_df[(events_df["time"] >= start) & (events_df["time"] <= end)]

    return {"data": events_df.to_dict("records")}
