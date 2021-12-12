import sqlite3
import click
import pandas as pd

from flask import current_app, g
from flask.cli import with_appcontext


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(*args, **kwargs):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    db = get_db()

    # Execute DDLs
    with current_app.open_resource("schema.sql") as f:
        db.executescript(f.read().decode("utf-8"))

    # Add raw data
    events_df = pd.read_json(current_app.config.get("RAW_EVENTS_DATA"))
    properties_df = pd.json_normalize(events_df["properties"])
    events_df = events_df[["event"]].join(properties_df)

    cur = db.cursor()

    cur.execute("BEGIN TRANSACTION")
    for _, row in events_df.iterrows():
        sql_statement = f"INSERT INTO raw_events(event, time, unique_visitor_id, ha_user_id, browser, os, country_code) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cur.execute(sql_statement, row.to_list())
    cur.execute("COMMIT")


@click.command("initdb")
@with_appcontext
def init_db_command():
    init_db()
    click.echo("Init database")
