from datetime import datetime

import click
import pandas as pd

from etl.config import get_config
from etl.core import (
    build_report,
    clean_and_preprocess_data,
    fetch_events,
    import_preprocess_data,
)
from etl.db import init_analytics_schema
from etl.io import flush, load
from etl.utils import ETLStage

etl_config = get_config()


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "etl-stage",
    type=click.Choice(
        [ETLStage.raw.name, ETLStage.preprocess.name], case_sensitive=False
    ),
)
def truncate(etl_stage: str):
    """Delete all files from intermediate data store for particular etl stage"""
    flush(etl_stage=ETLStage(etl_stage))
    click.echo("Cleanup `raw` data store")


@cli.command()
@click.argument(
    "start-time",
    type=click.DateTime(formats=[etl_config.events_timeperiod_date_format]),
)
@click.argument(
    "end-time", type=click.DateTime(formats=[etl_config.events_timeperiod_date_format])
)
def raw(start_time: datetime, end_time: datetime):
    """Fetch events from HTTP Server"""
    export_path = fetch_events(start_time, end_time)
    if export_path:
        click.echo(f"Exported fetched events to {export_path}")
    else:
        click.echo(f"Found no new events between {start_time} and {end_time}")


@cli.command()
def preprocess():
    """Preprocess data and exports it to intermediate data store"""
    raw_data = pd.concat([x for x in load(etl_stage=ETLStage.raw)])
    export_path = clean_and_preprocess_data(raw_data)
    click.echo(f"Exported preprocess data to {export_path}")


@cli.command()
def initdb():
    """Initialize `analytics` database by executing DDL queries"""
    init_analytics_schema()
    click.echo("Initialized `analytics` database")


@cli.command()
def importdb():
    """Imports the preprocessed data into `analytics` DB"""
    import_preprocess_data()
    click.echo("Data imported successfully")


@cli.command()
def report():
    """Build sample report based on `analytics` DB"""
    export_path = build_report()
    click.echo(f"Exported report at {export_path}")


if __name__ == "__main__":
    cli()
