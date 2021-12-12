import click

from etl.config import get_config
from etl.core import fetch_events, build_datalake
from etl.io import load
from etl.utils import ETLStage

etl_config = get_config()


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "start-time",
    type=click.DateTime(formats=[etl_config.events_timeperiod_date_format]),
)
@click.argument(
    "end-time", type=click.DateTime(formats=[etl_config.events_timeperiod_date_format])
)
def raw(start_time, end_time):
    """Fetch events from HTTP Server"""
    export_path = fetch_events(start_time, end_time)
    if export_path:
        click.echo(f"Exported fetched events to {export_path}")
    else:
        click.echo(f"Found no new events between {start_time} and {end_time}")


@cli.command()
def preprocess():
    """Preprocess data and loads into analytics DB"""
    for raw_data in load(etl_stage=ETLStage.raw):
        export_path = build_datalake(raw_data)
        click.echo(f"Exported preprocess data at {export_path}")


if __name__ == "__main__":
    cli()
