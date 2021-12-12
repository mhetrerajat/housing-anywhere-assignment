import click

from core import fetch_events
from config import get_config

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
    click.echo(f"Exported fetched events to {export_path}")


@cli.command()
def preprocess():
    """Preprocess data and loads into analytics DB"""
    pass


if __name__ == "__main__":
    cli()
