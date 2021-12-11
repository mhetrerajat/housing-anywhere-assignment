import click

from core import fetch_events


@click.group()
def cli():
    pass


@cli.command()
def raw():
    """Fetch events from HTTP Server"""
    export_path = fetch_events()
    click.echo(f"Exported fetched events to {export_path}")


if __name__ == "__main__":
    cli()
