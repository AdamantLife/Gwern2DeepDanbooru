import click
from Gwern2DeepDanbooru import G2DD
import time

@click.group()
def cli():
    pass

@cli.command()
def run():
    g2dd = G2DD()
    start = time.time()
    g2dd.create_project_immediate()
    diff = time.time() - start
    hm,seconds = divmod(diff,60)
    hours,minutes = divmod(hm,60)
    click.echo(f"Conversion Complete. Time Elapsed: {hours:02} hour{'s' if hours != 1 else ''}, {minutes:02} minute{'s' if minutes != 1 else ''}, and {seconds:02} second{'s' if seconds != 1 else ''}")

if __name__ == "__main__":
    cli()