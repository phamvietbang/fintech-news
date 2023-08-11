import click

from cli.multi_processing_crawler import multi_processing_crawler
from cli.multi_processing_exporter import multi_processing_exporter


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass

cli.add_command(multi_processing_crawler, "multi_processing_crawler")
cli.add_command(multi_processing_exporter, "multi_processing_exporter")