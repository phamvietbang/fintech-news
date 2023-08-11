import time

import click

from jobs.export_job import ExportingJob
from utils.logger_utils import get_logger

logger = get_logger('Exporter Process')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-j', '--job-list', default=['baodautu', 'dantri'], show_default=True, type=str, help='crawling job', multiple=True)
@click.option('-u', '--kafka-uri', default="localhost:39092", show_default=True, type=str, help='kafka uri')
@click.option('-r', '--re-run', default=False, show_default=True, type=bool, help='run again')
@click.option('-i', '--interval', default=3600, show_default=True, type=int, help='time to sleep')
def multi_processing_exporter(job_list, kafka_uri, re_run, interval):
    while True:
        job = ExportingJob(job_list, kafka_uri)
        job.run()
        if re_run:
            time.sleep(interval)
        else:
            break
