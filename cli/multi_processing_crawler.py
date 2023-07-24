import time

import click
from jobs.crawl_job import CrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Fix contract tag')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-j', '--job-list', default=['baodautu', 'dantri'], show_default=True, type=str, help='crawling job', multiple=True)
@click.option('-s', '--start-page', default=1, show_default=True, type=int, help='start page')
@click.option('-e', '--end-page', default=2, show_default=True, type=int, help='end page')
@click.option('-u', '--use-kafka', default=False, show_default=True, type=bool, help='use kafka')
@click.option('-r', '--re-run', default=False, show_default=True, type=bool, help='run again')
@click.option('-i', '--interval', default=3600, show_default=True, type=int, help='time to sleep')
def multi_processing_crawler(job_list, start_page, end_page, use_kafka, re_run, interval):
    while True:
        job = CrawlingJob(job_list, start_page, end_page, use_kafka)
        job.run()
        if re_run:
            time.sleep(interval)
        else:
            break
