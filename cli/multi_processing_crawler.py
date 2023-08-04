import time

import click
from kafka import KafkaProducer

from jobs.crawl_job import CrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Crawler Process')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-j', '--job-list', default=['baodautu', 'dantri'], show_default=True, type=str, help='crawling job', multiple=True)
@click.option('-s', '--start-page', default=1, show_default=True, type=int, help='start page')
@click.option('-e', '--end-page', default=None, show_default=True, type=int, help='end page')
@click.option('-u', '--kafka-uri', default=None, show_default=True, type=str, help='kafka uri')
@click.option('-r', '--re-run', default=False, show_default=True, type=bool, help='run again')
@click.option('-i', '--interval', default=3600, show_default=True, type=int, help='time to sleep')
def multi_processing_crawler(job_list, start_page, end_page, kafka_uri, re_run, interval):
    while True:
        if kafka_uri:
            job = CrawlingJob(job_list, start_page, end_page, kafka_uri, use_kafka=True)
        else:
            job = CrawlingJob(job_list, start_page, end_page)
        job.run()
        if re_run:
            time.sleep(interval)
        else:
            break
