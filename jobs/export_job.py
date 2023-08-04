import multiprocessing
import time

from constants import *
from src.spark_exporter import SparkElasticExporter
from utils.logger_utils import get_logger

logger = get_logger('Exporting Job')


class ExportingJob:
    def __init__(self, job_list, kafka_uri=None):
        self.kafka_uri = kafka_uri
        self.job_list = job_list

    def run(self):
        jobs = []
        for job in self.job_list:
            if job in JobName.all:
                self.topic = job
                self.add_job(jobs, job, self.export_function)

        if not jobs:
            logger.warning("There is no job to run!")
        else:
            begin = time.time()
            logger.info(f"Start run {len(jobs)} jobs!")
            for job in jobs:
                job.start()

            for job in jobs:
                job.join()
            logger.info(f"Run {len(jobs)} in {time.time() - begin}s")

    @staticmethod
    def add_job(jobs, name, function):
        logger.info(f"Add exporting job {name}!")
        process = multiprocessing.Process(
            target=function,
        )
        jobs.append(process)
        return jobs

    def export_function(self):
        spark_job = SparkElasticExporter(self.topic, self.kafka_uri)
        spark_job.export_data()


if __name__ == "__main__":
    job = ExportingJob(['dantri'], "localhost:39092")
    job.run()
