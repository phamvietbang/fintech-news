import multiprocessing
import time

from constants import *
from src.spark_es_exporter import SparkElasticExporter
from src.spark_mongo_exporter import SparkMongoExporter
from src.spark_mongodb_to_es import SparkMongoElasticExporter
from utils.logger_utils import get_logger

logger = get_logger('Exporting Job')


class SourceDatabase:
    mongodb = "mongodb"
    elasticsearch = "elasticsearch"
    mongo_es = "mongo_es"


class ExportingJob:
    def __init__(self, job_list, kafka_uri=None, source_database=SourceDatabase.mongodb):
        self.source_database = source_database
        self.kafka_uri = kafka_uri
        self.job_list = job_list

    def run(self):
        jobs = []
        for job in self.job_list:
            if job in JobName.all:
                self.topic = job
                if self.source_database == SourceDatabase.mongo_es:
                    self.add_job(jobs, job, self.export_mongo_to_elastic_function)
                elif self.source_database == SourceDatabase.mongodb:
                    self.add_job(jobs, job, self.export_mongo_function)
                else:
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

    def export_mongo_function(self):
        spark_job = SparkMongoExporter(self.topic, self.kafka_uri)
        spark_job.export_data()

    def export_mongo_to_elastic_function(self):
        while True:
            spark_job = SparkMongoElasticExporter(self.topic, end_timestamp=time.time() - 3600)
            spark_job.export_data()
            logger.info("sleeping for 1 hour")
            time.sleep(3600)


