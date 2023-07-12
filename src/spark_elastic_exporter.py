import logging
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructType,StructField
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.functions import udf, element_at, from_json, lit, col

from configs import Settings
from utils.logger_utils import get_logger

logger = get_logger('BaoDauTu Crawler')


class SparkElasticExporter:
    def __init__(self, topic, kafka_uri="localhost:29092", elastic_uri="localhost:29092"):
        self.elastic_uri = elastic_uri
        self.kafka_uri = kafka_uri
        self.topic = topic

    @staticmethod
    def get_spark_config():
        conf = SparkConf()
        conf.setMaster(Settings.SPARK_MASTER)
        conf.setAppName(Settings.SPARK_APP_NAME)
        conf.set("spark.streaming.kafka.consumer.poll.ms", "512")
        conf.set("spark.executor.heartbeatInterval", "20s")
        conf.set("spark.network.timeout", "1200s")
        conf.set("es.nodes", Settings.ES_HOST)
        conf.set("es.port", Settings.ES_PORT)
        conf.set("es.net.http.auth.user", Settings.ES_USERNAME)
        conf.set("es.net.http.auth.pass", Settings.ES_PASSWORD)
        conf.set("es.net.ssl", "true")
        conf.set("es.nodes.resolve.hostname", "false")
        conf.set("es.net.ssl.cert.allow.self.signed", "true")
        conf.set("es.nodes.wan.only", "true")
        conf.set("es.nodes.discovery", "false")
        conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        return conf

    @staticmethod
    def create_data_structure():
        schema = StructType([
            StructField("_id", StringType(), False),
            StructField("journal", StringType(), False),
            StructField("type", StringType(), False),
            StructField("attract", StringType(), True),
            StructField("author", StringType(), True),
            StructField("date", StringType(), False),
            StructField("tags", ArrayType(StringType()), True),
            StructField("content", ArrayType(StringType()), True),
            StructField("image", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
            ])), True),
        ])
        return schema

    def export_data(self):
        conf = self.get_spark_config()
        spark: SparkSession = (SparkSession.builder
                               .config(conf=conf)
                               .getOrCreate())
        schema = self.create_data_structure()
        news_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_uri)
            .option("subscribe", self.topic)
            .load()
            .select(from_json(col("value").cast("string"), schema))
        )

        projected_df = news_df.select(
            '_id', 'title', 'type', 'journal', 'attract', 'author', 'date', 'tags')
        while True:
            query = (
                projected_df.writeStream
                .option("checkpointLocation", Settings.SPARK_CHECKPOINT_LOCATION)
                .option("es.resource", f'{Settings.SPARK_ES_INDEX}/{Settings.SPARK_ES_DOC_TYPE}')
                .outputMode(Settings.SPARK_OUTPUT_MODE)
                .format(Settings.SPARK_DATA_SOURCE)
                .start(f'{Settings.SPARK_ES_INDEX}')
            )
            try:
                query.awaitTermination()
            except StreamingQueryException as error:
                print('Query Exception caught:', error)



