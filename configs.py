import os
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark import SparkConf
from dotenv import load_dotenv

load_dotenv()


class Settings:
    ES_USERNAME = os.environ.get("ES_USERNAME") or "elastic"
    ES_PASSWORD = os.environ.get("ES_PASSWORD") or 'password'
    ES_HOST = os.environ.get("ES_HOST") or 'host'
    ES_PORT = os.environ.get("ES_PORT") or 'port'

    SPARK_MASTER = os.environ.get("SPARK_MASTER") or 'master'
    SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME") or 'appName'
    SPARK_CHECKPOINT_LOCATION = os.environ.get("SPARK_CHECKPOINT_LOCATION") or 'checkpoint'
    SPARK_DATA_SOURCE = os.environ.get("SPARK_DATA_SOURCE") or 'parquet'
    SPARK_OUTPUT_MODE = os.environ.get("SPARK_OUTPUT_MODE") or 'append'
    SPARK_PATH = os.environ.get("SPARK_PATH") or 'data'
    SPARK_ES_INDEX = os.environ.get("SPARK_ES_INDEX") or 'esIndex'
    SPARK_ES_DOC_TYPE = os.environ.get("SPARK_ES_DOC_TYPE") or 'esDocType'
    SPARK_ES_CHECKPOINT_LOCATION = os.environ.get("SPARK_ES_CHECKPOINT_LOCATION") or 'checkpoint'
    SPARK_ES_DATA_SOURCE = os.environ.get("SPARK_ES_DATA_SOURCE") or 'dataSource'
    SPARK_ES_OUTPUT_MODE = os.environ.get("SPARK_ES_OUTPUT_MODE") or 'outputMode'

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
            StructField("title", StringType(), False),
            StructField("attract", StringType(), True),
            StructField("author", StringType(), True),
            StructField("date", StringType(), False),
            StructField("content", ArrayType(StringType()), True),
            StructField("image", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
            ])), True),
            StructField("tags", ArrayType(StringType()), True),
        ])
        return schema