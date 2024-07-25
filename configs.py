import os
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, FloatType
from pyspark import SparkConf
from dotenv import load_dotenv

load_dotenv()


class Settings:
    ES_USERNAME = os.environ.get("ELASTIC_USERNAME") or "elastic"
    ES_PASSWORD = os.environ.get("ELASTIC_PASSWORD") or 'password'
    ES_HOST = os.environ.get("ES_HOST") or "https://localhost"
    ES_PORT = os.environ.get("ES_PORT") or "9200"

    SPARK_MASTER = os.environ.get("SPARK_MASTER") or 'local[*]'
    SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME") or 'appName'
    SPARK_CHECKPOINT_LOCATION = os.environ.get("SPARK_CHECKPOINT_LOCATION") or 'checkpoint'
    SPARK_DATA_SOURCE = os.environ.get("SPARK_DATA_SOURCE") or 'parquet'
    SPARK_OUTPUT_MODE = os.environ.get("SPARK_OUTPUT_MODE") or 'append'
    SPARK_PATH = os.environ.get("SPARK_PATH") or 'data' # local storage
    SPARK_ES_INDEX = os.environ.get("SPARK_ES_INDEX") or 'news' # index name in elastic-kibana
    SPARK_ES_DOC_TYPE = os.environ.get("SPARK_ES_DOC_TYPE") or 'fintech-news' # database collection in elastic-kibana
    SPARK_ES_CHECKPOINT_LOCATION = os.environ.get("SPARK_ES_CHECKPOINT_LOCATION") or "data"
    SPARK_ES_DATA_SOURCE = os.environ.get("SPARK_ES_DATA_SOURCE") or "org.elasticsearch.spark.sql"
    SPARK_ES_OUTPUT_MODE = os.environ.get("SPARK_ES_OUTPUT_MODE") or 'append'

    @staticmethod
    def get_spark_config():
        conf = SparkConf()
        conf.setMaster(Settings.SPARK_MASTER)
        conf.setAppName(Settings.SPARK_APP_NAME)
        conf.set("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                 "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,"
                 "org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0")
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
        conf.set("spark.sql.legacy.timeParserPolicy", 'LEGACY')

        return conf

    @staticmethod
    def get_spark_mongo_config():
        conf = SparkConf()
        conf.setMaster(Settings.SPARK_MASTER)
        conf.setAppName(Settings.SPARK_APP_NAME)
        conf.set("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                 "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,"
                 "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
                 )
        conf.set("spark.streaming.kafka.consumer.poll.ms", "512")
        conf.set("spark.executor.heartbeatInterval", "20s")
        conf.set("spark.network.timeout", "1200s")
        conf.set("spark.sql.legacy.timeParserPolicy", 'LEGACY')

        return conf

    @staticmethod
    def get_spark_mongo_es_config():
        conf = SparkConf()
        conf.setMaster(Settings.SPARK_MASTER)
        conf.setAppName(Settings.SPARK_APP_NAME)
        conf.set("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                 "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,"
                 "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
                 "org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0")
        conf.set("spark.streaming.kafka.consumer.poll.ms", "512")
        conf.set("spark.executor.heartbeatInterval", "20s")
        conf.set("spark.network.timeout", "1200s")
        conf.set("spark.sql.legacy.timeParserPolicy", 'LEGACY')
        conf.set("es.nodes", Settings.ES_HOST)
        conf.set("es.port", Settings.ES_PORT)
        conf.set("es.net.http.auth.user", Settings.ES_USERNAME)
        conf.set("es.net.http.auth.pass", Settings.ES_PASSWORD)
        conf.set("es.net.ssl", "true")
        conf.set("es.nodes.resolve.hostname", "false")
        conf.set("es.net.ssl.cert.allow.self.signed", "true")
        conf.set("es.nodes.wan.only", "true")
        conf.set("es.nodes.discovery", "false")

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
            # StructField("image", ArrayType(StructType([
            #     StructField("url", StringType(), True),
            #     StructField("title", StringType(), True),
            # ])), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("crawled", FloatType(), True)
        ])
        return schema


def parse_data_from_kafka_message(sdf):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive treaming data"
    schema = Settings.create_data_structure()
    col = split(sdf['value'], ',')  # split attributes to nested array in one Column
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])


class MongoDBConfig:
    HOST = os.environ.get("MONGODB_HOST", '0.0.0.0')
    PORT = os.environ.get("MONGODB_PORT", '8529')
    USERNAME = os.environ.get("MONGODB_USERNAME", "root")
    PASSWORD = os.environ.get("MONGODB_PASSWORD", "dev123")
    CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL") or f"mongodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
    DATABASE = os.getenv('MONGODB_DATABASE', 'fintech_news')
