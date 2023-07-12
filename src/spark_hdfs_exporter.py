from prettytable import from_json
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from configs import Settings
from utils.logger_utils import get_logger

logger = get_logger('Spark Hadoop Exporter')


class SparkHDFSExporter:
    def __init__(self, topic, kafka_uri="localhost:29092"):
        self.kafka_uri = kafka_uri
        self.topic = topic

    def export_data(self):
        conf = Settings.get_spark_config()
        schema = Settings.create_data_structure()
        spark: SparkSession = (SparkSession.builder
                               .config(conf=conf)
                               .getOrCreate())
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
                .queryName(self.topic)
                .outputMode(Settings.SPARK_OUTPUT_MODE)
                .format(Settings.SPARK_DATA_SOURCE)
                .option("path", Settings.SPARK_PATH)
                .option("checkpointLocation", Settings.SPARK_CHECKPOINT_LOCATION)
                .option("truncate", False)
                .start()
            )
            try:
                query.awaitTermination()
            except StreamingQueryException as error:
                logger.error('Query Exception caught:', error)