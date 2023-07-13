from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.utils import StreamingQueryException

from configs import Settings
from utils.logger_utils import get_logger

logger = get_logger('Spark Elastic Exporter')


class SparkElasticExporter:
    def __init__(self, topic, kafka_uri="localhost:29092"):
        self.kafka_uri = kafka_uri
        self.topic = topic

    def export_data(self):
        conf = Settings.get_spark_config()
        spark: SparkSession = (SparkSession.builder
                               .config(conf=conf)
                               .getOrCreate())
        schema = Settings.create_data_structure()
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
                .option("checkpointLocation", Settings.SPARK_ES_CHECKPOINT_LOCATION)
                .option("es.resource", f'{Settings.SPARK_ES_INDEX}/{Settings.SPARK_ES_DOC_TYPE}')
                .outputMode(Settings.SPARK_ES_OUTPUT_MODE)
                .format(Settings.SPARK_ES_DATA_SOURCE)
                .start(f'{Settings.SPARK_ES_INDEX}')
            )
            try:
                query.awaitTermination()
            except StreamingQueryException as error:
                print('Query Exception caught:', error)


