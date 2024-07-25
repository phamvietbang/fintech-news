import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_timestamp, trim, regexp_replace

from configs import Settings, MongoDBConfig
from utils.logger_utils import get_logger

logger = get_logger('Spark Mongo Elastic Exporter')


class SparkMongoElasticExporter:
    def __init__(self, topic, end_timestamp=time.time() - 3600):
        self.topic = topic
        conf = Settings.get_spark_mongo_es_config()
        self.spark_mongo_es = SparkSession(SparkContext(conf=conf))
        self.end_timestamp = end_timestamp

    def export_data(self):
        schema = Settings.create_data_structure()
        df = (
            self.spark_mongo_es.read.format("mongodb")
            .option('spark.mongodb.connection.uri', MongoDBConfig.CONNECTION_URL)
            .option('spark.mongodb.database', MongoDBConfig.DATABASE)
            .option('spark.mongodb.collection', self.topic)
            .load(schema=schema)
        )
        df = df.withColumn('id', col('_id'))
        parsed_df = df.withColumn(
            'date',
            when(df.journal == "vneconomy", to_timestamp(df.date, 'dd/MM/yyyy'))
            .when(df.journal == "saigontimes", to_timestamp(df.date, "yyyy-MM-dd"))
            .when(df.journal == "vietnamnet", to_timestamp(trim(df.date), 'dd/MM/yyyy'))
            .when(df.journal == "diendandoanhnghiep", to_timestamp(df.date, 'dd/MM/yyyy'))
            .otherwise(to_timestamp(trim(df.date), 'dd/MM/yyyy')))
        if self.end_timestamp:
            parsed_df.filter(parsed_df.crawled > self.end_timestamp)
        projected_df = parsed_df.select(
            'id',
            'title',
            'type',
            'journal',
            'attract',
            'author',
            'date',
            'content',
            # 'image',
            'tags'
        )
        projected_df.show()
        query = (
                projected_df.write
                .format(Settings.SPARK_ES_DATA_SOURCE)
                .option("checkpointLocation", Settings.SPARK_ES_CHECKPOINT_LOCATION)
                .option("es.resource", f'{Settings.SPARK_ES_INDEX}')
                .mode(Settings.SPARK_ES_OUTPUT_MODE)
            )

        try:
            query.save(f'{Settings.SPARK_ES_INDEX}')
        except Exception as error:
            print('Query Exception caught:', error)


