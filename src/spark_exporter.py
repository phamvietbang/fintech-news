import pickle

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, element_at, col, from_json, regexp_replace, split
from pyspark.sql.types import MapType, StringType, ArrayType, StructType, StructField

from configs import Settings
from utils.logger_utils import get_logger

logger = get_logger('Spark Elastic Exporter')

@udf(StringType())
def jsonize_string(string: str):
    return string \
        .replace("=", "\":\"") \
        .replace("[{", "[{\"") \
        .replace("}]", "\"}]") \
        .replace(", ", "\", \"") \
        .replace("}\", \"{", "\"}, {\"") \

class SparkElasticExporter:
    def __init__(self, topic, kafka_uri="localhost:29092"):
        self.kafka_uri = kafka_uri
        self.topic = topic
        conf = Settings.get_spark_config()
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    def export_data(self):
        news_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_uri)
            .option("subscribe", self.topic)
            .load()
        )
        deser = udf(lambda x: pickle.loads(x), MapType(StringType(), StringType()))
        deserlized_df = news_df.withColumn('map', deser(news_df['value']))
        parsed_df = deserlized_df.withColumn('id', element_at('map', 'id')) \
            .withColumn('journal', element_at('map', 'journal')) \
            .withColumn('type', element_at('map', 'type')) \
            .withColumn('title', element_at('map', 'title')) \
            .withColumn('attract', element_at('map', 'attract')) \
            .withColumn('author', element_at('map', 'author')) \
            .withColumn('date', element_at('map', 'date')) \
            .withColumn('content', element_at('map', 'content')) \
            .withColumn('image', element_at('map', 'image')) \
            .withColumn('tags', element_at('map', 'tags'))

        schema = ArrayType(StructType([StructField("url", StringType()), StructField("title", StringType()), StructField("content", StringType())]))
        parsed_df = parsed_df.withColumn('id', col('id')) \
            .withColumn('journal', col('journal')) \
            .withColumn('type', col('type')) \
            .withColumn('title', col('title')) \
            .withColumn('attract', col('attract')) \
            .withColumn('author', col('author')) \
            .withColumn('date', col('date')) \
            .withColumn('content', split(regexp_replace("content", r"(^\[)|(\]$)", ""), ", ")) \
            .withColumn('image', from_json(jsonize_string(col('image')), schema=schema)) \
            .withColumn('tags', split(regexp_replace("tags", r"(^\[)|(\]$)", ""), ", "))

        projected_df = parsed_df.select(
            'id',
            'title',
            'type',
            'journal',
            'attract',
            'author',
            'date',
            'content',
            'image',
            'tags')
        while True:
            query = (
                projected_df.writeStream
                .option("checkpointLocation", Settings.SPARK_ES_CHECKPOINT_LOCATION)
                .option("es.resource", f'{Settings.SPARK_ES_INDEX}/{Settings.SPARK_ES_DOC_TYPE}')
                .outputMode(Settings.SPARK_ES_OUTPUT_MODE)
                .format(Settings.SPARK_ES_DATA_SOURCE)
                .start(f'{Settings.SPARK_ES_INDEX}')
            )

            query.awaitTermination()

if __name__ == '__main__':
    job = SparkElasticExporter("baodautu", kafka_uri="localhost:39092")
    job.export_data()