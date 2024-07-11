import pickle
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, udf, element_at, col, from_json, regexp_replace, split, to_timestamp, trim, lit
from pyspark.sql.types import MapType, StringType, ArrayType, StructType, StructField

from configs import Settings, MongoDBConfig
from src.mongodb_exporter import MongoDB
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
        .replace(",", "\", \"")
class SparkMongoExporter:
    def __init__(self, topic, kafka_uri="localhost:29092"):
        self.kafka_uri = kafka_uri
        self.topic = topic
        conf = Settings.get_spark_mongo_config()
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        self.mongo = MongoDB()

    def export(self, df):
        result = {
            '_id': df.id,
            'title': df.title,
            'type': df.type,
            'journal': df.journal,
            'attract': df.attract,
            'author': df.author,
            'date': df.date,
            'content': df.content,
            "tags": df.tags
        }

        self.mongo.update_docs(self.topic, [result])

    def export_data(self):
        news_df = (
            self.spark.readStream.format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", self.kafka_uri)
            .option("subscribe", self.topic)
            .load()
        )
        deser = udf(lambda x: pickle.loads(x), MapType(StringType(), StringType()))
        deserlized_df = news_df.withColumn('map', deser(news_df['value']))

        schema = ArrayType(StringType())
        df = deserlized_df.withColumn('id', element_at('map', 'id')) \
            .withColumn('journal', element_at('map', 'journal')) \
            .withColumn('type', element_at('map', 'type')) \
            .withColumn('title', element_at('map', 'title')) \
            .withColumn('attract', element_at('map', 'attract')) \
            .withColumn('author', element_at('map', 'author')) \
            .withColumn('date', element_at('map', 'date')) \
            .withColumn('content', element_at('map', 'content')) \
            .withColumn('image', element_at('map', 'image')) \
            .withColumn('tags', element_at('map', 'tags')) \
            .withColumn("crawled", lit(time.time()))

        df = df.withColumn('_id', col('id')) \
            .withColumn('journal', col('journal')) \
            .withColumn('type', col('type')) \
            .withColumn('title', col('title')) \
            .withColumn('attract', col('attract')) \
            .withColumn('author', col('author')) \
            .withColumn('date', col('date')) \
            .withColumn('content', split(regexp_replace("content", r"(^\[)|(\]$)", ""), ", ")) \
            .withColumn('tags', split(regexp_replace("tags", r"(^\[)|(\]$)", ""), ", "))

        parsed_df = df

        projected_df = parsed_df.select(
            '_id',
            'title',
            'type',
            'journal',
            'attract',
            'author',
            'date',
            'content',
            # 'image',
            'tags',
            'crawled'
        )
        while True:
            query = (
                projected_df.writeStream
                .outputMode("append")
                # foreach(self.export).start()
                .format("mongodb")
                .queryName("ToMDB")
                .option("checkpointLocation", "mongodb_checkpoints")
                .option("forceDeleteTempCheckpointLocation", "true")
                .option('spark.mongodb.connection.uri', MongoDBConfig.CONNECTION_URL)
                .option('spark.mongodb.database', MongoDBConfig.DATABASE)
                .option('spark.mongodb.collection', self.topic)
                .trigger(continuous="10 seconds")
                .start()
            )

            query.awaitTermination()

if __name__ == '__main__':
    job = SparkMongoExporter("baodautu", kafka_uri="localhost:29092")
    job.export_data()