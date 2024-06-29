import pickle

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, udf, element_at, col, from_json, regexp_replace, split, to_timestamp, trim
from pyspark.sql.types import MapType, StringType, ArrayType, StructType, StructField

from configs import Settings, MongoDBConfig
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

class SparkMongoExporter:
    def __init__(self, topic, kafka_uri="localhost:29092"):
        self.kafka_uri = kafka_uri
        self.topic = topic
        conf = Settings.get_spark_mongo_config()
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
        df = deserlized_df.withColumn('id', element_at('map', 'id')) \
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
        df = df.withColumn('id', col('id')) \
            .withColumn('journal', col('journal')) \
            .withColumn('type', col('type')) \
            .withColumn('title', col('title')) \
            .withColumn('attract', col('attract')) \
            .withColumn('author', col('author')) \
            .withColumn('date', col('date')) \
            .withColumn('content', split(regexp_replace("content", r"(^\[)|(\]$)", ""), ", ")) \
            .withColumn('image', from_json(jsonize_string(col('image')), schema=schema)) \
            .withColumn('tags', split(regexp_replace("tags", r"(^\[)|(\]$)", ""), ", "))

        parsed_df = df.withColumn(
            'date',
            when(df.journal == "baodautu", to_timestamp(df.date, 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "dantri", to_timestamp(df.date, 'yyyy-MM-dd HH:mm'))
            .when(df.journal == "vietnamnet", to_timestamp(trim(df.date), 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "vneconomy", to_timestamp(df.date, 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "nhipcaudautu", to_timestamp(df.date, 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "phapluatdoisong", to_timestamp(df.date, 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "vtc", to_timestamp(df.date, 'dd/MM/yyyy HH:mm:ss ZZZZZ'))
            .when(df.journal == "laodong", to_timestamp(df.date, 'dd/MM/yyyy HH:mm'))
            .when(df.journal == "diendandoanhnghiep", to_timestamp(df.date, 'dd/MM/yyyy HH:mm:ss'))
            .otherwise(to_timestamp(trim(df.date), 'dd/MM/yyyy HH:mm')))
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
            'tags'
        )
        while True:
            query = (
                projected_df.writeStream.
                format("mongo").
                outputMode("append").
                option("database", MongoDBConfig.DATABASE).
                option("collection", self.topic).start()
            )

            query.awaitTermination()

if __name__ == '__main__':
    job = SparkMongoExporter("baodautu", kafka_uri="localhost:29092")
    job.export_data()