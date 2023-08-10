from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf
from datetime import datetime
from pyspark.sql.session import SparkSession

from configs import Settings

conf = Settings.get_spark_config()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext
schema = Settings.create_data_structure()
df = spark.read.option("multiline", "true").json('./data/*.json', schema=schema)
df = df.withColumn('date', trim(col('date')))
df = df.withColumn('date',
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

df.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "fintech-news") \
    .mode("append") \
    .save()
