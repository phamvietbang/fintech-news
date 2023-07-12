import os

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
    SPARK_DATA_SOURCE = os.environ.get("SPARK_DATA_SOURCE") or 'dataSource'
    SPARK_OUTPUT_MODE = os.environ.get("SPARK_OUTPUT_MODE") or 'outputMode'
    SPARK_ES_INDEX = os.environ.get("SPARK_ES_INDEX") or 'esIndex'
    SPARK_ES_DOC_TYPE = os.environ.get("SPARK_ES_DOC_TYPE") or 'esDocType'