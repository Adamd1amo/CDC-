from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import functions as func
from pyspark.sql.types import *


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()