from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import functions as func
from pyspark.sql.types import *
import logging

from project.common import get_spark_session
from project.common.utils.PathManager import PathManager

#Global Variables
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SUBSCRIBE_TOPIC = "TimeVarianceAuthority"


def read_kafka_message() -> DataFrame:
    try:
        message_reader = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_SUBSCRIBE_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafkaConsumer.pollTimeoutMs", "1000") \
            .option("fetchOffset.numRetries", "3") \
            .load()

        return message_reader
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in read_message_kafka: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def write_kafka_message(message_reader: DataFrame) -> StreamingQuery:
    try:
        message_writer = message_reader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("json") \
            .option("path", PathManager.get_hdfs_streaming_path() + "/raw/message/") \
            .option("checkpointLocation", PathManager.get_hdfs_streaming_path() + "/raw/checkpoint/") \
            .outputMode("append") \
            .start()
        return message_writer
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in kafka_message_writer: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def transform(message_reader: DataFrame):
    try:
        mapped_df = mapping_string_to_df(message_reader)
        dataframes_dict = divide_dataframes_by_table_name(mapped_df)
        for table_name in dataframes_dict.keys():
            # check table configuration exists
            if TableProperties(spark).get_table_configurations(table_name):
                source_df = mapping_raw_to_df(spark, table_name, source_df=dataframes_dict.get(table_name))
                if DeltaTable.isDeltaTable(spark, PathManager.get_hdfs_table_path(table_name, "delta")):
                    cdc_processing(spark, source_df, table_name)
                else:
                    write_df_to_hdfs(spark=spark, df=source_df, table_name=table_name)
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in transform: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")
        raise Exception


def transform_message_from_kafka(message_reader: DataFrame) -> StreamingQuery:
    try:
        transform_writer = message_reader.selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .option("checkpointLocation", PathManager.get_hdfs_default_message_path() + "/transform/checkpoint/") \
            .trigger(processingTime='10 seconds') \
            .foreachBatch(lambda batch_df, _: transform(batch_df)) \
            .start()
        return transform_writer
    except Exception as e:

        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in transform_message_from_kafka: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def main():
    message_reader = read_kafka_message()
    message_writer = write_kafka_message(message_reader)
    transform_mess_writer = transform_message_from_kafka(message_reader)

    transform_mess_writer.awaitTermination()
    message_writer.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("dev_streaming") \
        .getOrCreate()

    main()
    spark.stop()




spark = get_spark_session("CDCStreaming")