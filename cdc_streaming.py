from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import functions as func
from pyspark.sql.types import *

from delta.tables import DeltaTable
from confluent_kafka import Consumer

from spark_workflow.common.utils.helper import get_spark_session
from spark_workflow.common.utils.PathManager import PathManager
from spark_workflow.common.utils.TableProperties import TableProperties
from spark_workflow.common.sink.Sink import *
from spark_workflow.transform.Transform import *
from spark_workflow.common.utils.Kafka import KafkaConsumer

import logging

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
        partition = message_reader.select("partition").collect()
        offsets = message_reader.select("offset").collect()

        # combine the partition and offset to the list of dictionary
        partition_offset = [{partition[i][0]: offsets[i][0]} for i in range(len(partition))]

        message_reader = message_reader.drop("offset")
        mapped_df = parse_kafka_value_to_df(message_reader)
        dataframes_dict = get_table_names(mapped_df)
        for table_name in dataframes_dict.keys():
            # check table configuration exists
            source_df = map_df_with_structure_table(table_name, source_df=dataframes_dict.get(table_name))
            if TableProperties().get_table_configurations(table_name):
                if DeltaTable.isDeltaTable(spark, PathManager.get_hdfs_table_path(table_name, "delta")):
                    cdc_processing(source_df, table_name)
                else:
                    write_df_to_hdfs(df=source_df, table_name=table_name)
            else:
                write_df_to_hdfs(df=source_df, table_name=table_name, mode_write="append")

            # commit Kafka's offset
            # In case of error, the offset will not be committed successfully
            ## I will code later        :v
            consumer = KafkaConsumer(KAFKA_BOOTSTRAP_SERVERS, "1", KAFKA_SUBSCRIBE_TOPIC, auto_commit=False)
            consumer.commit_offsets(partition_offset)
        
                    
        
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in transform: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")
        raise Exception


def transform_message_from_kafka(message_reader: DataFrame) -> StreamingQuery:
    try:
        transform_writer = message_reader.selectExpr("CAST(value AS STRING), offset, partition") \
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