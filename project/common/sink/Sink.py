import logging
from typing import Dict

from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.streaming import StreamingQuery

from project.common.utils.PathManager import PathManager
from project.common.utils.TableProperties import TableProperties
from project.common.utils.helper import is_time_segment_key, assign_options


def write_kafka_message(message_reader: DataFrame, store_path: str, checkpoint_path: str) -> StreamingQuery:
    try:
        message_writer = message_reader.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("json") \
            .option("path", PathManager.get_hdfs_streaming_path() + store_path) \
            .option("checkpointLocation", PathManager.get_hdfs_streaming_path() + checkpoint_path) \
            .outputMode("append") \
            .start()
        return message_writer
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in kafka_message_writer: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def write_df_to_hdfs(dataframe: DataFrame, table_name: str, options: Dict = None,
                     mode_write: str = 'overwrite', format_file: str = "delta", istemp: bool = None):
    df_writer: DataFrameWriter = dataframe.write.format(format_file).mode(mode_write)
    if istemp is None:
        segment_key = TableProperties().get_segment_key(table_name)
        if is_time_segment_key(segment_key):
            segment_key = 'date'
        if segment_key != "":
            df_writer = df_writer.partitionBy(segment_key)
    else:
        table_name = table_name + "_tmp"
    if options:
        df_writer = assign_options(df_writer, options)
    df_writer.save(PathManager.get_hdfs_table_path(table_name, format_file))
