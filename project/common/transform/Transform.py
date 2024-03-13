from typing import Union, List
import json
import logging

from pyspark.sql import functions as func, DataFrameWriter, DataFrame, SparkSession
from pyspark.sql.window import Window
from delta import DeltaTable

from project.common.utils.PathManager import PathManager
from project.common.utils.helper import is_time_segment_key, get_conditions


def parse_kafka_value_to_df(message_reader: DataFrame) -> DataFrame:
    try:
        return message_reader \
            .na.drop() \
            .withColumn("before", func.get_json_object(func.col("value"), "$.before")) \
            .withColumn("after", func.get_json_object(func.col("value"), "$.after")) \
            .withColumn("timestamp", func.get_json_object(func.col("value"), "$.ts_ms")) \
            .withColumn("transaction",
                        func.when(func.col("before").isNotNull() & func.col("after").isNull(), "delete") \
                        .when(func.col("before").isNull() & func.col("after").isNotNull(), "insert")
                        .otherwise("update")) \
            .withColumn("table_name", func.get_json_object(func.col("value"), "$.source.table")) \
            .withColumn("data",
                        func.when(func.col("transaction") != 'delete', func.col("after")) \
                        .otherwise(func.col("before")))
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in parse_kafka_value_to_df: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def map_df_with_structure_table(table_name: str, source_df: DataFrame) -> DataFrame:
    table_schema = TableProperties().get_struct_type(table_name)
    segment_key = TableProperties().get_segment_key(table_name)
    source_df = source_df \
        .withColumn("parse_data", func.from_json(col=func.col("data"), schema=table_schema)) \
        .select("parse_data.*", "timestamp", "transaction")
    if is_time_segment_key(segment_key):
        source_df = source_df \
            .withColumn(segment_key, func.from_unixtime(func.col(segment_key) / 1000000,
                                                        "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("date", func.date_format(func.col(segment_key), "yyyy-MM-dd"))
    return source_df


def cdc_processing(source_df: DataFrame, table_name: str):
    table_primary_key = TableProperties().get_primary_key(table_name)
    try:
        matching_conditions = get_conditions(table_primary_key)
        update_conditions = "source.timestamp > destination.timestamp"
        delete_conditions = "source.transaction = 'delete'"
        destination_path = PathManager().get_hdfs_table_path(table_name, format_file="delta")
        window_func = Window.partitionBy(table_primary_key).orderBy(func.col("timestamp").desc())
        newest_rows_df = source_df.withColumn("rank", func.rank().over(window_func)) \
            .where("rank == 1") \
            .drop("rank")
        # newest_rows_df.persist()
        destination_df = DeltaTable.forPath(SparkSession.getActiveSession(), destination_path)
        destination_df.alias("destination") \
            .merge(
            newest_rows_df.alias("source"),
            matching_conditions) \
            .whenMatchedDelete(condition=delete_conditions) \
            .whenMatchedUpdateAll(condition=update_conditions) \
            .whenNotMatchedInsertAll() \
            .execute()
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in cdc_processing: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")


def divide_dataframes_by_table_name(dataframe: DataFrame) -> dict:
    try:
        rows_list = dataframe.select("table_name").distinct().collect()
        table_names_list = [row.asDict()['table_name'] for row in rows_list]
        # for row in rows_list:
        #     table_names_list.append(row.asDict()['table_name'])
        dataframes_dict = dict()
        for table_name in table_names_list:
            table_df = dataframe.where(func.col("table_name") == table_name)
            dataframes_dict[table_name] = table_df
        return dataframes_dict
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"##############################")
        logging.error(f"Error in divide_dataframes_by_table_name: {e}")
        logging.error(f"##############################")
        logging.error(f"##############################")
