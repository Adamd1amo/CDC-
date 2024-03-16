import json
import os
from typing import Union, List, Dict
import logging

from pyspark.sql import DataFrameWriter, SparkSession, DataFrameReader, DataFrame
from pyspark.sql.streaming import DataStreamReader

from spark_workflow.common.utils.PathManager import PathManager
from spark_workflow.common.utils.DataFrameSQLMapping import DataFrameSQLMapping
from spark_workflow.common.CONSTANTS.CONSTANTS import QUERY_EXECUTION_ORDER, QUERY_EXECUTION


def get_conditions(keys: Union[str, List]) -> str:
    condition_pattern = "source.{key} = destination.{key}"
    if "," not in keys:
        return condition_pattern.format(key=keys)
    keys = json.loads(keys)
    return " AND ".join(condition_pattern.format(key=condition_key) for condition_key in keys)


def is_time_segment_key(segment_key: str) -> bool:
    if 'time' in segment_key:
        return True
    return False


def get_raw_configs():
    path = PathManager.get_hdfs_prefix_path() + "/template.json"
    df = (SparkSession.getActiveSession().read.format("json").option("multiLine", True) \
          .load(path))
    return df.collect()[0]


def assign_options(dataframe: Union[DataFrameReader, DataStreamReader, DataFrameWriter], configs: Dict) \
        -> Union[DataFrameReader, DataStreamReader, DataFrameWriter]:
    for item in configs.items():
        dataframe = dataframe.option(item[0], item[1])
    return dataframe


def get_env_variable(name_variable: str):
    return os.environ[name_variable]


def delivery_callback(err, msg):
    logging.basicConfig(filename='kafka_logging.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    if err:
        logging.error('Message failed delivery: %s', err)
    else:
        log_message = f"Produced event to topic {msg.topic()}: key = {msg.key().decode('utf-8')} value = {msg.value().decode('utf-8')}"
        logging.info(log_message)


def mapping_sql_query(data: DataFrame, requirement: Dict) -> DataFrame:
    dataframe_sql_mapping = DataFrameSQLMapping()
    for key, value in QUERY_EXECUTION_ORDER.items():
        if key in requirement.keys():
            QUERY_EXECUTION_ORDER[key] = requirement[key]
    query_ordered = {k: v for k, v in QUERY_EXECUTION_ORDER.items() if v is not None}
    for sql_query, value in query_ordered.items():
        data = dataframe_sql_mapping.mapping(data, sql_query, value)
    return data


def convert_dataframe_to_dict(dataframe: DataFrame) -> str:
    result = dataframe.toPandas().to_dict(orient='list')
    # result = []
    # for row in rows_list:
    #     result.append(row.asDict())
    return json.dumps(result)

def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()