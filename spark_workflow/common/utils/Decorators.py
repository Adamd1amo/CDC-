from functools import wraps

from pyspark.sql import SparkSession
from spark_workflow.common.utils.PathManager import PathManager
from spark_workflow.common.utils.helper import mapping_sql_query
from spark_workflow.common.CONSTANTS.CONSTANTS import QUERY_EXECUTION_ORDER


def prefix_process(func):
    @wraps(func)
    def wrapper(*args):
        params = dict(*args)
        table_name = params['table_name']
        spark = SparkSession.getActiveSession()
        df = spark.read.format("delta").load(PathManager.get_hdfs_table_path(table_name, "delta"))
        df = mapping_sql_query(df, params)
        # params['table_name'] = df
        new_params = {key: value for key, value in params.items() if key not in QUERY_EXECUTION_ORDER.keys()}
        new_params['table_name'] = df
        return func(*(new_params.values()))
    return wrapper
