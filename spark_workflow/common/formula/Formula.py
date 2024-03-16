from typing import *
import logging

from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import sum, col, stddev, avg, date_format, to_date
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from spark_workflow.common.utils.Decorators import prefix_process
from spark_workflow.common.utils.helper import convert_dataframe_to_dict


@prefix_process
def standard_deviation_formula(data_frame: DataFrame, column_name):
    try:
        # Calculate the standard deviation  
        std_deviation = data_frame.agg(stddev(column_name).alias('standard_deviation')).collect()[0][
            'standard_deviation']
        # print(f"Standard Deviation of '{column_name}': {std_deviation}")
        return std_deviation
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"Error in SDF: str{e}")
        logging.error(f"##############################")
        return None


@prefix_process
def percent_formula(data_frame: DataFrame, column_name):
    try:
        # Calculate total value of column name
        total_sum = data_frame.agg({column_name: 'sum'}).collect()[0][0]

        # Calculate percent of column name
        percent_result = data_frame.withColumn(f"{column_name}_percent", col(column_name) / total_sum * 100)

        return percent_result

    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"Error in PF: str{e}")
        logging.error(f"##############################")
        return None


# Key and value: Product_code, quantity
@prefix_process
def trending_formula(data_frame: DataFrame, identify_col: str, cal_col: str, limit: int = 1):
    try:
        limit = int(limit)
        result = (data_frame.groupBy(col(identify_col)).agg(sum(cal_col).alias("numbers"))
                  .orderBy(['numbers'], ascending=[False]).limit(limit))
        return convert_dataframe_to_dict(result)
    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"Error in Trending formula: str{e}")
        logging.error(f"##############################")
        return None


@prefix_process
def revenue_formula(data_frame: DataFrame, price_col, quantity_col, segment=None):
    try:
        # Calculate the revenue profit
        revenue_profit_df = data_frame.withColumn("revenue_profit", col(price_col) * col(quantity_col))

        if segment is not None:
            if segment in ['year', 'month']:
                pass
            revenue_profit_df = revenue_profit_df.groupBy(col("date"))

        # Sum up the revenue profit
        total_revenue_profit = revenue_profit_df.agg({"revenue_profit": "sum"}).collect()[0]["sum(revenue_profit)"]

        # Return an integer result
        return total_revenue_profit

    except Exception as e:
        logging.error(f"##############################")
        logging.error(f"Error: str{e}")
        logging.error(f"##############################")
        return None


@prefix_process
def show_data(dataframe: DataFrame) -> str:
    return convert_dataframe_to_dict(dataframe)
