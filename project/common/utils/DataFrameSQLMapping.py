from pyspark.sql import DataFrame, functions as func
from pyspark.sql import SparkSession

from project.common.CONSTANTS.CONSTANTS import JOIN_TYPES
from project.common.utils.PathManager import PathManager


class DataFrameSQLMapping(object):
    @classmethod
    def mapping(cls, dataframe: DataFrame, sql_query: str, value: str):
        """The method to map the SQL query.
        :param value: A sql query string to be mapped to a sql query function.
        :returns: The sql query function of the Dataframe.
        """
        if not DataFrame:
            raise ValueError("The dataframe is empty.")
        try:
            if sql_query == 'having':
                sql_query = 'where'
            mapped_function = getattr(
                cls, f"mapping_{sql_query}".lower()
            )
        except AttributeError:
            raise ValueError("The sql query is invalid.")
        return mapped_function(dataframe, value)

    @classmethod
    def mapping_join(cls, dataframe: DataFrame, value: str):
        try:
            join_clauses = value.split("/")
            print("######################################################")
            print(join_clauses)

            def making_alias(df_alias: DataFrame, suffix_name: str):
                for column_name in df_alias.columns:
                    df_alias = df_alias.withColumnRenamed(column_name, f"{column_name}{suffix_name}")
                return df_alias

            df_1 = making_alias(dataframe, "_1")
            for num, clause in enumerate(join_clauses, 2):
                print(num, clause)
                table_name = clause.split(":")[0]
                join_conditions = clause.split(":")[1].split(",")
                join_type = clause.split(":")[2]
                other_df = (SparkSession.getActiveSession().read.format("delta")
                            .load(PathManager.get_hdfs_table_path(table_name, "delta")))

                df = making_alias(other_df, f"_{num}")

                processed_conditions = []
                for condition in join_conditions:
                    cond = condition.split("=")
                    processed_conditions.append(func.col(cond[0]) == func.col(cond[1]))

                df_1 = df_1.join(other=df, on=processed_conditions, how=join_type)
                df_1.show()
            return df_1
        except Exception as e:
            raise Exception("The join clause is invalid:" + value + " " + e)

    @classmethod
    def mapping_where(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_limit #####################################")
            import re
            value = re.sub(r'(AND|OR)', r' \1 ', value)
            value = value.replace("|", "'")
            return dataframe.where(value)
        except Exception:
            raise Exception("The where conditions is invalid:" + value)

    @classmethod
    def mapping_group_by(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_group_by #####################################")
            return dataframe.groupBy(value.split(","))
        except Exception:
            raise Exception("The group by clause is invalid:" + value)

    @classmethod
    def mapping_order_by(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_order_by #####################################")
            columns = value.split("/")[0].split(",")
            ascending = value.split("/")[1]
            asc_bool = [True if x.strip().lower() == 'true' else False for x in ascending.split(',')]
            return dataframe.orderBy(columns, ascending=asc_bool)
        except Exception as e:
            print(e)
            raise Exception("The order by clause is invalid:" + value)

    @classmethod
    def mapping_distinct(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_distinct #####################################")
            if value.lower() == "true":
                return dataframe.distinct()
            return dataframe
        except Exception:
            raise Exception("The distinct clause is invalid:" + value)

    @classmethod
    def mapping_limit(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_limit #####################################")
            return dataframe.limit(int(value))
        except Exception:
            raise Exception("The where conditions is invalid:" + value)

    @classmethod
    def mapping_aggregate(cls, dataframe: DataFrame, value: str):
        try:
            print("mapping_aggregate #####################################")
            agg_funcs = {
                "count": func.count,
                "sum": func.sum,
                "avg": func.avg,
                "max": func.max,
                "min": func.min,
                "mean": func.mean,
                "sumDistinct": func.sumDistinct
            }
            elements = value.split(",")
            agg_expressions = []
            for element in elements:
                agg_func = agg_funcs[element.split(":")[0]]
                col = element.split(":")[1]
                alias = element.split(":")[2]
                if alias:
                    agg_expressions.append(agg_func(col).alias(alias))
                else:
                    agg_expressions.append(agg_func(col))
            return dataframe.agg(*agg_expressions)
        except Exception as e:
            print(e)
            raise Exception("The aggregate is invalid:" + value)

    @classmethod
    def mapping_alias(cls, dataframe: DataFrame, value: str):
        try:
            return dataframe.alias(value)
        except Exception:
            raise Exception("The alias is invalid:" + value)
