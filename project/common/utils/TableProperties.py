import time

from pyspark.sql.types import *

from project.common.entities.SingletonMeta import SingletonMeta
from project.common.entities.Singleton import Singleton
from project.common.utils.ConfigurationParser import ConfigurationParser
from project.common.utils.helper import get_raw_configs


class TableProperties(Singleton, metaclass=SingletonMeta):
    _time_interval = 10  # Seconds

    def __init__(self):
        self.__props = None
        self.__createdTime = None
        self.initial_instance()

    def initial_instance(self):
        configs = get_raw_configs()
        self.__props = ConfigurationParser.get_tables_props_list(configs)
        self.__createdTime = time.time()

    def get_table_configurations(self, table_name: str):
        if time.time() - self.__createdTime > self._time_interval:
            self.initial_instance()
        for element in self.__props:
            if list(element.keys())[0] == table_name:
                return element[table_name]
        return None

    def get_struct_type(self, table_name):
        def map_type(field_type: str):
            type_map = {
                'integer': IntegerType(),
                'string': StringType(),
                'double': DoubleType(),
                'boolean': BooleanType(),
                'decimal': DecimalType(10, 2),
                'long': LongType()
                # add more
            }
            return type_map.get(field_type.lower(), StringType())

        struct = []
        columns = self.get_columns_list(table_name)
        if columns is None:
            return None
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            struct.append(StructField(column_name, map_type(column_type), True))
        return StructType(struct)

    def get_schema_dict(self, table_name):
        try:
            table_props = self.get_table_configurations(table_name)
            return table_props[0]['schema']
        except:
            return None

    def get_primary_key(self, table_name):
        try:
            return self.get_schema_dict(table_name)['primary_key']
        except:
            return None

    def get_segment_key(self, table_name):
        try:
            return self.get_schema_dict(table_name)['segment_key']
        except:
            return None

    # def get_columns_list(self, table_name):
    #     try:
    #         return self.get_schema_dict(table_name)['columns']
    #     except:
    #         return None
