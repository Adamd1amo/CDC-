from dev.spark_workflow.common.entities.SingletonMeta import SingletonMeta

from spark_workflow.common.entities.Singleton import Singleton


class ConfigurationParser(Singleton, metaclass=SingletonMeta):
    _time_interval = False  # Seconds

    @classmethod
    def transform(cls):
        pass

    @classmethod
    def parser_tables_properties(cls, configs):
        # tables_props = configs['tables']
        tables_props = []
        for row in configs['tables']:
            conf = dict()
            transformed_row = row.asDict()
            table_name = None
            if "table_name" in transformed_row.keys():
                table_name = transformed_row['table_name']
                conf[table_name] = []
                transformed_row.pop("table_name")
            if "schema" in transformed_row.keys():
                schema_fields = transformed_row['schema']['fields']
                transformed_row['schema'] = transformed_row['schema'].asDict()
                transformed_row['schema']['columns'] = []
                for num, _ in enumerate(schema_fields, 0):
                    column_type_pair = transformed_row['schema']['fields'][num].asDict()
                    transformed_row['schema']['columns'].append(column_type_pair)
            del transformed_row['schema']['fields']
            conf[table_name].append(transformed_row)
            tables_props.append(conf)

        return tables_props

    @classmethod
    def get_tables_props_list(cls, configs):
        return cls.parser_tables_properties(configs)

    # Find the pattern of configs/props to avoid create multiple functions
    @classmethod
    def get_database_props(cls):
        pass

    @classmethod
    def get_kafka_configs(cls):
        pass

    @classmethod
    def get_jdbc_configs(cls):
        pass
