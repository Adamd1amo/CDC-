from dev.spark_workflow.common.entities.SingletonMeta import SingletonMeta
from spark_workflow.common.entities.Singleton import Singleton


class PathManager(Singleton, metaclass=SingletonMeta):
    # __HDFS_PREFIX_PATH = f"hdfs://{os.environ['hdfs_host']}:{os.environ['hdfs_port']}/data"
    __HDFS_PREFIX_PATH = "hdfs://namenode:9000/data"
    # __HDFS_PREFIX_PATH = constants.HDFS_PREFIX
    _time_interval = False

    @classmethod
    def get_hdfs_prefix_path(cls):
        return cls.__HDFS_PREFIX_PATH

    @classmethod
    def get_hdfs_streaming_path(cls):
        return cls.__HDFS_PREFIX_PATH + "/streaming"

    @classmethod
    def get_hdfs_default_checkpoint_path(cls):
        return cls.get_hdfs_streaming_path() + "/checkpoint"

    @classmethod
    def get_hdfs_default_message_path(cls):
        return cls.get_hdfs_streaming_path() + "/message"

    @classmethod
    def get_hdfs_table_path(cls, table_name: str, format_file: str) -> str:
        return cls.get_hdfs_prefix_path() + f"/{table_name}/{format_file}/"
