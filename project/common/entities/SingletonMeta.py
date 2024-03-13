import time


class SingletonMeta(type):
    __instances = {}
    __instances_time = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            cls.__instances[cls] = super().__call__(*args, **kwargs)
            if cls.__instances[cls].get_time_interval() is not None:
                cls.__instances_time[cls] = time.time()
        elif time.time() - cls.__instances_time[cls] > cls.__instances[cls].get_time_interval():
            cls.__instances[cls] = super().__call__(*args, **kwargs)
            cls.__instances_time[cls] = time.time()
        return cls.__instances[cls]
