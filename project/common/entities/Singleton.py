class Singleton:
    _time_interval = None

    @classmethod
    def get_time_interval(cls):
        return cls._time_interval
