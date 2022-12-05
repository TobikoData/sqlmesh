class SparkDbApiError(Exception):
    pass


class NotSupportedError(SparkDbApiError):
    pass


class ProgrammingError(SparkDbApiError):
    pass
