class DruidUtilError(Exception):
    """Exception raised for errors in the DruidUtil module."""

    def __init__(self, message="Error in DruidUtil module"):
        self.message = message
        super().__init__(self.message)


class KafkaUtilError(Exception):
    """Exception raised for errors in the KafkaUtil module."""

    def __init__(self, message="Error in KafkaUtil module"):
        self.message = message
        super().__init__(self.message)
