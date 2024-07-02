class DruidUtilError(Exception):
    """Exception raised for errors in the DruidUtil module.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error in DruidUtil module"):
        self.message = message
        super().__init__(self.message)
