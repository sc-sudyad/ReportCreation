class InvalidDateFormatError(Exception):
    """Exception raised for invalid date format.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Please use the YYYY-MM-DD format."):
        self.message = message
        super().__init__(self.message)
