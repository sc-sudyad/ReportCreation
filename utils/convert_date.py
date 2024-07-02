from datetime import datetime

from exception_handling.invalid_date_format import InvalidDateFormatError


class DateConverter:
    @staticmethod
    def convert_to_required_format_for_date_range(start_date: str, end_date: str):
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            formatted_start_date = start_date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
            formatted_end_date = end_date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
            return formatted_start_date, formatted_end_date
        except ValueError:
            raise InvalidDateFormatError()

    @staticmethod
    def convert_to_required_format_for_date(date: str):
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            formatted_start_date = date_obj.strftime("%Y-%m-%dT00:00:00Z")
            formatted_end_date = date_obj.strftime("%Y-%m-%dT23:59:59Z")
            return formatted_start_date, formatted_end_date
        except ValueError:
            raise InvalidDateFormatError()

