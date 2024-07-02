from exception_handling.druid_exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from repo.druid_util import DruidUtil
from utils.convert_date import DateConverter
from utils.queries import *
from utils.aggregation import *


class RecordsService:
    def __init__(self, druid_util: DruidUtil):
        self.druid_util = druid_util

    def get_records_count_date_range(self, device_type: str, device_id: str, start_date: str, end_date: str) -> int:
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(start_date, end_date)
        except InvalidDateFormatError:
            raise InvalidDateFormatError()

        if device_id is not None:
            sql_query = SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_PER_DEVICE.format(
                device_type=device_type,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date
            )
        else:
            sql_query = SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_ALL_DEVICE.format(
                device_type=device_type,
                start_date=start_date,
                end_date=end_date
            )
        try:
            return self.druid_util.get_record_count(sql_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query: {sql_query}. Error: {e}")

    def get_records_count_aggregated(self, device_id: str, device_type: str, start_date: str, end_date: str,
                                     aggregation_type: str):
        try:
            aggregation_interval = AggregationUtils.get_aggregation_interval(aggregation_type)
        except ValueError as e:
            raise ValueError(str(e))

        # Convert the date to the required format
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(start_date, end_date)
        except InvalidDateFormatError:
            raise InvalidDateFormatError()

        if device_id is not None:
            sql_query = SQL_GET_RECORDS_COUNT_AGGREGATED_PER_DEVICE.format(
                aggregation_interval=aggregation_interval,
                device_type=device_type,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date
            )
        else:
            sql_query = SQL_GET_RECORDS_COUNT_AGGREGATED_ALL_DEVICE.format(
                aggregation_interval=aggregation_interval,
                device_type=device_type,
                start_date=start_date,
                end_date=end_date
            )
        try:
            return self.druid_util.get_record_count_dict(sql_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query. {e}")
