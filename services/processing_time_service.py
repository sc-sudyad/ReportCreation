from datetime import datetime

from exception_handling.druid_exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from utils.aggregation import AggregationUtils
from utils.convert_date import DateConverter
from repo.druid_util import DruidUtil
from utils.queries import *


class ProcessingTimeService:
    def __init__(self, druid_util: DruidUtil):
        self.druid_util = druid_util

    def get_processing_time_data_range(self, datasource: str, device_id: str, start_date: str,
                                       end_date: str, metric_type: str):
        try:
            metric_interval = AggregationUtils.get_metric_interval(metric_type)
        except ValueError as e:
            raise ValueError(str(e))
        # Convert the date to the required format
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(start_date, end_date)
        except InvalidDateFormatError:
            raise InvalidDateFormatError()

        if device_id is not None:
            sql_query = SQL_GET_PROCESSING_TIME_BY_DATE_RANGE_PER_DEVICE.format(
                datasource=datasource,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date,
                metric_interval=metric_interval
            )
        else:
            sql_query = SQL_GET_PROCESSING_TIME_BY_DATE_RANGE_ALL_DEVICE.format(
                datasource=datasource,
                start_date=start_date,
                end_date=end_date,
                metric_interval=metric_interval
            )
        try:
            return self.druid_util.get_record_count_dict(sql_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query. {e}")

    def get_processing_time_aggregated(self, datasource: str, device_id: str, start_date: str,
                                       end_date: str, aggregation_type: str, metric_type: str):
        try:
            aggregation_interval = AggregationUtils.get_aggregation_interval(aggregation_type)
        except ValueError as e:
            raise ValueError(str(e))

        try:
            metric_interval = AggregationUtils.get_metric_interval(metric_type)
        except ValueError as e:
            raise ValueError(str(e))
        # Convert the date to the required format
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(start_date, end_date)
        except InvalidDateFormatError:
            raise InvalidDateFormatError()
        if device_id is not None:
            sql_query = SQL_GET_PROCESSING_TIME_BY_AGGREGATED_PER_DEVICE.format(
                datasource=datasource,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date,
                aggregation_interval=aggregation_interval,
                metric_interval=metric_interval
            )
        else:
            sql_query = SQL_GET_PROCESSING_TIME_AGGREGATED_ALL_DEVICE.format(
                datasource=datasource,
                start_date=start_date,
                end_date=end_date,
                aggregation_interval=aggregation_interval,
                metric_interval=metric_interval
            )
        # Get the data from Druid
        try:
            return self.druid_util.get_record_count_dict(sql_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query. {e}")
