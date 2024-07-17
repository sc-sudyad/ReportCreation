import logging
from datetime import datetime

from exception_handling.exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from utils.aggregation import AggregationUtils
from utils.convert_date import DateConverter
from repo.druid_util import DruidUtil
from utils.queries import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ProcessingTimeService:
    def __init__(self):
        self.druid_util = DruidUtil()

    def get_processing_time_data_range(self, datasource: str, device_id: str, start_date: str,
                                       end_date: str, metric_type: str):
        try:
            metric_interval = AggregationUtils.get_metric_interval(metric_type)
            logger.info(f"Metric interval selected: {metric_interval}")
        except ValueError as e:
            logger.error(f"ValueError occurred while selecting metric interval: {e}")
            raise ValueError(f"Invalid metric type: {metric_type}")

        # Convert the date to the required format
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(
                start_date, end_date)
            logger.info(f"Dates converted to required format: {start_date} to {end_date}")
        except InvalidDateFormatError as e:
            logger.error(f"Invalid date format: {start_date}, {end_date}")
            raise InvalidDateFormatError(f"Invalid date format: {e}")

        if device_id:
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
            result = self.druid_util.get_record_count_dict(sql_query)
            logger.info(f"Query executed successfully, result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}, Error: {e}")
            raise DruidUtilError(f"Error executing SQL query: {e}")

    def get_processing_time_aggregated(self, datasource: str, device_id: str, start_date: str,
                                       end_date: str, aggregation_type: str, metric_type: str):
        try:
            aggregation_interval = AggregationUtils.get_aggregation_interval(aggregation_type)
            logger.info(f"Aggregation interval selected: {aggregation_interval}")
        except ValueError as e:
            logger.error(f"ValueError occurred while selecting aggregation interval: {e}")
            raise ValueError(f"Invalid aggregation type: {aggregation_type}")

        try:
            metric_interval = AggregationUtils.get_metric_interval(metric_type)
            logger.info(f"Metric interval selected: {metric_interval}")
        except ValueError as e:
            logger.error(f"ValueError occurred while selecting metric interval: {e}")
            raise ValueError(f"Invalid metric type: {metric_type}")

        # Convert the date to the required format
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(
                start_date, end_date)
            logger.info(f"Dates converted to required format: {start_date} to {end_date}")
        except InvalidDateFormatError as e:
            logger.error(f"Invalid date format: {start_date}, {end_date}")
            raise InvalidDateFormatError(f"Invalid date format: {e}")

        if device_id:
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

        try:
            result = self.druid_util.get_record_count_dict(sql_query)
            logger.info(f"Query executed successfully, result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}, Error: {e}")
            raise DruidUtilError(f"Error executing SQL query: {e}")
