import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from exception_handling.exception import DruidUtilError
from exception_handling.invalid_date_format import InvalidDateFormatError
from repo.druid_util import DruidUtil
from utils.convert_date import DateConverter
from utils.queries import *
from utils.aggregation import AggregationUtils

from config.logging_config import logger


class RecordsService:
    def __init__(self):
        self.druid_util = DruidUtil()
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def get_records_count_date_range(self, datasource: str, device_id: str, start_date: str,
                                           end_date: str):
        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(
                start_date, end_date)
            logger.info(f"Dates converted to required format: {start_date} to {end_date}")
        except InvalidDateFormatError:
            logger.error("Invalid date format")
            raise InvalidDateFormatError()

        if device_id:
            sql_query = SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_PER_DEVICE.format(
                datasource=datasource,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date
            )
        else:
            sql_query = SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_ALL_DEVICE.format(
                datasource=datasource,
                start_date=start_date,
                end_date=end_date
            )

        try:
            count = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.druid_util.get_record_count, sql_query
            )
            response = {'count': count}
            return response
        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}", exc_info=True)
            raise DruidUtilError(f"Error executing SQL query: {sql_query}. Error: {e}")

    async def get_records_count_aggregated(self, datasource: str, device_id: str, start_date: str, end_date: str,
                                           aggregation_type: str):
        try:
            aggregation_interval = AggregationUtils.get_aggregation_interval(aggregation_type)
            logger.info(f"Aggregation interval selected: {aggregation_interval}")
        except ValueError as e:
            logger.error(f"ValueError occurred: {e}", exc_info=True)
            raise ValueError(f"Invalid aggregation type: {aggregation_type}")

        try:
            start_date, end_date = DateConverter.convert_to_required_format_for_date_range(start_date, end_date)
            logger.info(f"Dates converted to required format: {start_date} to {end_date}")
        except InvalidDateFormatError as e:
            logger.error(f"Invalid date format: {start_date}, {end_date}")
            raise InvalidDateFormatError(f"Invalid date format: {e}")

        if device_id:
            sql_query = SQL_GET_RECORDS_COUNT_AGGREGATED_PER_DEVICE.format(
                aggregation_interval=aggregation_interval,
                datasource=datasource,
                device_id=device_id,
                start_date=start_date,
                end_date=end_date
            )
        else:
            sql_query = SQL_GET_RECORDS_COUNT_AGGREGATED_ALL_DEVICE.format(
                aggregation_interval=aggregation_interval,
                datasource=datasource,
                start_date=start_date,
                end_date=end_date
            )

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.druid_util.get_record_count_dict, sql_query
            )
            return response
        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}, Error: {e}")
            raise DruidUtilError(f"Error executing SQL query: {e}")
