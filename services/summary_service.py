import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List

from exception_handling.exception import DruidUtilError, KafkaUtilError
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil
from utils.aggregation import AggregationUtils
from utils.queries import *

from config.logging_config import logger


class SummaryService:
    def __init__(self):
        self.kafka_util = KafkaUtil()
        self.druid_util = DruidUtil()
        self.executor = ThreadPoolExecutor(max_workers=10)

    async def get_processing_stats(self, input_topic, output_topic, device_type, device_id):
        logger.info("Fetching processing stats")
        try:
            total_records = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.kafka_util.get_record_count, input_topic, device_type, device_id, "all_record"
            )
            processed_records = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.kafka_util.get_record_count, output_topic, device_type, device_id,
                "processed_record"
            )

            unprocessed_records = total_records - processed_records
            logger.info("Processing stats fetched successfully")
            response = {
                "total_records": total_records,
                "processed_records": processed_records,
                "unprocessed_records": unprocessed_records
            }
            return json.dumps(response)
        except KafkaUtilError as e:
            logger.error(f"KafkaUtilError occurred while fetching processing stats", exc_info=True)
            raise KafkaUtilError(f"KafkaUtilError occurred while fetching processing stats: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching processing stats", exc_info=True)
            raise KafkaUtilError(f"An unexpected error occurred while fetching processing stats: {e}")

    async def get_summary_all_datasource(self, datasources: List[str]):
        logger.info(f"Fetching summary for all datasources: {datasources}")
        results = []

        for datasource in datasources:
            logger.info(f"Processing datasource: {datasource}")
            device_ids_query = SQL_GET_COUNT_ALL_DEVICE_IDS.format(datasource=datasource)
            try:
                device_id_count = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count, device_ids_query
                )
                logger.info(f"Device ID count for datasource '{datasource}': {device_id_count}")
            except Exception as e:
                logger.error(f"Error executing SQL query to get device_ids for datasource {datasource}", exc_info=True)
                raise DruidUtilError(f"Error executing SQL query to get device_ids for datasource {datasource}. {e}")

            try:
                mean_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='AVG'
                )
                max_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='MAX'
                )
                min_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='MIN'
                )
                total_record_count_query = SQL_GET_PROCESSED_COUNT_ALL_DEVICE.format(datasource=datasource)

                mean_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, mean_processing_time_query
                )
                mean_processing_time = mean_processing_time[0]['res']

                max_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, max_processing_time_query
                )
                max_processing_time = max_processing_time[0]['res']

                min_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, min_processing_time_query
                )
                min_processing_time = min_processing_time[0]['res']

                total_record_count = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count, total_record_count_query
                )

                results.append({
                    'datasource': datasource,
                    'device_id_count': device_id_count,
                    'mean_processing_time': mean_processing_time,
                    'max_processing_time': max_processing_time,
                    'min_processing_time': min_processing_time,
                    'total_record_count': total_record_count
                })
                logger.info(f"Summary for datasource '{datasource}' added to results")
            except Exception as e:
                logger.error(f"Error executing SQL query for datasource {datasource}", exc_info=True)
                raise DruidUtilError(f"Error executing SQL query for datasource {datasource}. {e}")
        logger.info("Summary for all datasources fetched successfully")
        return results

    async def get_latest_summary_by_interval(self, datasources: List[str], interval_type: str):
        logger.info(f"Fetching summary for all datasources: {datasources}")
        results = []
        interval = AggregationUtils.get_summary_interval(interval_type)
        for datasource in datasources:
            logger.info(f"Processing datasource: {datasource}")
            device_ids_query = SQL_GET_COUNT_ALL_DEVICE_IDS.format(datasource=datasource)
            try:
                device_id_count = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count, device_ids_query
                )
                logger.info(f"Device ID count for datasource '{datasource}': {device_id_count}")
            except Exception as e:
                logger.error(f"Error executing SQL query to get device_ids for datasource {datasource}", exc_info=True)
                raise DruidUtilError(f"Error executing SQL query to get device_ids for datasource {datasource}. {e}")

            try:
                mean_processing_time_query = SQL_GET_PROCESSING_TIME_BY_INTERVAL.format(
                    datasource=datasource,
                    metric_interval='AVG',
                    interval=interval
                )
                max_processing_time_query = SQL_GET_PROCESSING_TIME_BY_INTERVAL.format(
                    datasource=datasource,
                    metric_interval='MAX',
                    interval=interval
                )
                min_processing_time_query = SQL_GET_PROCESSING_TIME_BY_INTERVAL.format(
                    datasource=datasource,
                    metric_interval='MIN',
                    interval=interval
                )
                record_count_query = SQL_GET_RECORDS_COUNT_BY_INTERVAL.format(datasource=datasource, interval=interval)

                mean_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, mean_processing_time_query
                )
                mean_processing_time = mean_processing_time[0]['res']

                max_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, max_processing_time_query
                )
                max_processing_time = max_processing_time[0]['res']

                min_processing_time = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count_dict, min_processing_time_query
                )
                min_processing_time = min_processing_time[0]['res']

                total_record_count = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.druid_util.get_record_count, record_count_query
                )

                results.append({
                    'datasource': datasource,
                    'device_id_count': device_id_count,
                    'mean_processing_time': mean_processing_time,
                    'max_processing_time': max_processing_time,
                    'min_processing_time': min_processing_time,
                    'total_record_count': total_record_count
                })
                logger.info(f"Summary for datasource '{datasource}' added to results")
            except Exception as e:
                logger.error(f"Error executing SQL query for datasource {datasource}", exc_info=True)
                raise DruidUtilError(f"Error executing SQL query for datasource {datasource}. {e}")
        logger.info("Summary for all datasources fetched successfully")
        return results

    async def get_device_id_per_datasource(self, datasource):
        logger.info(f"Fetching Device Id for all datasource: {datasource}")
        results = []
        device_ids_query = SQL_GET_ALL_DEVICE_IDS.format(datasource=datasource)

        try:
            device_ids = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.druid_util.get_record_count_dict, device_ids_query
            )
            print(device_ids)
            logger.info(f"Device Id fetched successfully for datasource '{datasource}'")
            for device in device_ids:
                results.append({'device_id': device['deviceID']})
        except Exception as e:
            logger.error(f"Error executing SQL query to get device_ids for datasource {datasource}", exc_info=True)
            raise DruidUtilError(f"Error executing SQL query to get device_ids for datasource {datasource}. {e}")
        return results
