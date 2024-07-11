# services/summary_service.py
from typing import List

from exception_handling.exception import DruidUtilError, KafkaUtilError
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil
from utils.queries import *


class SummaryService:
    def __init__(self, kafka_util: KafkaUtil, druid_util: DruidUtil):
        self.kafka_util = kafka_util
        self.druid_util = druid_util

    def get_processing_stats(self, input_topic, output_topic, device_type, device_id):
        try:
            total_records = self.kafka_util.get_record_count(input_topic, device_type, device_id, "all_record")
            processed_records = self.kafka_util.get_record_count(output_topic, device_type, device_id,
                                                                 "processed_record")

            unprocessed_records = total_records - processed_records
            return {
                "total_records": total_records,
                "processed_records": processed_records,
                "unprocessed_records": unprocessed_records
            }
        except KafkaUtilError as e:
            raise KafkaUtilError(f"{e}")
        except Exception as e:
            raise KafkaUtilError(f"{e}")

    def get_summary_all_datasource(self, datasources: List[str]):
        results = []

        for datasource in datasources:
            device_ids_query = SQL_GET_COUNT_ALL_DEVICE_IDS.format(
                datasource=datasource)
            try:
                device_id_count = self.druid_util.get_record_count(
                    device_ids_query)
            except Exception as e:
                raise DruidUtilError(f"Error executing SQL query to get device_ids for datasource {datasource}. {e}")

            try:
                mean_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='AVG')

                max_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='MAX')
                min_processing_time_query = SQL_GET_PROCESSING_TIME_ALL_DEVICE.format(
                    datasource=datasource,
                    metric_interval='MIN')
                total_record_count_query = SQL_GET_PROCESSED_COUNT_ALL_DEVICE.format(
                    datasource=datasource)

                mean_processing_time = self.druid_util.get_record_count_dict(
                    mean_processing_time_query)[0]['res']
                max_processing_time = self.druid_util.get_record_count_dict(
                    max_processing_time_query)[0]['res']
                min_processing_time = self.druid_util.get_record_count_dict(
                    min_processing_time_query)[0]['res']
                total_record_count = self.druid_util.get_record_count(
                    total_record_count_query)

                results.append({
                    'datasource': datasource,
                    'device_id_count': device_id_count,
                    'mean_processing_time': mean_processing_time,
                    'max_processing_time': max_processing_time,
                    'min_processing_time': min_processing_time,
                    'total_record_count': total_record_count
                })
            except Exception as e:
                raise DruidUtilError(
                    f"Error executing SQL query for datasource {datasource}. {e}")
        return results
