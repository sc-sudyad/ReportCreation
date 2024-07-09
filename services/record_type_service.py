# services/record_type_service.py
from typing import List

from exception_handling.druid_exception import DruidUtilError
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil
from utils.queries import *


class RecordTypeService:
    def __init__(self, kafka_util: KafkaUtil, druid_util: DruidUtil):
        self.kafka_util = kafka_util
        self.druid_util = druid_util

    def get_processing_stats(self, datasource, device_id):
        if device_id is not None:
            total_records = self.kafka_util.get_device_record_count_per_device_repo(
                datasource, device_id)
            sql_query = SQL_GET_PROCESSED_RECORD_COUNT_PER_DEVICE.format(
                datasource=datasource,
                device_id=device_id
            )
        else:
            total_records = self.kafka_util.get_all_device_record_count_repo(
                datasource)
            sql_query = SQL_GET_PROCESSED_RECORD_COUNT_ALL_DEVICE.format(
                datasource=datasource
            )

        try:
            processed_records = self.druid_util.get_record_count(sql_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query. {e}")

        unprocessed_records = total_records - processed_records

        return {
            "total_records": total_records,
            "processed_records": processed_records,
            "unprocessed_records": unprocessed_records
        }

    def get_summary_per_datasource(self, datasource):
        device_ids_query = SQL_GET_ALL_DEVICE_IDS.format(
            datasource=datasource)
        try:
            device_ids = self.druid_util.get_record_count_dict(
                device_ids_query)
        except Exception as e:
            raise DruidUtilError(
                f"Error executing SQL query to get device_ids. {e}")

        results = []
        for device_id in device_ids:
            device_id = device_id['deviceID']
            try:
                mean_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    datasource=datasource,
                    device_id=device_id,
                    metric_interval='AVG')

                max_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    datasource=datasource,
                    device_id=device_id,
                    metric_interval='MAX')
                min_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    datasource=datasource,
                    device_id=device_id,
                    metric_interval='MIN')
                total_record_count_query = SQL_GET_PROCESSED_RECORD_COUNT_PER_DEVICE.format(
                    datasource=datasource,
                    device_id=device_id)

                mean_processing_time = self.druid_util.get_record_count_dict(
                    mean_processing_time_query)[0]['time_diff_in_seconds']
                max_processing_time = self.druid_util.get_record_count_dict(
                    max_processing_time_query)[0]['time_diff_in_seconds']
                min_processing_time = self.druid_util.get_record_count_dict(
                    min_processing_time_query)[0]['time_diff_in_seconds']
                total_record_count = self.druid_util.get_record_count(
                    total_record_count_query)

                results.append({
                    'device_id': device_id,
                    'mean_processing_time': mean_processing_time,
                    'max_processing_time': max_processing_time,
                    'min_processing_time': min_processing_time,
                    'total_record_count': total_record_count
                })
            except Exception as e:
                raise DruidUtilError(
                    f"Error executing SQL query for device_id {device_id}. {e}")

        return results

    def get_summary_all_datasource(self, datasources: List[str]):
        results = []

        for datasource in datasources:
            device_ids_query = SQL_GET_COUNT_ALL_DEVICE_IDS.format(datasource=datasource)
            try:
                device_id_count = self.druid_util.get_record_count(device_ids_query)
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
                total_record_count_query = SQL_GET_PROCESSED_RECORD_COUNT_ALL_DEVICE.format(
                    datasource=datasource)

                mean_processing_time = self.druid_util.get_record_count_dict(
                    mean_processing_time_query)[0]['time_diff_in_seconds']
                max_processing_time = self.druid_util.get_record_count_dict(
                    max_processing_time_query)[0]['time_diff_in_seconds']
                min_processing_time = self.druid_util.get_record_count_dict(
                    min_processing_time_query)[0]['time_diff_in_seconds']
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
                raise DruidUtilError(f"Error executing SQL query for datasource {datasource}. {e}")
        return results
