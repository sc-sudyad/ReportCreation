# services/record_type_service.py
from exception_handling.druid_exception import DruidUtilError
from repo.kafka_util import KafkaUtil
from repo.druid_util import DruidUtil
from utils.queries import *


class RecordTypeService:
    def __init__(self, kafka_util: KafkaUtil, druid_util: DruidUtil):
        self.kafka_util = kafka_util
        self.druid_util = druid_util

    def get_processing_stats(self, device_type, device_id):
        if device_id is not None:
            total_records = self.kafka_util.get_device_record_count_per_device_repo(device_type, device_id)
            sql_query = SQL_GET_PROCESSED_RECORD_COUNT_PER_DEVICE.format(
                device_type=device_type,
                device_id=device_id
            )
        else:
            total_records = self.kafka_util.get_all_device_record_count_repo(device_type)
            sql_query = SQL_GET_PROCESSED_RECORD_COUNT_ALL_DEVICE.format(
                device_type=device_type
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

    def get_summary(self, device_type):
        device_ids_query = SQL_GET_ALL_DEVICE_IDS.format(device_type=device_type)
        try:
            device_ids = self.druid_util.get_record_count_dict(device_ids_query)
        except Exception as e:
            raise DruidUtilError(f"Error executing SQL query to get device_ids. {e}")

        results = []
        for device_id in device_ids:
            device_id = device_id['deviceID']
            try:
                mean_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    device_type=device_type,
                    device_id=device_id,
                    metric_interval='AVG')

                max_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    device_type=device_type,
                    device_id=device_id,
                    metric_interval='MAX')
                min_processing_time_query = SQL_GET_PROCESSING_TIME_PER_DEVICE.format(
                    device_type=device_type,
                    device_id=device_id,
                    metric_interval='MIN')
                total_record_count_query = SQL_GET_PROCESSED_RECORD_COUNT_PER_DEVICE.format(
                    device_type=device_type,
                    device_id=device_id)

                mean_processing_time = self.druid_util.get_record_count_dict(mean_processing_time_query)[0]['time_diff_in_seconds']
                max_processing_time = self.druid_util.get_record_count_dict(max_processing_time_query)[0]['time_diff_in_seconds']
                min_processing_time = self.druid_util.get_record_count_dict(min_processing_time_query)[0]['time_diff_in_seconds']
                total_record_count = self.druid_util.get_record_count(total_record_count_query)

                results.append({
                    'device_id': device_id,
                    'mean_processing_time': mean_processing_time,
                    'max_processing_time': max_processing_time,
                    'min_processing_time': min_processing_time,
                    'total_record_count': total_record_count
                })
            except Exception as e:
                raise DruidUtilError(f"Error executing SQL query for device_id {device_id}. {e}")

        return results
