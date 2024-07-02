# queries.py

SQL_GET_ALL_DEVICE_IDS = '''SELECT DISTINCT deviceID
FROM "{device_type}" '''

# -------------------- record_type_queries -----------------------
SQL_GET_PROCESSED_RECORD_COUNT_ALL_DEVICE = '''
    SELECT COUNT(*) AS total_count FROM "{device_type}"
'''
SQL_GET_PROCESSED_RECORD_COUNT_PER_DEVICE = '''
    SELECT COUNT(*) AS total_count FROM "{device_type}"
    WHERE "deviceID" = '{device_id}'
'''

# -------------------- no_of_records_queries -----------------------

SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_ALL_DEVICE = '''
    SELECT COUNT(*) AS total_count 
    FROM "{device_type}" 
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}'
'''

SQL_GET_RECORDS_COUNT_BY_DATE_RANGE_PER_DEVICE = '''
    SELECT COUNT(*) AS total_count
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}' AND "deviceID" = '{device_id}'
'''

SQL_GET_RECORDS_COUNT_AGGREGATED_ALL_DEVICE = '''
    SELECT TIME_FLOOR(__time, '{aggregation_interval}') AS __time,
           COUNT(*) AS record_count
    FROM "{device_type}" 
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}'
    GROUP BY TIME_FLOOR(__time, '{aggregation_interval}')
'''
SQL_GET_RECORDS_COUNT_AGGREGATED_PER_DEVICE = '''
    SELECT TIME_FLOOR(__time, '{aggregation_interval}') AS __time,
           COUNT(*) AS record_count
    FROM "{device_type}" 
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}' AND "deviceID" = '{device_id}'
    GROUP BY TIME_FLOOR(__time, '{aggregation_interval}')
'''
# ----------------------- processing_time_queries -----------------------
SQL_GET_PROCESSING_TIME_ALL_DEVICE = '''
    SELECT
         {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
'''

SQL_GET_PROCESSING_TIME_PER_DEVICE = '''
    SELECT
        {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}" 
    WHERE "deviceID"='{device_id}'
'''
SQL_GET_PROCESSING_TIME_BY_DATE_RANGE_ALL_DEVICE = '''
    SELECT
        {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}'
'''

SQL_GET_PROCESSING_TIME_BY_DATE_RANGE_PER_DEVICE = '''
    SELECT
        {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}' AND "deviceID" = '{device_id}'
'''

SQL_GET_PROCESSING_TIME_AGGREGATED_ALL_DEVICE = '''
    SELECT TIME_FLOOR(__time, '{aggregation_interval}') AS __time,
           {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}'
    GROUP BY TIME_FLOOR(__time, '{aggregation_interval}')
'''
SQL_GET_PROCESSING_TIME_BY_AGGREGATED_PER_DEVICE = '''
    SELECT TIME_FLOOR(__time, '{aggregation_interval}') AS __time,
           {metric_interval}((CAST(ingestionTime AS BIGINT) - CAST(creationTime AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}' AND "deviceID" = '{device_id}'
    GROUP BY TIME_FLOOR(__time, '{aggregation_interval}')
'''
