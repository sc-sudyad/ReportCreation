from datetime import datetime
from typing import List, Dict, Any

import requests
import json

from fastapi import HTTPException

# Specify the parameters
cert = ('C:/Users/sudheer_yadav/Documents/GitHub/Certificates/DruidUatCertificates/druid.crt',
        'C:/Users/sudheer_yadav/Documents/GitHub/Certificates/DruidUatCertificates/druid.key')
verify = 'C:/Users/sudheer_yadav/Documents/GitHub/Certificates/DruidUatCertificates/druidCA.crt'
druid_url = 'https://uat-druid-querynode:14024'

device_type = 'EnergyMeter'
device_id = '2020070072'

date = '2022-03-20'
date_obj = datetime.strptime(date, "%Y-%m-%d")
start_date = date_obj.strftime("%Y-%m-%dT00:00:00Z")
end_date = date_obj.strftime("%Y-%m-%dT23:59:59Z")


def get_record_count(sql_query: str) -> List[Dict[str, Any]]:
    try:
        print('Inside druid...')
        session = requests.Session()
        session.verify = False
        session.cert = cert

        # Send the query to Druid
        response = session.post(f"{druid_url}/druid/v2/sql", json={"query": sql_query})

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            # Return the list of dictionaries containing hourly data counts
            return data
        else:
            raise HTTPException(status_code=500, detail=f"Druid query failed: {response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing Druid query: {str(e)}")


SQL = '''SELECT TIME_FLOOR(__time, 'P1M') AS __time,
                MAX((CAST(creationTime AS BIGINT) - CAST(__time AS BIGINT)) / 1000) AS time_diff_in_seconds
                FROM "{device_type}" 
                WHERE "__time" >='{start_date}' AND "__time" <'{end_date}' AND "deviceID"='{device_id}'
                GROUP BY TIME_FLOOR(__time, 'P1M') '''
SQL_2 = '''
    SELECT TIME_FLOOR(__time, 'P1M') AS __time,
           MAX((CAST(creationTime AS BIGINT) - CAST(__time AS BIGINT)) / 1000) AS time_diff_in_seconds
    FROM "{device_type}"
    WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}' AND "deviceID" = '{device_id}'
    GROUP BY TIME_FLOOR(__time, 'P1M')
'''
# Format the query with the provided device_type, start_date, and end_date
sql1 = SQL.format(device_type=device_type, device_id=device_id, start_date=start_date, end_date=end_date)
sql2 = SQL.format(device_type=device_type, device_id=device_id, start_date=start_date, end_date=end_date)
if sql1 == sql2:
    print("Equal")
