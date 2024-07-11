# repo/druid_util.py

from typing import List, Dict, Any

import requests
from config.druid_config import DruidConfig
from exception_handling.exception import DruidUtilError


class DruidUtil:
    def __init__(self):
        self.druid_url = DruidConfig.DRUID_URL
        self.verify = DruidConfig.VERIFY
        self.cert = DruidConfig.CERT

    def get_record_count(self, sql_query: str) -> int:
        try:
            print('Inside druid...')
            session = requests.Session()
            session.verify = self.verify
            session.cert = self.cert

            # Send the query to Druid
            response = session.post(f"{self.druid_url}/druid/v2/sql", json={"query": sql_query})

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                total_count = data[0]['total_count']
                return total_count
            else:
                raise DruidUtilError()
        except Exception as e:
            raise DruidUtilError(f"{e}")

    def get_record_count_dict(self, sql_query: str) -> List[Dict[str, Any]]:
        try:
            print('Inside druid...')
            session = requests.Session()
            session.verify = self.verify
            session.cert = self.cert

            # Send the query to Druid
            response = session.post(f"{self.druid_url}/druid/v2/sql", json={"query": sql_query})

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                raise DruidUtilError()
        except Exception as e:
            raise DruidUtilError(f"{e}")
