# repo/druid_util.py

import requests
from requests.auth import HTTPBasicAuth

from config.druid_config import DruidConfig
from exception_handling.exception import DruidUtilError

from config.logging_config import logger


class DruidUtil:
    def __init__(self):
        self.druid_url = DruidConfig.DRUID_URL
        self.verify = DruidConfig.VERIFY
        self.cert = DruidConfig.CERT
        self.auth = HTTPBasicAuth(DruidConfig.USERNAME, DruidConfig.PASSWORD)

    def get_record_count(self, sql_query: str) -> int:
        try:
            logger.info(f"Executing SQL query: {sql_query}")
            session = requests.Session()
            session.verify = self.verify
            session.cert = self.cert
            session.auth = self.auth

            # Send the query to Druid
            response = session.post(f"{self.druid_url}/druid/v2/sql", json={"query": sql_query})

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                total_count = data[0]['total_count']
                logger.info(f"Query successful, total_count: {total_count}")
                return total_count
            else:
                logger.error(
                    f"Failed to execute SQL query: {sql_query}. Status code: {response.status_code}, Response: {response.text}")
                raise DruidUtilError("Failed to execute SQL query.")

        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}", exc_info=True)
            raise DruidUtilError(f"Error executing SQL query: {sql_query}. {e}")

    def get_record_count_dict(self, sql_query: str):
        try:
            logger.info(f"Executing SQL query: {sql_query}")
            session = requests.Session()
            session.verify = self.verify
            session.cert = self.cert
            session.auth = self.auth

            # Send the query to Druid
            response = session.post(f"{self.druid_url}/druid/v2/sql", json={"query": sql_query})

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Query successful, retrieved {len(data)} records")
                return data
            else:
                logger.error(f"Failed to execute SQL query: {sql_query}. Status code: {response.status_code}, Response: {response.text}")
                raise DruidUtilError("Failed to execute SQL query.")

        except Exception as e:
            logger.error(f"Error executing SQL query: {sql_query}", exc_info=True)
            raise DruidUtilError(f"Error executing SQL query: {sql_query}. {e}")
