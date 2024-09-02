import requests
from requests.auth import HTTPBasicAuth
from config.druid_config import DruidConfig

# Druid configuration
druid_url = DruidConfig.DRUID_URL
verify = DruidConfig.VERIFY
cert = DruidConfig.CERT
auth = HTTPBasicAuth(DruidConfig.USERNAME, DruidConfig.PASSWORD)


def get_time_difference():
    sql_query = """
        SELECT "__time", "creationTime", "ingestionTime" 
        FROM "loadtest_sharmaopcua1"
        ORDER BY "__time" DESC
        LIMIT 23500
    """
    try:
        session = requests.Session()
        session.verify = verify
        session.cert = cert
        session.auth = auth

        # Send the query to Druid
        response = session.post(f"{druid_url}/druid/v2/sql", json={"query": sql_query})

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()

            time_differences = []
            records = []
            for record in data:
                creation_time = record["creationTime"]
                ingestion_time = record["ingestionTime"]
                time_difference = ingestion_time - creation_time
                records.append(record)
                time_differences.append(time_difference)

            return time_differences, records
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Test script to call the function and see the results
if __name__ == "__main__":
    result = get_time_difference()

    if result is not None:
        time_differences, records = result
        print("Time Differences (in seconds) between creationTime and ingestionTime:")
        for diff in time_differences:
            if diff > 100000:
                print(diff)

        # Calculate and print the average time difference
        if time_differences:
            avg_time_difference = sum(time_differences) / len(time_differences)
            print(f"\nAverage Time Difference: {avg_time_difference:.2f} seconds")
            print("\nRecord Length:", len(records))
        else:
            print("No time differences found.")
    else:
        print("Failed to retrieve time differences.")
