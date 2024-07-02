import json
from json import JSONDecodeError

from confluent_kafka import Consumer
from config.kafka_config import KafkaConfig


class KafkaUtil:
    def __init__(self):
        self.consumer = None
        self.topic = KafkaConfig.TOPIC
        self.bootstrap_servers = KafkaConfig.BOOTSTRAP_SERVER
        self.group_id = KafkaConfig.GROUP_ID

    def create_consumer(self):
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

    def get_all_device_record_count_repo(self, device_type) -> int:
        self.create_consumer()
        self.consumer.subscribe([self.topic])
        total_count = 0
        try:
            while True:
                msg = self.consumer.consume(num_messages=1, timeout=1.0)
                if msg is None or len(msg) == 0:
                    print("No more messages or empty message received. Exiting.")
                    break
                json_data = json.loads(msg[0].value().decode('utf-8'))
                con_value_str = json_data.get('m2m:cin', {}).get('con', {})

                # Parse the inner JSON string
                con_value = json.loads(con_value_str)
                if con_value.get('attributes', {}).get('deviceType') == device_type:
                    total_count += 1

        except JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if self.consumer is not None:
                self.consumer.close()
        return total_count

    def get_device_record_count_per_device_repo(self, device_type, device_id) -> int:
        self.create_consumer()
        self.consumer.subscribe([self.topic])
        count = 0
        try:
            while True:
                msg = self.consumer.consume(num_messages=1, timeout=1.0)
                if msg is None or len(msg) == 0:
                    print("No more messages or empty message received. Exiting.")
                    break

                json_data = json.loads(msg[0].value().decode('utf-8'))
                con_value_str = json_data.get('m2m:cin', {}).get('con', {})

                # Parse the inner JSON string
                con_value = json.loads(con_value_str)

                temp_device_id = con_value.get('attributes', {}).get('deviceID')
                temp_device_type = con_value.get('attributes', {}).get('deviceType')
                if temp_device_id == device_id and temp_device_type == device_type:
                    count += 1
        except JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if self.consumer is not None:
                self.consumer.close()

        return count
