import json
import logging
from json import JSONDecodeError

from confluent_kafka import Consumer
from config.kafka_config import KafkaConfig
from exception_handling.exception import KafkaUtilError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaUtil:
    def __init__(self):
        self.consumer = None
        self.bootstrap_servers = KafkaConfig.BOOTSTRAP_SERVER
        self.group_id = KafkaConfig.GROUP_ID
        self.security_protocol = 'SSL'
        self.ssl_ca_location = KafkaConfig.SSL_CA_LOCATION
        self.ssl_cert_location = KafkaConfig.SSL_CERT_LOCATION
        self.ssl_key_location = KafkaConfig.SSL_KEY_LOCATION

    def create_consumer(self):
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'security.protocol': self.security_protocol,
            'ssl.ca.location': self.ssl_ca_location,
            'ssl.certificate.location': self.ssl_cert_location,
            'ssl.key.location': self.ssl_key_location
        })

    @staticmethod
    def _parse_message(json_data, parse_type):
        try:
            if parse_type == "all_record":
                con_value_str = json_data.get('m2m:cin', {}).get('con', {})
                con_value = json.loads(con_value_str)
                temp_device_type = con_value.get('attributes', {}).get('deviceType')
                temp_device_id = con_value.get('attributes', {}).get('deviceID')
            elif parse_type == "processed_record":
                temp_device_type = json_data.get('deviceType')
                temp_device_id = json_data.get('deviceID')
            else:
                raise KafkaUtilError(f"Unknown parse type: {parse_type}")
        except JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            raise KafkaUtilError(f"Failed to decode JSON: {e}")
        except KeyError as e:
            logger.error(f"Key error: {e}")
            raise KafkaUtilError(f"Key error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise KafkaUtilError(f"Unexpected error: {e}")

        return temp_device_type, temp_device_id

    def get_record_count(self, input_topic, device_type, device_id, parse_type) -> int:
        self.create_consumer()
        self.consumer.subscribe([input_topic])
        total_count = 0
        logger.info(f"Starting to consume messages from topic: {input_topic}")
        try:
            while True:
                msg = self.consumer.consume(num_messages=1, timeout=1.0)
                if not msg:
                    logger.info("No more messages or empty message received. Exiting.")
                    break
                try:
                    json_data = json.loads(msg[0].value().decode('utf-8'))
                    temp_device_type, temp_device_id = self._parse_message(json_data, parse_type)

                    if device_id and device_type:
                        if temp_device_type == device_type and temp_device_id == device_id:
                            total_count += 1
                    elif device_type:
                        if temp_device_type == device_type:
                            total_count += 1
                    elif device_id:
                        if temp_device_id == device_id:
                            total_count += 1

                except KafkaUtilError as e:
                    logger.error(f"Error while parsing message: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing message:", exc_info=True)
                    raise KafkaUtilError(f"Unexpected error while processing message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during message consumption: {e}", exc_info=True)
            raise KafkaUtilError(f"Unexpected error during message consumption: {e}")
        finally:
            if self.consumer is not None:
                self.consumer.close()
                logger.info("Kafka consumer closed")
        logger.info(f"Total record count: {total_count}")
        return total_count
