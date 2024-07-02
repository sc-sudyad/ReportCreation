import time
import uuid


class KafkaConfig:
    BOOTSTRAP_SERVER = 'localhost:9092'
    GROUP_ID = f"g1-{int(time.time())}-{uuid.uuid4()}"
    TOPIC = 'kafka-test'
