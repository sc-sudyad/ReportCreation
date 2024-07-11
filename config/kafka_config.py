import time
import uuid


class KafkaConfig:
    BOOTSTRAP_SERVER = 'devkafka1.pvtneom.com:13010'
    GROUP_ID = f"{time.time()}"
    SSL_CA_LOCATION = 'C:/Users/sudheer_yadav/Downloads/certs/certs/rootCA.crt'
    SSL_CERT_LOCATION = 'C:/Users/sudheer_yadav/Downloads/certs/certs/devkafka1.pvtneom.com.crt'
    SSL_KEY_LOCATION = 'C:/Users/sudheer_yadav/Downloads/certs/certs/devkafka1.pvtneom.com.key'

