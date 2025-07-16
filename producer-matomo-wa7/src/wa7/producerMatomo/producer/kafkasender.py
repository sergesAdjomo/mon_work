 
""" Classe pour envoyer des messages dans des files kafka """
import json
import logging
from confluent_kafka import Producer, KafkaError, KafkaException
from wa7.producerMatomo.producer import parameter as param 
import wa7.producerMatomo.tools.configuration as cfg 

#logger = logging.getLogger(__name__)
logger = cfg.loggerInstance()
config=cfg.configInstance()
bootstrap_servers = config.get('KAFKA' , 'KAFKA_SERVERS')
topic = config.get('KAFKA' , 'KAFKA_TOPIC')
kafka_user = config.get('KAFKA' , 'KAFKA_USER')
kafka_pwd = config.get('KAFKA' , 'KAFKA_PW')
certification = config.get('KAFKA' , 'PATH_CACERT')

class KafkaSender:
    """ Create a KafkaProducer and will send json binary to it with topic """

    def __init__(self):
        self.topic = topic
        self.bootstrap_servers=bootstrap_servers
        self.kafka_user=kafka_user
        self.kafka_pwd=kafka_pwd
        self.certification=certification
        self.producer = self.get_producer()

    def send(self, data):
        """ send encoded json data at topic in KafkaProducer """
        self.producer.produce(self.topic, json.dumps(data).encode('utf-8'))
        self.producer.flush()

    def get_producer(self):
        logger.info("confluent_config ",)
        confluent_config = {
        "bootstrap.servers": self.bootstrap_servers ,
        "sasl.username": self.kafka_user,
        "sasl.password": self.kafka_pwd,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "message.max.bytes": 10000000,
        "ssl.ca.location": certification
        }
        producer = Producer(confluent_config)
        logger.info('Kafka Producer has been initiated...')
        return producer
