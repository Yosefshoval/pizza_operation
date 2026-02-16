from os import getenv
from confluent_kafka import Producer

KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'pizza-orders')

producer_config = {'bootstrap.servers' : KAFKA_URI}
producer = Producer(producer_config)


def publish_message(message: dict):
    pass
