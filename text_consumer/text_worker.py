from confluent_kafka import Consumer
from os import getenv

KAFKA_TOPIC = getenv('KAFKA_TOPIC')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')


consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_TOPIC,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

