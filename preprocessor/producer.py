from confluent_kafka import Producer
from os import getenv
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
NEW_TOPIC = getenv('NEW_TOPIC', 'cleaned-instructions')


producer_config = {'bootstrap.servers' : KAFKA_URI}
producer = Producer(producer_config)
logger.info('producer created')


def callback(err, msg):
    if err:
        logger.error(f'Error while trying to send the message: {err}')
        raise KafkaException(err)
    else:
        logger.info(f'message: {msg.value().decode("utf-8")}')


def flush_message(message: dict):
    if "_id" in message: message["_id"] = str(message["_id"])
    value = json.dumps(message).encode('utf-8')
    logger.info(f'order value to push: {value}')
    producer.produce(
        topic=NEW_TOPIC,
        value=value,
        callback=callback
    )
    logger.info(f'order with id {message["_id"]} pushed to kafka')
    producer.flush()