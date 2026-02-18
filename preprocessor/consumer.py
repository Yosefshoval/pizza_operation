from confluent_kafka import Consumer, KafkaException
import json
from producer import *
from os import getenv
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info('hello from preprocessor consumer')


KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'prep-team')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'pizza-orders')

consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}

logger.info('consumer created')
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])
logger.info(f'consumer has been subscribed to topic {KAFKA_TOPIC}')


prep_path = './pizza_prep.json'

with open(prep_path, 'r') as file:
    PIZZA_PREP_DATA = json.load(file)


def remove_punctuation_translate(input_string: str):
    cleaned_string = ''.join(c for c in input_string.lower() if c.islower() or c == ' ' or c.isdigit()).upper()
    return cleaned_string


def text_analysis(order_instructions: str):
    allergies = ["gluten ", "peanut ", "allergy"]

    for word in allergies:
        if word in order_instructions.lower():
            cleaned_text = remove_punctuation_translate(order_instructions)
            upper_instructions = cleaned_text.upper()
            return upper_instructions

    return False


def search_prep(pizza_type: str):
    instructions = PIZZA_PREP_DATA.get(pizza_type)
    if instructions:
        return instructions
    return False


def listener():
    logger.info('loop starting: ')
    while True:
        try:
            order = consumer.poll(1.0)
            if order is None:
                continue
            logger.info(f'order {order} received')
            if order.error():
                logger.error(order.error().str())
                raise order.error()

            order_value = json.loads(order.value().decode('utf-8'))

            analysis = text_analysis(order_value['special_instructions'])

            message = {"_id" : order_value["_id"], "pizza_type": order_value["pizza_type"]}
            if analysis:
                message['protocol_cleaned'] = analysis
            logger.info(f'message: {message}')

            pizza_prep = search_prep(order_value['pizza_type'])
            if not pizza_prep:
                continue
            cleaned_prep = remove_punctuation_translate(pizza_prep)

            message['pizza_prep'] = cleaned_prep.upper()
            flush_message(message)

        except Exception as e:
            logger.error(e)
            continue

topics = consumer.list_topics(timeout=5.0)
if topics:
    logger.info(f'topics: {topics}')
    listener()