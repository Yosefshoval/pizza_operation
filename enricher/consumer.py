from confluent_kafka import Consumer
from os import getenv
import logging
import json
from mongodb import update_order
from redis_cache import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info('hello from enricher consumer')

KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'enricher-team')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'cleaned-instructions')
ttl = 5

consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
logger.info('consumer created')
consumer.subscribe([KAFKA_TOPIC])
logger.info(f'consumer subscribe to topic {KAFKA_TOPIC}. consumer details: {consumer.list_topics()}')

analysis_path = 'pizza_analysis_lists.json'
with open(analysis_path, 'r') as file:
    logger.info(f'analysis file is open')
    analysis_list = json.load(file)

def analysis(order_instructions: str):
    status = {
        "is_kosher" : True,
        "is_meat" : False,
        "is_dairy" : True,
        "gluten" : True
        }

    logger.info('analysis_list["forbidden_non_kosher"] loop: ')
    for word in analysis_list['forbidden_non_kosher']:
        if word in order_instructions:
            status['is_kosher'] = False
            break

    logger.info("analysis_list['meat_ingredients'] loop: ")
    for word in analysis_list['meat_ingredients']:
        if word in order_instructions:
            status['is_meat'] = True
            break

    logger.info("analysis_list['dairy_ingredients'] loop: ")
    for word in analysis_list['dairy_ingredients']:
        if word in order_instructions:
            status['is_dairy'] = True
            if status['is_meat']:
                status['is_kosher'] = False

    logger.info(f'status = {status}')
    return status



def listener():
    logger.info('listener loop starting')
    while True:
        try:
            order_metadata = consumer.poll(1.0)
            if order_metadata is None:
                continue
            if order_metadata.error():
                logger.error(f'Error: {order_metadata.error()}.')
                continue

            order_value = json.loads(order_metadata.value().decode('utf-8'))
            logger.info(f'order_value: {order_value}')

            status = search_pizza_type(order_value['pizza_type'])
            logger.info(f'status received from redis: {status}')
            if not status:
                status = analysis(order_value['pizza_prep'])
                if 'GLUTEN_FREE' in order_value['pizza_prep']:
                    status['gluten'] = False
                if 'VEGAN' in order_value['pizza_prep']:
                    status['is_dairy'] = False

                if status['is_kosher']:
                    status['status'] = 'DELIVERED'
                else:
                    status['status'] = 'BURNT'

            updated = update_order(status)
            logger.info(f'order updated in mongo: {updated}\n\n')
            cache_pizza_type(pizza_type=order_value['pizza_type'], status=status, ttl=ttl)

        except Exception as e:
            logger.error(e)
            continue


listener()