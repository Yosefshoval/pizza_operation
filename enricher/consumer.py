from confluent_kafka import Consumer
from os import getenv
import logging
import json
from mongodb import update_order

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logger.info('hello from enricher consumer')

KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'enricher-team')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'cleaned-instructions')



consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_config)
logger.info('consumer created')
consumer.subscribe([KAFKA_TOPIC])
logger.info(f'consumer subscribe to topic {KAFKA_TOPIC}')

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


"""
is_meat
is_dairy
is_kosher

Dairy אין → VEGAN
Gluten אין → GLUTEN-FREE

status = BURNT
"""

def listener():
    while True:
        try:
            order_metadata = consumer.poll(1.0)
            if order is None:
                continue
            if order.error():
                logger.error(f'Error: {order.error()}.')
                continue

            order_value = json.loads(order_metadata.value().decode('utf-8'))
            logger.info(f'order_value: {order_value}')

            status = analysis(order_value['pizza_prep'])
            if 'GLUTEN_FREE' in order_value['pizza_prep']:
                status['gluten'] = False
            if 'VEGAN' in order_value['pizza_prep']:
                status['is_dairy'] = False

            if status['is_kosher']:
                status['status'] = 'DELIVERED'
            else:
                status['status'] = 'BURNT'

            update_order(status)

                # 2: update mongo if kosher or not
            # 3: status = BURNT

        except Exception as e:
            print(e)
            continue