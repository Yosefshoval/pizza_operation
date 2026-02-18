from confluent_kafka import Consumer
from pymongo import MongoClient
from os import getenv
import json


KAFKA_TOPIC = getenv('KAFKA_TOPIC')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
MONGODB_COLLECTION = getenv('MONGODB_COLLECTION', 'pizza_orders')
MONGODB_DATABASE = getenv('MONGODB_DATABASE', 'pizza_orders')
MONGO_URI = getenv('MONGO_URI', 'mongodb://root:password@localhost:27017/?authSource=admin')

consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_TOPIC,
    "auto.offset.reset": "earliest"
}
print(consumer_config)
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

mongodb_client = MongoClient(MONGO_URI)

allergies = ["gluten ","peanut ","allergy"]

def get_collection():
    db = mongodb_client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    return collection


def remove_punctuation_translate(input_string: str):
    cleaned_string = ''.join(c for c in input_string.lower() if c.islower() or c == ' ' or c.isdigit()).upper()
    return cleaned_string


def text_analysis(order_instructions: str):
    for word in allergies:
        if word in order_instructions.lower():
            cleaned_text = remove_punctuation_translate(order_instructions)
            upper_instructions = cleaned_text.upper()
            return upper_instructions
    return False


def update_order_mongo(order: dict):
    cnx = get_collection()
    set_statement = {'allergies_flaged' : order['allergies_flaged']}
    if order.get('protocol_cleaned'):
        set_statement['protocol_cleaned'] = order['protocol_cleaned']

    result = cnx.update_one(
        {'order_id' : order['order_id']},
        {'$set' : {set_statement}}
            )
    effect = f'matched: {result.matched_count}. modified: {result.modified_count}'
    print(effect)
    return effect


def worker_listener():
    while True:
        try:
            order = consumer.poll(1.0)
            print(f'order: {order}')
            if order is None:
                continue
            if order.error():
                print(f'Error: {order.error()}')
                continue

            order_value = json.loads(order.value().decode('utf-8'))
            print(f'order received: {order_value}')

            if "_id" not in order:
                continue

            analysis = text_analysis(order_value['special_instructions'])
            print('analysis: ', analysis)
            if analysis:
                order_value['allergies_flaged'] = True
                order_value['protocol_cleaned'] = analysis
            else:
                order_value['allergies_flaged'] = False
            update_order_mongo(order_value)

        except Exception as e:
            print(e)
            continue

worker_listener()

