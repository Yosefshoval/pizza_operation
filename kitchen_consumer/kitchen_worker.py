from confluent_kafka import Consumer
from os import getenv
from pymongo import MongoClient
from redis import Redis
from bson import ObjectId
import json
import time



KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'pizza-orders')
KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'text-team')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', '6441')

MONGODB_COLLECTION = getenv('MONGODB_COLLECTION', 'pizza_orders')
MONGODB_DATABASE = getenv('MONGODB_DATABASE', 'pizza_orders')
MONGO_URI = getenv('MONGO_URI', 'mongodb://root:password@localhost:27017/?authSource=admin')


consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

client = MongoClient(MONGO_URI)
r = Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0)

def get_collection():
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    return collection


def update_status(order_id: str):
    collection = get_collection()
    result = collection.update_one({'_id' : ObjectId(order_id)},
                                   {'$set': {'status': 'DELIVERED'}})
    return f'matched: {result.matched_count}. modified: {result.modified_count}'

def delete_from_redis(order_id: str):
    order_id = f'order:{order_id}'
    deleted = r.delete(order_id)
    return deleted



def kafka_listener():
    while True:
        try:
            order = consumer.poll(1.0)

            if order is None:
                continue
            if order.error():
                print(f'Error: {order.error()}.')
                continue

            order_value = json.loads(order.value().decode('utf-8'))

            if "_id" not in order_value:
                continue

            time.sleep(15)
            mongo_updated = update_status(order_value["_id"])

            deleted_from_redis = delete_from_redis(order_value["_id"])

        except Exception as err:
            print(err)
            continue


kafka_listener()