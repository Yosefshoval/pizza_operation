from confluent_kafka import Consumer
from os import getenv
from pymongo import MongoClient
from redis import Redis
from bson import ObjectId


KAFKA_TOPIC = getenv('KAFKA_TOPIC')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
REDIS_HOST = getenv('REDIS_HOST')
REDIS_PORT = getenv('REDIS_PORT')
MONGODB_COLLECTION = getenv('MONGODB_COLLECTION')
MONGODB_DATABASE = getenv('MONGODB_DATABASE')
MONGO_URI = getenv('MONGO_URI', 'mongodb://root:password@mongodb:27017/?authSource=admin')

consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_TOPIC,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
client = MongoClient(MONGO_URI)
r = Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0)

def get_collection():
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    return collection


def update_status(order_id: str):
    collection = get_collection()
    result = collection.update_one({'_id' : ObjectId(order_id)},
                          {'status' : 'DELIVERED'})
    return f'matched: {result.matched_count}. modified: {result.modified_count}'

def delete_from_redis(order_id: str):
    order_id = f'order:{order_id}'
    deleted = r.delete(order_id)
    return deleted