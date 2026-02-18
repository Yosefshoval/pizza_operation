from os import getenv
from pymongo import MongoClient
import logging
from bson import ObjectId

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


MONGODB_COLLECTION = getenv('MONGODB_COLLECTION', 'pizza_orders')
MONGODB_DATABASE = getenv('MONGODB_DATABASE', 'pizza_orders')
MONGO_URI = getenv('MONGO_URI', 'mongodb://root:password@localhost:27017/?authSource=admin')

mongodb_client = MongoClient(MONGO_URI)
logger.info(f'mongo client created: {mongodb_client.server_info()}')

def get_collection():
    db = mongodb_client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    return collection



def update_order(order_id: str, new_status: dict):
    cnx = get_collection()
    result = cnx.update_one({"_id" : ObjectId(order_id)},
                            {'$set' : new_status})
    effect = f'matched: {result.matched_count}, modified: {result.modified_count}.'
    logger.info(f'order with id {order_id} updated')
    logger.info(effect)
    return effect