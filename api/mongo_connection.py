from pymongo import MongoClient
from os import getenv
from bson import ObjectId

MONGODB_COLLECTION = getenv('MONGODB_COLLECTION', 'pizza_orders')
MONGODB_DATABASE = getenv('MONGODB_DATABASE', 'pizza_orders')
MONGO_URI = getenv('MONGO_URI', 'mongodb://root:password@localhost:27017/?authSource=admin')


class DBConnection:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)


    def get_collection(self):
        db = self.client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        return collection

    def save_order(self, order: dict):
        cnx = self.get_collection()
        if cnx.find({"order_id" : order.get('order_id')}).to_list():
            print('order already exist in MongoDB')
            return None
        result = cnx.insert_one(order)
        return result.inserted_id


    def get_order_by_id(self, order_id: str):
        cnx = self.get_collection()
        result = cnx.find({'_id' : ObjectId(order_id)})
        return result
