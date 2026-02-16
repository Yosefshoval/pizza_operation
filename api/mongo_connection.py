from pymongo import MongoClient
from os import getenv

MONGODB_COLLECTION = getenv('MONGODB_COLLECTION')
MONGODB_DATABASE = getenv('MONGODB_DATABASE')
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
        result = cnx.insert_one(order)
        return result.inserted_id


    def get_order_by_id(self, order_id: str):
        cnx = self.get_collection()
        result =  cnx.find({'order_id' : order_id})
        print(result)
        return result
