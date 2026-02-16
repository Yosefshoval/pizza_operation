from pymongo import MongoClient
from os import getenv

MONGODB_COLLECTION = getenv('MONGODB_COLLECTION')
MONGODB_DATABASE = getenv('MONGODB_DATABASE')
MONGO_URI = getenv('MONGO_URI')


class DBConnection:
    def __init__(self):
        self.client = MongoClient(MONGO_URI)


    def get_collection(self):
        db = self.client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        return collection

    def save_order(self, order: dict):
        pass

    def get_order_by_id(self, order_id: str):
        pass
