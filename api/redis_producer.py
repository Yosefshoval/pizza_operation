from os import getenv
from redis import Redis

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6441)

r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def cache_order(order: dict):
    pass

def get_order(order_id: str):
    pass

