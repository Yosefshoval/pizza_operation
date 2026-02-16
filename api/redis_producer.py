from os import getenv


REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6441)


def cache_order(order: dict):
    pass

def get_order(order_id: str):
    pass

