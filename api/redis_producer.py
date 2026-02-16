import json
from os import getenv
from redis import Redis

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6441)
REDIS_QUEUE_NAME = getenv('REDIS_QUEUE_NAME', '')

r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def cache_order(order: dict):
    key = f"order:{order['order_id']}"
    serialized_order = json.dumps(order)

    r.set(
        name=key,
        value=serialized_order
    )


def get_order(order_id: str):
    order = r.get(f'order:{order_id}')
    return order
