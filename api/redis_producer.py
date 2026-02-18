import json
from os import getenv
from redis import Redis

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6441)
print(REDIS_HOST)
r = Redis(host=REDIS_HOST, port=6379, db=0)


def cache_order(order: dict, ttl: int = 60):
    if "_id" in order: order["_id"] = str(order["_id"])

    key = f'order:{str(order["_id"])}'
    serialized_order = json.dumps(order)

    r.set(
        name=key,
        value=serialized_order
    )
    r.expire(key, ttl)
    print('cached')

def get_order(order_id: str):
    order = r.get(f'order:{order_id}')
    return order
