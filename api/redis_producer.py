from os import getenv
from redis import Redis

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6441)
REDIS_QUEUE_NAME = getenv('REDIS_QUEUE_NAME', '')

r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def cache_order(order: dict):
    key = f"order:{order['order_id']}"

    r.hset(key, mapping=order)
    r.lpush(
        REDIS_QUEUE_NAME,
        json.dumps({
            "order_id": order["order_id"],
            'sender': 'producer1',
            'pushed_at': datetime.now().isoformat()
        })
    )

def get_order(order_id: str):
    pass

