import json
from os import getenv
from redis import Redis
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6379)
r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

logger.info('redis client created.')

def cache_pizza_type(pizza_type: str, status: dict, ttl):
    str_status = json.dumps(status)
    result = r.set(name=pizza_type, value=str_status, ex=ttl)
    logger.info(f'pizza type {pizza_type} cached in redis. result: {result}')
    return result

def search_pizza_type(pizza_type: str):
    result = r.get(name=pizza_type)
    logger.info(f'get pizza type {pizza_type}: {result}.')
    return result