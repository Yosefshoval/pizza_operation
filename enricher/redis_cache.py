import json
from os import getenv
from redis import Redis

REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PORT = getenv('REDIS_PORT', 6379)
r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
