from confluent_kafka import Consumer
from os import getenv


KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'enricher-team')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'cleaned-instructions')


consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])


"""
אלרגנים
forbidden_non_kosher
meat_ingredients
dairy_ingredients

is_meat
is_dairy
is_kosher

Dairy אין → VEGAN
Gluten אין → GLUTEN-FREE

status = BURNT
"""