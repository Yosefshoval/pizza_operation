from os import getenv
from confluent_kafka import Producer, KafkaException
import json

KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
print(KAFKA_URI)
KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'pizza-orders')
print(KAFKA_TOPIC)

producer_config = {'bootstrap.servers' : KAFKA_URI}
producer = Producer(producer_config)


def callback(err, msg):
    if err:
        print(f'Error while trying to send the message: {err}')
        raise KafkaException(err)
    else:
        print(f'message: {msg.value().decode("utf-8")}')


def publish_message(message: dict):
    if "_id" in message: message["_id"] = str(message["_id"])
    value = json.dumps(message).encode('utf-8')
    producer.produce(
        topic=KAFKA_TOPIC,
        value=value,
        callback=callback
    )

    producer.flush()
