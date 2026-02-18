from confluent_kafka import Producer
from os import getenv


KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')
NEW_TOPIC = 'cleaned-instructions'


producer_config = {'bootstrap.servers' : KAFKA_URI}
producer = Producer(producer_config)


def callback(err, msg):
    if err:
        print(f'Error while trying to send the message: {err}')
        raise KafkaException(err)
    else:
        print(f'message: {msg.value().decode("utf-8")}')


def flush_message(message: dict):
    if "_id" in message: message["_id"] = str(message["_id"])
    value = json.dumps(message).encode('utf-8')
    producer.produce(
        topic=NEW_TOPIC,
        value=value,
        callback=callback
    )

    producer.flush()