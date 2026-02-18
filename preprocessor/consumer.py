from confluent_kafka import Consumer
import json
from producer import flush_message
from os import getenv


KAFKA_TOPIC = getenv('KAFKA_TOPIC', 'pizza-orders')
KAFKA_GROUP_ID = getenv('KAFKA_GROUP_ID', 'prep-team')
KAFKA_URI = getenv('KAFKA_URI', 'localhost:9092')

consumer_config = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

prep_path = './pizza_prep.json'

with open(prep_path, 'r') as file:
    PIZZA_PREP_DATA = json.load(file)


def remove_punctuation_translate(input_string: str):
    cleaned_string = ''.join(c for c in input_string.lower() if c.islower() or c == ' ' or c.isdigit()).upper()
    return cleaned_string


def text_analysis(order_instructions: str):
    allergies = ["gluten ", "peanut ", "allergy"]

    for word in allergies:
        if word in order_instructions.lower():
            cleaned_text = remove_punctuation_translate(order_instructions)
            upper_instructions = cleaned_text.upper()
            return upper_instructions

    return False


def search_prep(pizza_type: str):
    instructions = PIZZA_PREP_DATA.get(pizza_type)
    if instructions:
        return instructions
    return False


def listener():
    while True:
        try:
            order = consumer.poll(1.0)

            if order is None:
                continue


            if order.error():
                continue

            order_value = json.loads(order.value().decode('utf-8'))

            analysis = text_analysis(order_value['special_instructions'])

            message = {"_id" : order_value["_id"], "pizza_type": order_value["pizza_type"]}
            if analysis:
                message['protocol_cleaned'] = analysis

            pizza_prep = search_prep(order_value['pizza_type'])
            if not pizza_prep:
                continue
            cleaned_prep = remove_punctuation_translate(pizza_prep)

            message['pizza_prep'] = cleaned_prep.upper()
            flush_message(message)

        except Exception as e:
            print(f'Error: {e}')
            continue


listener()