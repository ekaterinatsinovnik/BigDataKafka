from confluent_kafka import Producer, Consumer
import json
import yaml
import sys
import time
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

def load_config(path):
    with open(path, "r") as file:
        config = yaml.safe_load(file)
    return config

class KafkaProducer:
    def __init__(self, topic, config):
        self.producer = Producer({'bootstrap.servers': config['bootstrap_servers']})
        self.topic = topic

    def send_message(self, event):
        time.sleep(1)
        self.producer.produce(self.topic, key="l", value=json.dumps(event))
        self.producer.flush()


class KafkaConsumer:
    def __init__(self, topic, config, group_id = ""):
        self.consumer = Consumer({
            'bootstrap.servers': config['bootstrap_servers'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.consumer.subscribe([topic])

    def consume_message(self):
        event = self.consumer.poll(1.0)
        if event is None:
            return None
        if event.error():
            print(f"Consumer error: {event.error()}")
            return None
        return json.loads(event.value().decode('utf-8'))
