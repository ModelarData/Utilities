import time
import json
import random
import threading
from abc import ABC, abstractmethod
import paho.mqtt.client as mqtt


class Topic(ABC):
    def __init__(self, broker_url, broker_port, topic_url, topic_data, retain_probability):
        self.broker_url = broker_url
        self.broker_port = broker_port
        self.topic_url = topic_url
        self.topic_data = topic_data
        self.retain_probability = retain_probability
        self.client = None

    def connect(self):
        self.client = mqtt.Client(self.topic_url, clean_session=True, transport='tcp')
        self.client.on_publish = self.on_publish
        self.client.connect(self.broker_url, self.broker_port)
        self.client.loop_start()

    @abstractmethod
    def run(self):
        pass

    def disconnect(self):
        self.client.loop_end()
        self.client.disconnect()

    def on_publish(self, client, userdata, result):
        print(f'[{time.strftime("%H:%M:%S")}] Data published on: {self.topic_url}')


class TopicAuto(Topic, threading.Thread):
    def __init__(self, broker_url, broker_port, topic_url, topic_data, retain_probability, time_interval):
        Topic.__init__(self, broker_url, broker_port, topic_url, topic_data, retain_probability)
        threading.Thread.__init__(self, args=(), kwargs=None)
        self.time_interval = time_interval
        self.old_payload = None

    def run(self):
        self.connect()
        while True:
            payload = self.generate_data()
            self.old_payload = payload
            self.client.publish(topic=self.topic_url, payload=json.dumps(list(payload.values())), qos=1, retain=False)
            time.sleep(self.time_interval)

    def generate_data(self):
        payload = {}

        timestamp_milliseconds = int(time.time() * 1000)

        if self.old_payload is None:
            # Generate initial data.
            for data in self.topic_data:
                if data['TYPE'] == 'timestamp':
                    payload[data['NAME']] = timestamp_milliseconds
                if data['TYPE'] == 'int':
                    payload[data['NAME']] = round(random.randint(data['MIN_VALUE'], data['MAX_VALUE']), 2)
                elif data['TYPE'] == 'float':
                    payload[data['NAME']] = round(random.uniform(data['MIN_VALUE'], data['MAX_VALUE']), 2)
                elif data['TYPE'] == 'bool':
                    payload[data['NAME']] = random.choice([True, False])
        else:
            # Generate next data.
            payload = self.old_payload
            for data in self.topic_data:
                if random.random() > (1 - self.retain_probability):
                    continue
                if data['TYPE'] == 'bool':
                    payload[data['NAME']] = not payload[data['NAME']]
                if data['TYPE'] == 'timestamp':
                    payload[data['NAME']] = timestamp_milliseconds
                else:
                    step = random.uniform(-data['MAX_STEP'], data['MAX_STEP'])
                    step = round(step) if data['TYPE'] == 'int' else step

                    if step < 0:
                        payload[data['NAME']] = round(max(payload[data['NAME']] + step, data['MIN_VALUE']), 2)
                    else:
                        payload[data['NAME']] = round(min(payload[data['NAME']] + step, data['MAX_VALUE']), 2)

        return payload
