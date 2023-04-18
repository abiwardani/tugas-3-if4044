import requests
import json

from kafka import KafkaProducer

URL = "http://128.199.176.197:7551/streaming"
USERNAME = "a57de080-f7bc-4022-93dc-612d2af58d31"

TOPIC = "json-social-media"
producer = KafkaProducer(bootstrap_servers="localhost:9092")


def stream_api():
    s = requests.Session()
    s.auth = (USERNAME, "")

    resp = s.get(URL, stream=True)
    for chunk in resp.iter_content(chunk_size=None):
        yield chunk


if __name__ == "__main__":
    for raw in stream_api():
        try:
            data = json.loads(raw)
            producer.send(TOPIC, raw)
        except ValueError as e:
            continue
