import requests
import json

from kafka import KafkaProducer

STREAMING_URL = "http://128.199.176.197:7551/streaming"
USERNAME = "a57de080-f7bc-4022-93dc-612d2af58d31"

KAFKA_TOPIC = "json-social-media"
producer = KafkaProducer(bootstrap_servers="localhost:9092")


def stream_api():
    try:
        s = requests.Session()
        s.auth = (USERNAME, "")

        resp = s.get(STREAMING_URL, stream=True)
        for chunk in resp.iter_content(chunk_size=None):
            yield chunk
    except Exception as e:
        print(e)


def run():
    try:
        for raw in stream_api():
            try:
                json.loads(raw)
                producer.send(KAFKA_TOPIC, raw)
            except ValueError as e:
                continue
    except ConnectionResetError as e:
        print(e)
    finally:
        print("restarting connection...")
        run()


if __name__ == "__main__":
    run()
