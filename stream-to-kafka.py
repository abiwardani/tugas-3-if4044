import requests
import json

from kafka import KafkaProducer

URL = "http://128.199.176.197:7551/streaming"
USERNAME = "a57de080-f7bc-4022-93dc-612d2af58d31"

TOPIC = "json-social-media"
producer = KafkaProducer(bootstrap_servers="localhost:9092")


def stream1():
    s = requests.Session()
    s.auth = (USERNAME, "")

    with s.get(URL, headers=None, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                print(line)


def stream2():
    s = requests.Session()
    s.auth = (USERNAME, "")

    resp = s.get(URL, stream=True)
    for chunk in resp.iter_content(chunk_size=None):
        yield chunk


if __name__ == "__main__":
    for raw in stream2():
        try:
            data = json.loads(raw)
            producer.send(TOPIC, raw)
            # with open(f"./json_dump/data-{count_doc}.json", "w") as f:
            #     json.dump(data, f)
            # count_doc += 1
        except ValueError as e:
            print(e)
            continue
