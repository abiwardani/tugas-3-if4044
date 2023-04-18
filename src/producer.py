import requests
from json import loads, dumps
from kafka import KafkaProducer

def get_stream(url, prev=None):
    s = requests.Session()
    out = ""
    i = 0
    thresh = 5
    if prev is None:
        prev = ["" for _ in range(thresh)]
    curr = ["" for _ in range(thresh)]
    match = True

    try:
        with s.get(url, headers=None, auth=('a57de080-f7bc-4022-93dc-612d2af58d31', ''), stream=True, timeout=10) as resp:
            for line in resp.iter_lines():
                if line:
                    linestr = line.decode('utf-8')
                    out += linestr.strip()

                    if i < thresh:
                        curr[i] = linestr
                        match = match and (prev[i] == linestr)
                    if match and i >= thresh:
                        return [], curr
                    
                    i += 1

    except:
        # print("Connection timed out.")
        pass

    out += "}]"
    data = loads(out)

    return data, curr

producer = KafkaProducer(bootstep_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

url = "http://128.199.176.197:7551/streaming"
prev = None

while True:
    data, prev = get_stream(url, prev)

    if len(data) != 0:
        print(data[:5])

        for row in data:
            producer.send('my_stream', value=row)