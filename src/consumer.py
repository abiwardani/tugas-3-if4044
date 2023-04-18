from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('my_stream', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', value_deserializer=lambda x: (loads(x)))

for message in consumer:
    message = message.value
    print(message)