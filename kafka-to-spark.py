import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

from utils import (
    facebook_date_to_YYYYMMDD_HHMMSS,
    round_time_five_minute,
    timestamp_to_YYYYMMDD_HHMMSS,
    twitter_date_to_YYYYMMDD_HHMMSS,
    youtube_date_to_YYYYMMDD_HHMMSS,
)

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "json-social-media"

sc = SparkContext()


def run_spark():
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("./checkpoint")
    lines = KafkaUtils.createDirectStream(
        ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER}
    )

    def process_stream_data(lines, window_length=2, sliding_interval=2):
        def preprocess_json(x):
            data = json.loads(x[1])
            socmed_type = data["crawler_target"]["specific_resource_type"]

            timestamp = "null"
            user = "null"

            if socmed_type == "instagram":
                timestamp_data = data["created_time"]
                timestamp = timestamp_to_YYYYMMDD_HHMMSS(timestamp_data)
                user_id = data["user"]["id"]
            elif socmed_type == "youtube":
                timestamp_data = data["snippet"]["publishedAt"]
                timestamp = youtube_date_to_YYYYMMDD_HHMMSS(timestamp_data)
                user_id = data["snippet"].get("channelId", "unknown")
            elif socmed_type == "facebook":
                timestamp_data = data["created_time"]
                timestamp = facebook_date_to_YYYYMMDD_HHMMSS(timestamp_data)
                user_id = data["from"]["id"]
            elif socmed_type == "twitter":
                timestamp_data = data["created_at"]
                timestamp = twitter_date_to_YYYYMMDD_HHMMSS(timestamp_data)
                user_id = data["user_id"]

            rounded_timestamp = round_time_five_minute(timestamp)

            return (socmed_type, timestamp, rounded_timestamp, user_id, 1)

        result = lines.window(window_length, sliding_interval)
        result = result.map(preprocess_json)

        return result

    # run the function
    result = process_stream_data(lines)

    result.pprint()
    ssc.start()
    ssc.awaitTermination()


def produce_msg():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
    for _ in range(20):
        try:
            producer.send("foobar", b"yahh")
            print("sent")
        except ValueError as e:
            print(e)


def consume_msg():
    topic = "foobar"
    consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVER)

    partitions = consumer.partitions_for_topic(topic)
    for p in partitions:
        topic_partition = TopicPartition(topic, p)
        consumer.seek(partition=topic_partition, offset=0)
        for msg in consumer:
            print(msg.value.decode("utf-8"))


def consume_json():
    topic = "json-social-media"
    consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVER)

    partitions = consumer.partitions_for_topic(topic)
    for p in partitions:
        topic_partition = TopicPartition(topic, p)
        consumer.seek(partition=topic_partition, offset=0)
        for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            print(data)
            print("-------------------------")
            print("-------------------------")
            print("-------------------------")
            print("-------------------------")


if __name__ == "__main__":
    # produce_msg()
    # consume_json()
    run_spark()
