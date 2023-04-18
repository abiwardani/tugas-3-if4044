import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "json-social-media"


def run_spark():
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("./checkpoint")
    lines = KafkaUtils.createDirectStream(
        ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER}
    )

    def process_stream_data(lines, window_length=2, sliding_interval=2):
        def updateStatKey(newData, runningCount):
            if runningCount is None:
                runningCount = 0
            return sum(newData, runningCount)

        def convert_line_to_stat_data(x):
            data_int = list(map(int, x[1].split()))
            return [
                ("sum_x_square:", sum(list(map(lambda x: x * x, data_int)))),
                ("sum_x:", sum(data_int)),
                ("n_data:", len(data_int)),
            ]

        result = lines.window(window_length, sliding_interval)
        result = result.flatMap(convert_line_to_stat_data)
        result = result.updateStateByKey(updateStatKey)

        def compute_variance(data):
            sum_x_square, sum_x, n_data = data[1], data[3], data[5]
            variance = (sum_x_square / n_data) - ((sum_x / n_data) ** 2)
            return (
                "sum_x_square:",
                sum_x_square,
                "sum_x:",
                sum_x,
                "n_data:",
                n_data,
                "var:",
                variance,
            )

        result = result.reduce(lambda x, y: (*x, y[0], y[1]))
        result = result.map(compute_variance)
        return result

    # run the function
    result = process_stream_data(lines, window_length=2, sliding_interval=2)

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
    consume_json()
