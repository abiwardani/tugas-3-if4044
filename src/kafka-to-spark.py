from datetime import datetime
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import sql


from utils.date import (
    facebook_date_to_YYYYMMDD_HHMMSS,
    round_time_five_minute,
    timestamp_to_YYYYMMDD_HHMMSS,
    twitter_date_to_YYYYMMDD_HHMMSS,
    youtube_date_to_YYYYMMDD_HHMMSS,
)

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "json-social-media"

conf = SparkConf().setAppName("spark-social-media").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)
# spark = SparkSession.builder.appName("spark-social-media").getOrCreate()

# sc = SparkContext()


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
            user_id = "null"

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

            return (socmed_type, rounded_timestamp, user_id, 1)

        def aggreggate(x):
            pass

        result = lines.window(window_length, sliding_interval)
        result = result.map(preprocess_json)

        return result

    # run the function
    result = process_stream_data(lines)

    HOST = "localhost"
    PORT = 5432
    DB = "tbd_medsos"
    TABLE = "socmed_2"
    USER = "medsos"
    PASSWORD = "123456"
    URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"

    def saveToDB(rdd):
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            TimestampType,
            IntegerType,
        )

        socmed2Schema = StructType(
            [
                StructField("social_media", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("user_id", StringType(), True),
                StructField("test", IntegerType(), True),
            ]
        )
        if not rdd.isEmpty():
            # df = sqlContext.createDataFrame(rdd, schema=socmed2Schema)
            # df = rdd.toDF(["social_media", "timestamp", "user_id", "test"])
            # datetime.strptime('2020-08-20 10:00:00', '%Y-%m-%d %H:%M:%S')
            rdd = rdd.map(
                lambda x: (
                    x[0],
                    datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S"),
                    x[2],
                    x[3],
                )
            )
            df = rdd.toDF(socmed2Schema)
            df.show(5)
            df.select("social_media", "timestamp", "user_id", "test").write.format(
                "jdbc"
            ).mode("append").option("url", URL).option(
                "driver", "org.postgresql.Driver"
            ).option(
                "dbtable", TABLE
            ).option(
                "user", USER
            ).option(
                "password", PASSWORD
            ).save()

    result.foreachRDD(saveToDB)

    # result.pprint()
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
