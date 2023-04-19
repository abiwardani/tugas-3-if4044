import json

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext

from pyspark import SparkConf
from pyspark import sql

from utils.date import (
    facebook_date_to_YYYYMMDD_HHMMSS,
    round_time_five_minute,
    timestamp_to_YYYYMMDD_HHMMSS,
    twitter_date_to_YYYYMMDD_HHMMSS,
    youtube_date_to_YYYYMMDD_HHMMSS,
)

from utils.db import process_partition

BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "json-social-media"

conf = SparkConf().setAppName("spark-social-media").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)


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

        result = lines.window(window_length, sliding_interval)
        result = result.map(preprocess_json)
        result = result.map(
            lambda x: ((x[0], x[1], x[2]), x[3])
        )  # key: triple of (social_media, timestamp, id), value = count
        result = result.reduceByKey(lambda x, y: x + y)
        result = result.map(lambda x: ((x[0][0], x[0][1]), (1, x[1])))
        result = result.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        result = result.map(
            lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])
        )  # (social_media, timestamp, unique_count, count)

        return result

    # run the function
    result = process_stream_data(lines)
    result.pprint()

    def saveToDB(rdd):
        if not rdd.isEmpty():
            rdd.foreachPartition(process_partition)

    result.foreachRDD(saveToDB)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    run_spark()
