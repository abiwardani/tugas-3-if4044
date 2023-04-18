from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils

KAFKA_TOPIC = "variance"
BOOTSTRAP_SERVER = "localhost:9092"


sc = SparkContext()

ssc = StreamingContext(sc, 1)  # stream each one second
ssc.checkpoint("./checkpoint")
lines = KafkaUtils.createDirectStream(
    ssc, [KAFKA_TOPIC], {"metadata.broker.list": BOOTSTRAP_SERVER}
)


def calculate_variance(lines, window_length=2, sliding_interval=2):
    """
    Function to calculate "accumulated/global variance" in each window operation
    Params:
        lines: Spark DStream defined above (in this jupyter cell)
        window_length: length of window in windowing operation
        sliding_interval: sliding interval for the window operation
    Return:
        result: DStream (RDD) of variance result with
                format --> ('sum_x_square:', sum_x_square_value, 'sum_x:', sum_x_value, 'n:', n_value, 'var:', variance_value)
                Example:   ('sum_x_square:', 182.0, 'sum_x:', 42.0, 'n:', 12.0, 'var:', 2.916666666666666)
    """

    # Realize this function here. Note that you are not allowed to modify any code other than this function.
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
        sum_x, sum_x_square, n_data = data[1], data[3], data[5]
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
result = calculate_variance(lines, window_length=2, sliding_interval=2)
# Print
result.pprint()
ssc.start()
ssc.awaitTermination()
