from pyspark.sql import SparkSession
from pyspark.sql import Row

HOST = "localhost"
PORT = 5432
DB = "tbd_medsos"
TABLE = "socmed"
USER = "medsos"
PASSWORD = "123456"


def set_database():
    postgres = (
        SparkSession.builder.config(
            "spark.executor.extraClassPath", "~/Downloads/postgresql-42.6.0.jar"
        )
        .config("spark.driver.extraClassPath", "~/Downloads/postgresql-42.6.0.jar")
        .config("spark.executor.memory", "1g")
        .appName("dataframe-spark")
        .getOrCreate()
    )
    df = postgres.write.jdbc(
        url=f"jdbc:postgresql://{HOST}:{PORT}/{DB}",
        table=TABLE,
        mode="append",
        properties={
            "user": USER,
            "password": PASSWORD,
            "driver": "org.postgresql.Driver",
        },
    )

    # df = (
    #     postgres.read.format("jdbc")
    #     .option("driver", "org.postgresql.Driver")
    #     .option("url", f"jdbc:postgresql://{HOST}:{PORT}/{DB}")
    #     .option("table", TABLE)
    #     .option("dbtable", _select_sql)
    #     .option("user", USER)
    #     .option("password", PASSWORD)
    #     .load()
    # )


if __name__ == "__main__":
    set_database()
