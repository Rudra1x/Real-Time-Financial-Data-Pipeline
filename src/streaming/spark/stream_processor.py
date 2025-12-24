from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("kafka_streaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .load()
    )

    out = df.selectExpr("CAST(value AS STRING)")

    query = (
        out.writeStream
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()