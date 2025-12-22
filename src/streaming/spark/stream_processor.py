from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType
)
import yaml


class TransactionStreamProcessor:

    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            spark_cfg = yaml.safe_load(f)["spark"]

        self.spark = (
            SparkSession.builder
            .appName(spark_cfg["app_name"])
            .master(spark_cfg["master"])
            .getOrCreate()
        )

        self.bootstrap_servers = spark_cfg["kafka_bootstrap_servers"]
        self.topic = spark_cfg["topic"]
        self.checkpoint_location = spark_cfg["checkpoint_location"]

    @staticmethod
    def transaction_schema() -> StructType:
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("wallet_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_category", StringType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("ip_address", StringType(), False),
            StructField("country", StringType(), False),
            StructField("is_international", BooleanType(), False),
            StructField("event_timestamp", TimestampType(), False),
        ])

    def read_stream(self):
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )

    def process(self):
        raw_stream = self.read_stream()

        parsed_stream = (
            raw_stream
            .selectExpr("CAST(value AS STRING) as json")
            .select(
                from_json(col("json"), self.transaction_schema()).alias("data")
            )
            .select("data.*")
        )

        enriched_stream = (
            parsed_stream
            .withWatermark("event_timestamp", "5 minutes")
            .groupBy(
                col("user_id"),
                window(col("event_timestamp"), "5 minutes")
            )
            .agg(
                count("*").alias("txn_count_5min"),
                avg("amount").alias("avg_amount_5min")
            )
        )

        query = (
            enriched_stream
            .writeStream
            .outputMode("update")
            .format("console")
            .option("checkpointLocation", self.checkpoint_location)
            .start()
        )

        query.awaitTermination()


if __name__ == "__main__":
    processor = TransactionStreamProcessor("config/spark.yaml")
    processor.process()