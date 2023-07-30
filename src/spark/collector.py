from os import getenv

import pyspark.sql.functions as F
import pyspark.sql.types as T
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from src.logger import LogManager
from src.spark.spark import SparkInitializer

log = LogManager().get_logger(name=__name__)


class StreamCollector(SparkInitializer):
    __slots__ = "spark"

    def __init__(self, app_name: str) -> None:
        super().__init__(app_name)

    def get_marketing_data(self, path_to_data: str) -> DataFrame:
        df = self.spark.read.csv(
            path_to_data,
            inferSchema=True,
            header=True,
        )

        return df.select(
            F.col("id").alias("adv_campaign_id"),
            F.col("name").alias("adv_campaign_name"),
            F.col("description").alias("adv_campaign_description"),
            F.col("start_time").alias("adv_campaign_start_time"),
            F.col("end_time").alias("adv_campaign_end_time"),
            F.col("point_lat").alias("adv_campaign_point_lat"),
            F.col("point_lon").alias("adv_campaign_point_lon"),
        )

    def get_clients_locations_stream(self, topic: str) -> DataFrame:
        schema = T.StructType(
            [
                T.StructField("client_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("lat", T.DoubleType(), True),
                T.StructField("lon", T.DoubleType(), True),
            ]
        )

        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVER"))
            .option("failOnDataLoss", False)
            .option("startingOffsets", "latest")
            .option("subscribe", topic)
            .load()
            .select(
                F.col("key").cast(T.StringType()),
                F.col("value").cast(T.StringType()),
                "topic",
                "partition",
                "offset",
                "timestamp",
                "timestampType",
            )
            .withColumn("value", F.from_json(col=F.col("value"), schema=schema))
        )

        df = df.select(
            F.col("value.client_id").alias("client_id"),
            F.col("value.timestamp").alias("timestamp"),
            F.col("value.lat").alias("client_lat"),
            F.col("value.lon").alias("client_lon"),
        ).withColumn(
            "timestamp",
            F.from_unixtime(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").cast(
                T.TimestampType()
            ),
        )

        return df

    def get_stream(
        self,
        marketing_data_path: str,
        input_topic: str,
        output_topic: str,
    ) -> DataStreamWriter:
        stream = self.get_clients_locations_stream(topic=input_topic)

        df = self.get_marketing_data(path_to_data=marketing_data_path)

        df = stream.join(df, how="cross")

        return (
            df.writeStream.format("console")
            .option("truncate", False)
            .outputMode("append")
            .trigger(processingTime="5 seconds")
        )
