from datetime import date, datetime
from os import getenv

import pyspark  # type: ignore
import pyspark.sql.functions as F  # type: ignore
import pyspark.sql.types as T  # type: ignore

from src.logger import LogManager
from src.spark.spark import SparkInitializer

log = LogManager().get_logger(name=__name__)


class StreamCollector(SparkInitializer):
    __slots__ = ("spark", "is_debug", "app_name")

    def __init__(self, app_name: str, is_debug: bool = False) -> None:
        super().__init__(app_name)
        self.is_debug = is_debug
        self.app_name = app_name

    def get_actual_adv_campaign_data(self, path_to_data: str) -> pyspark.sql.DataFrame:
        """ """
        log.debug(f"Getting actual advertisment data from -> {path_to_data}")
        CURRENT_TIMESTAMP = datetime.now().timestamp()

        return (
            self.spark.read.parquet(
                path_to_data,
            )
            .where(
                (F.col("start_time") <= CURRENT_TIMESTAMP)
                & (F.col("end_time") >= CURRENT_TIMESTAMP)
            )
            .select(
                F.col("id").alias("adv_campaign_id"),
                F.col("name").alias("adv_campaign_name"),
                F.col("description").alias("adv_campaign_description"),
                F.col("provider_id").alias("adv_campaign_provider_id"),
                F.col("provider_name").alias("adv_campaign_provider_name"),
                F.col("start_time").alias("adv_campaign_start_time"),
                F.col("end_time").alias("adv_campaign_end_time"),
                F.col("point_lat").alias("adv_campaign_point_lat"),
                F.col("point_lon").alias("adv_campaign_point_lon"),
            )
        )

    def get_clients_locations_stream(self, topic: str) -> pyspark.sql.DataFrame:
        options: dict = {
            "kafka.bootstrap.servers": getenv("KAFKA_BOOTSTRAP_SERVER"),
            "failOnDataLoss": False,
            "startingOffsets": "latest",
            "subscribe": topic,
        }

        df = (
            self.spark.readStream.format("kafka")
            .options(**options)
            .load()
            .select(
                F.col("key").cast(T.StringType()),
                F.col("value").cast(T.StringType()),
            )
        )

        value_schema = T.StructType(
            [
                T.StructField("timestamp", T.DoubleType(), False),
                T.StructField("lat", T.DoubleType(), False),
                T.StructField("lon", T.DoubleType(), False),
            ]
        )

        df = df.withColumns(
            {
                "value": F.from_json(col=F.col("value"), schema=value_schema),
            }
        )

        df = df.select(
            F.col("key").alias("client_id"),
            F.col("value.timestamp").alias("client_device_timestamp"),
            F.col("value.lat").alias("client_point_lat"),
            F.col("value.lon").alias("client_point_lon"),
        ).withColumn(
            "client_device_timestamp",
            F.from_unixtime(
                F.col("client_device_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
            ).cast(T.TimestampType()),
        )

        # Drops duplicated messages. Very important step to get rid of messages that was
        # sent by user's device twice or even many times
        df = df.dropDuplicates(["client_id", "client_device_timestamp"]).withWatermark(
            eventTime="client_device_timestamp", delayThreshold="1 minute"
        )

        return df

    def prepare_for_kafka(self, frame: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return frame.withColumns(
            {
                "key": F.to_json(F.struct(F.col("client_id"))),
                "value": F.to_json(
                    F.struct(
                        F.col("client_distance"),
                        F.col("adv_campaign_id"),
                        F.col("adv_campaign_name"),
                        F.col("adv_campaign_provider_id"),
                        F.col("adv_campaign_provider_name"),
                        F.col("adv_campaign_description"),
                        F.col("adv_campaign_start_time"),
                        F.col("adv_campaign_end_time"),
                        F.col("trigger_timestamp"),
                    )
                ),
            }
        ).select("key", "value")

    def get_streaming_query(
        self,
        clients_locations_topic: str,
        clients_push_topic: str,
        adv_campaign_data_path: str,
        adv_campaign_update_dt: date,
        query_name: str,
        checkpoints_location: str,
    ) -> pyspark.sql.streaming.DataStreamWriter:
        adv_campaign_frame = self.get_actual_adv_campaign_data(
            path_to_data=f"{adv_campaign_data_path}/date={adv_campaign_update_dt.strftime(r'%Y%m%d')}"
        )

        clients_locations_stream = self.get_clients_locations_stream(
            topic=clients_locations_topic
        )

        frame = clients_locations_stream.join(adv_campaign_frame, how="cross")

        frame = (
            frame.withColumn(
                "calc",
                (
                    F.pow(
                        F.sin(
                            F.radians(
                                F.col("adv_campaign_point_lat")
                                - F.col("client_point_lat")
                            )
                            / 2
                        ),
                        2,
                    )
                    + F.cos(F.radians(F.col("client_point_lat")))
                    * F.cos(F.radians(F.col("adv_campaign_point_lat")))
                    * F.pow(
                        F.sin(
                            F.radians(
                                F.col("adv_campaign_point_lon")
                                - F.col("client_point_lon")
                            )
                            / 2
                        ),
                        2,
                    )
                ),
            )
            .withColumn(
                "distance",
                (
                    F.atan2(F.sqrt(F.col("calc")), F.sqrt(-F.col("calc") + 1))
                    * 12_742_000
                ),
            )
            .withColumn("client_distance", F.col("distance").cast(T.IntegerType()))
            .drop("calc", "distance")
        )

        frame = (
            frame.where(F.col("client_distance") <= 2_000)
            .withColumns(
                {
                    "trigger_timestamp": F.lit(datetime.now().timestamp()),
                    "trigger_time": F.from_unixtime(
                        F.col("trigger_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
                    ).cast(T.TimestampType()),
                }
            )
            .dropDuplicates(["client_id", "adv_campaign_id"])
            .withWatermark("trigger_time", "1 minute")
            .select(
                "client_id",
                "client_distance",
                "adv_campaign_id",
                "adv_campaign_name",
                "adv_campaign_provider_id",
                "adv_campaign_provider_name",
                "adv_campaign_description",
                "adv_campaign_start_time",
                "adv_campaign_end_time",
                "trigger_timestamp",
            )
        )

        if self.is_debug:
            return (
                frame.writeStream.format("console")
                .queryName(query_name)
                .outputMode("append")
                .trigger(processingTime="30 seconds")
                .options(
                    checkpointLocation=f"{checkpoints_location}/{self.app_name}/{query_name}",
                    truncate=False,
                )
                .start()
            )
        else:
            frame = self.prepare_for_kafka(frame)

            return (
                frame.writeStream.format("kafka")
                .queryName(query_name)
                .outputMode("append")
                .trigger(processingTime="30 seconds")
                .options(
                    checkpointLocation=f"{checkpoints_location}/{self.app_name}/{query_name}",
                    topic=clients_push_topic,
                )
                .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVER"))
                .start()
            )
