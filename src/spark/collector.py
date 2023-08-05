from datetime import date, datetime
from os import getenv

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from src.logger import LogManager
from src.spark.spark import SparkInitializer

log = LogManager().get_logger(name=__name__)


class StreamCollector(SparkInitializer):
    __slots__ = ("spark", "is_debug", "app_name")

    def __init__(self, app_name: str, is_debug: bool = False) -> None:
        super().__init__(app_name)
        self.is_debug = is_debug
        self.app_name = app_name

    def get_actual_adv_campaign_data(self, path_to_data: str) -> DataFrame:
        """ """
        log.debug(f"Getting actual advetisment data from -> {path_to_data}")
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

    def get_clients_locations_stream(self, topic: str) -> DataFrame:
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

    def get_stream(
        self,
        clients_locations_topic: str,
        adv_campaign_data_path: str,
        adv_campaign_update_dt: date,
    ) -> DataStreamWriter:
        def foreachbatch_func(frame: DataFrame, batch_id: int) -> DataFrame:
            adv_campaign_frame = self.get_actual_adv_campaign_data(
                path_to_data=f"{adv_campaign_data_path}/date={adv_campaign_update_dt.strftime(r'%Y%m%d')}"
            )

            frame = frame.join(adv_campaign_frame, how="cross")

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
                .withColumn("distance", F.col("distance").cast(T.IntegerType()))
                .drop("calc")
            )

            # frame = frame.where(F.col("distance") <= 2_000)

        clients_locations_stream = self.get_clients_locations_stream(
            topic=clients_locations_topic
        )

        # df = (
        #     df.where(F.col("distance") <= 2_000)
        #     .dropDuplicates(["client_id", "adv_campaign_id"])
        #     .withWatermark("client_device_timestamp", "1 minute")
        #     .select(
        #         "client_id",
        #         "distance",
        #         "adv_campaign_id",
        #         "adv_campaign_name",
        #         "adv_campaign_provider_id",
        #         "adv_campaign_provider_name",
        #         "adv_campaign_description",
        #         "adv_campaign_start_time",
        #         "adv_campaign_end_time",
        #     )
        # )
        if self.is_debug:
            return (
                clients_locations_stream.writeStream.format("console")
                .outputMode("append")
                .trigger(processingTime="60 seconds")
                .options(
                    checkpointLocation=f"/app/checkpoints/{self.app_name}",
                    truncate=True,
                )
                .foreachBatch(foreachbatch_func)
            )
        else:
            ...


class StaticCollector(SparkInitializer):
    def __init__(self, app_name: str) -> None:
        super().__init__(app_name)

    def _get_adv_campaign_data(self, path_to_data: str) -> DataFrame:
        """
        ## Examples
        >>> df = spark._get_marketing_data(path_to_data='some/s3/like/path')
        >>> df.show(10)
        +--------------------+-----------------+------------------------+------------------------+--------------------------+-----------------------+---------------------+----------------------+----------------------+
        |     adv_campaign_id|adv_campaign_name|adv_campaign_description|adv_campaign_provider_id|adv_campaign_provider_name|adv_campaign_start_time|adv_campaign_end_time|adv_campaign_point_lat|adv_campaign_point_lon|
        +--------------------+-----------------+------------------------+------------------------+--------------------------+-----------------------+---------------------+----------------------+----------------------+
        |f590a6cc-2ec4-11e...|    Morning Boost|    Jump-start your m...|    f590a456-2ec4-11e...|           The Daily Grind|    2023-07-30 10:00:00|  2023-07-30 12:00:00|      55.8790015313034|       37.714565000436|
        |f590a924-2ec4-11e...|   Cup and Canvas|    Spend a soothing ...|    f590a5e6-2ec4-11e...|            Beanstalk Cafe|    2023-07-30 12:30:00|  2023-07-30 14:30:00|      55.8790015313034|       37.714565000436|
        |f590a9f6-2ec4-11e...|     Loyalty Love|    Be a part of our ...|    f590a65e-2ec4-11e...|            Coffee Harmony|    2023-07-30 13:00:00|  2023-07-30 15:00:00|      55.8790015313034|       37.714565000436|
        +--------------------+-----------------+------------------------+------------------------+--------------------------+-----------------------+---------------------+----------------------+----------------------+

        >>> df.printSchema()
        root
        |-- adv_campaign_id: string (nullable = true)
        |-- adv_campaign_name: string (nullable = true)
        |-- adv_campaign_description: string (nullable = true)
        |-- adv_campaign_provider_id: string (nullable = true)
        |-- adv_campaign_provider_name: string (nullable = true)
        |-- adv_campaign_start_time: timestamp (nullable = true)
        |-- adv_campaign_end_time: timestamp (nullable = true)
        |-- adv_campaign_point_lat: double (nullable = true)
        |-- adv_campaign_point_lon: double (nullable = true)
        """
        return self.spark.read.parquet(
            path_to_data,
        ).select(
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

    def persist_clients_locations_stream(self, topic: str) -> ...:
        schema = T.StructType(
            [
                T.StructField("client_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("lat", T.DoubleType(), True),
                T.StructField("lon", T.DoubleType(), True),
            ]
        )

        df = (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVER"))
            .option("startingOffsets", "earliest")
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
            F.col("value.timestamp").alias("client_device_timestamp"),
            F.col("value.lat").alias("client_point_lat"),
            F.col("value.lon").alias("client_point_lon"),
        ).withColumn(
            "client_device_timestamp",
            F.from_unixtime(
                F.col("client_device_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
            ).cast(T.TimestampType()),
        )

        df = df.dropDuplicates(["client_id", "client_device_timestamp"]).withWatermark(
            eventTime="client_device_timestamp", delayThreshold="1 minute"
        )

        adv_campaign_frame = self._get_adv_campaign_data(
            path_to_data="s3a://data-ice-lake-05/master/data/source/spark-static-stream/adv-campaign-data"
        )

        df = df.join(adv_campaign_frame, how="cross")

        df = (
            df.withColumn(
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
                (F.atan2(F.sqrt(F.col("calc")), F.sqrt(-F.col("calc") + 1)) * 12742000),
            )
            .withColumn("distance", F.col("distance").cast(T.IntegerType()))
            .drop("calc")
        )

        df = (
            df.where(F.col("distance") <= 1000)
            .dropDuplicates(["client_id", "adv_campaign_id"])
            .withWatermark("client_device_timestamp", "1 minute")
            .select(
                "client_id",
                "distance",
                "adv_campaign_id",
                "adv_campaign_name",
                "adv_campaign_provider_id",
                "adv_campaign_provider_name",
                "adv_campaign_description",
                "adv_campaign_start_time",
                "adv_campaign_end_time",
            )
        )

        df.show(100)

        # df.groupBy("client_id").agg(
        #     F.count(F.col("timestamp")).alias("events_cnt")
        # ).orderBy(F.desc(F.col("events_cnt"))).show(200, False)

        # df.where(F.col("client_id") == "03b20ae0-2ed7-11ee-a4fd-0242ac120009").show(300)

        df.orderBy(F.asc(F.col("distance"))).show(100)

        # df.where(F.col("client_id") == "05d8e1cc-2ed7-11ee-a4fd-0242ac120009").select(
        #     "timestamp"
        # ).distinct().show(100)
        # print(df.count())
