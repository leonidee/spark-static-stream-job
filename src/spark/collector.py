from datetime import date
from os import getenv

import pyspark.sql.functions as F
import pyspark.sql.types as T
import yaml
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from src.logger import LogManager
from src.spark.spark import SparkInitializer

log = LogManager().get_logger(name=__name__)


class StreamCollector(SparkInitializer):
    __slots__ = "spark"

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

    def _get_clients_locations_stream(self, topic: str) -> DataFrame:
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
            F.col("value.timestamp").alias("client_device_timestamp"),
            F.col("value.lat").alias("client_point_lat"),
            F.col("value.lon").alias("client_point_lon"),
        ).withColumn(
            "client_device_timestamp",
            F.from_unixtime(
                F.col("client_device_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"
            ).cast(T.TimestampType()),
        )

        # Drops duplicated messages. Vary important step to get rid of messages that was
        # sent by user's device many times into topic
        df = df.dropDuplicates(["client_id", "client_device_timestamp"]).withWatermark(
            eventTime="client_device_timestamp", delayThreshold="1 minute"
        )

        return df

    def get_stream(
        self,
        adv_campaign_data_path: str,
        adv_campaign_update_dt: date,
        input_topic: str,
        output_topic: str,
    ) -> DataStreamWriter:
        user_locations_stream = self._get_clients_locations_stream(topic=input_topic)

        adv_campaign_frame = self._get_adv_campaign_data(
            path_to_data=f"{adv_campaign_data_path}/date={adv_campaign_update_dt.strftime(r'%Y%m%d')}"
        )

        df = user_locations_stream.join(adv_campaign_frame, how="cross")

        return (
            df.select(
                "client_id",
                "timestamp",
                "adv_campaign_id",
                "adv_campaign_name",
                "adv_campaign_start_time",
                "adv_campaign_end_time",
            )
            .writeStream.format("console")
            .option("truncate", False)
            .outputMode("append")
            .trigger(processingTime="5 seconds")
        )


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
