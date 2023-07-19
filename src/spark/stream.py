# /usr/bin/env python3

import logging
import sys

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, CapturedException

logging.basicConfig(
    level=logging.INFO,
    format=r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s",
)
logger = logging.getLogger(name=__name__)


def main() -> ...:
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("test")
        .getOrCreate()
    )
    # .config(
    #     "spark.jars.packages",
    #     "org.apache.spark:spark-streaming-kafka-0-10_2.13-3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13-3.4.1"
    # )

    msg_value_schema = T.StructType(
        [
            T.StructField("client_id", T.StringType(), True),
            T.StructField("timestamp", T.DoubleType(), True),
            T.StructField("lat", T.DoubleType(), True),
            T.StructField("lon", T.DoubleType(), True),
        ]
    )

    df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
        )
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="de-kafka-admin-2022";',
        )
        .option("failOnDataLoss", False)
        .option("startingOffsets", "latest")
        .option("subscribe", "student.topic.cohort12.leonidgrishenkov")
        .load()
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType",
        )
        .withColumn("value", F.from_json(col=F.col("value"), schema=msg_value_schema))
    )

    df = df.select(
        F.col("value.client_id").alias("client_id"),
        F.col("value.timestamp").alias("timestamp"),
        F.col("value.lat").alias("lat"),
        F.col("value.lon").alias("lon"),
    )

    query = (
        df.writeStream.format("console")
        .option("truncate", False)
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logger.error(err)
        sys.exit(1)
