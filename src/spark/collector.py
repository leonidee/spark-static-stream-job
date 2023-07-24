import sys
from os import getenv

import pyspark.sql.functions as F
import pyspark.sql.types as T
import yaml
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


class StreamCollector:
    __slots__ = "spark"

    def __init__(self, app_name: str) -> None:
        from pyspark.sql import SparkSession

        log.info("Initializing Spark Session")

        self.spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(app_name)
            .config(
                map={
                    "spark.hadoop.fs.s3a.access.key": getenv("AWS_ACCESS_KEY_ID"),
                    "spark.hadoop.fs.s3a.secret.key": getenv("AWS_SECRET_ACCESS_KEY"),
                    "spark.hadoop.fs.s3a.endpoint": getenv("AWS_ENDPOINT_URL"),
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                }
            )
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel(config["logging"]["spark"]["level"].upper())

    def get_marketing_data(self) -> DataFrame:
        df = self.spark.read.csv(
            "s3a://data-ice-lake-05/master/data/source/spark-statis-stream/marketing-data/marketing_companies.csv",
            inferSchema=True,
            header=True,
        )

        df = df.select(
            F.col("id").alias("adv_campaign_id"),
            F.col("name").alias("adv_campaign_name"),
            F.col("description").alias("adv_campaign_description"),
            F.col("start_time").alias("adv_campaign_start_time"),
            F.col("end_time").alias("adv_campaign_end_time"),
            F.col("point_lat").alias("adv_campaign_point_lat"),
            F.col("point_lon").alias("adv_campaign_point_lon"),
        )

        return df

    def get_clients_locations_stream(self, topic: str) -> DataFrame:
        msg_value_schema = T.StructType(
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
            .withColumn(
                "value", F.from_json(col=F.col("value"), schema=msg_value_schema)
            )
        )

        return df

    def get_stream(
        self,
        marketing_data_path: str,
        input_topic: str,
        output_topic: str,
    ) -> DataStreamWriter:
        df = self.get_clients_locations_stream(topic=input_topic)

        return (
            df.writeStream.format("console")
            .option("truncate", False)
            .outputMode("append")
            .trigger(processingTime="15 seconds")
        )


# def main() -> ...:

#     msg_value_schema = T.StructType(
#         [
#             T.StructField("client_id", T.StringType(), True),
#             T.StructField("timestamp", T.DoubleType(), True),
#             T.StructField("lat", T.DoubleType(), True),
#             T.StructField("lon", T.DoubleType(), True),
#         ]
#     )

#     df = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", getenv("KAFKA_BOOTSTRAP_SERVER"))
#         .option("failOnDataLoss", False)
#         .option("startingOffsets", "latest")
#         .option("subscribe", "base")
#         .load()
#         .select(
#             F.col("key").cast("string"),
#             F.col("value").cast("string"),
#         )
#         .withColumn("value", F.from_json(col=F.col("value"), schema=msg_value_schema))
#     )

#     df = df.select(
#         F.col("value.client_id").alias("client_id"),
#         F.col("value.timestamp").alias("timestamp"),
#         F.col("value.lat").alias("lat"),
#         F.col("value.lon").alias("lon"),
#     )

#     query = (
#         df.writeStream.format("console")
#         .trigger(processingTime="10 seconds")
#         .option("truncate", False)
#         .outputMode("append")
#         .start()
#         .awaitTermination()
#     )


# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as err:
#         logger.error(err)
#         sys.exit(1)


# [23-07-19 18:27:28] {MicroBatchExecution} INFO: Streaming query made progress: {
#   "id" : "90a7ee2c-2e91-4fcc-94cf-b2f4d2f431a3",
#   "runId" : "ab99d132-bfba-4d5f-9dc5-d7004ca249cd",
#   "name" : null,
#   "timestamp" : "2023-07-19T18:27:25.049Z",
#   "batchId" : 0,
#   "numInputRows" : 0,
#   "inputRowsPerSecond" : 0.0,
#   "processedRowsPerSecond" : 0.0,
#   "durationMs" : {
#     "addBatch" : 893,
#     "commitOffsets" : 25,
#     "getBatch" : 572,
#     "latestOffset" : 645,
#     "queryPlanning" : 1438,
#     "triggerExecution" : 3663,
#     "walCommit" : 70
#   },
#   "stateOperators" : [ ],
#   "sources" : [ {
#     "description" : "KafkaV2[Subscribe[base]]",
#     "startOffset" : null,
#     "endOffset" : {
#       "base" : {
#         "0" : 3
#       }
#     },
#     "latestOffset" : {
#       "base" : {
#         "0" : 3
#       }
#     },
#     "numInputRows" : 0,
#     "inputRowsPerSecond" : 0.0,
#     "processedRowsPerSecond" : 0.0,
#     "metrics" : {
#       "avgOffsetsBehindLatest" : "0.0",
#       "maxOffsetsBehindLatest" : "0",
#       "minOffsetsBehindLatest" : "0"
#     }
#   } ],
#   "sink" : {
#     "description" : "org.apache.spark.sql.execution.streaming.ConsoleTable$@7687f0e2",
#     "numOutputRows" : 0
#   }
# }
# [23-07-19 18:29:46] {MicroBatchExecution} INFO: Streaming query made progress: {
#   "id" : "90a7ee2c-2e91-4fcc-94cf-b2f4d2f431a3",
#   "runId" : "ab99d132-bfba-4d5f-9dc5-d7004ca249cd",
#   "name" : null,
#   "timestamp" : "2023-07-19T18:29:46.282Z",
#   "batchId" : 303,
#   "numInputRows" : 1035,
#   "inputRowsPerSecond" : 9495.412844036697,
#   "processedRowsPerSecond" : 10247.524752475247,
#   "durationMs" : {
#     "addBatch" : 51,
#     "commitOffsets" : 20,
#     "getBatch" : 0,
#     "latestOffset" : 2,
#     "queryPlanning" : 4,
#     "triggerExecution" : 101,
#     "walCommit" : 24
#   },
#   "stateOperators" : [ ],
#   "sources" : [ {
#     "description" : "KafkaV2[Subscribe[base]]",
#     "startOffset" : {
#       "base" : {
#         "0" : 410401
#       }
#     },
#     "endOffset" : {
#       "base" : {
#         "0" : 411436
#       }
#     },
#     "latestOffset" : {
#       "base" : {
#         "0" : 411436
#       }
#     },
#     "numInputRows" : 1035,
#     "inputRowsPerSecond" : 9495.412844036697,
#     "processedRowsPerSecond" : 10247.524752475247,
#     "metrics" : {
#       "avgOffsetsBehindLatest" : "0.0",
#       "maxOffsetsBehindLatest" : "0",
#       "minOffsetsBehindLatest" : "0"
#     }
#   } ],
#   "sink" : {
#     "description" : "org.apache.spark.sql.execution.streaming.ConsoleTable$@7687f0e2",
#     "numOutputRows" : 1035
#   }
# }
