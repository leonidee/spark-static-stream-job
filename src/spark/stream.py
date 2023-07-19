import logging
import sys

import findspark

findspark.init("/opt/bitnami/spark")
findspark.find()

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
        .appName("streaming-test-app")
        .getOrCreate()
    )

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
            "kafka.bootstrap.servers", "158.160.78.165:9092"
        )
        .option("failOnDataLoss", False)
        .option("startingOffsets", "latest")
        .option("subscribe", "base")
        .load()
        .select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
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
        .trigger(processingTime='10 seconds')
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