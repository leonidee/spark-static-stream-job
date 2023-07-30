from os import getenv

from pyspark.sql import SparkSession

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class SparkInitializer:
    __slots__ = "spark"

    def __init__(self, app_name: str) -> None:
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
