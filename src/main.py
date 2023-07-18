#!/usr/bin/env python

from pyspark.sql import SparkSession  # type: ignore


def main():
    spark = (
        SparkSession.builder.master("spark://spark:7077")
        .config("spark.hadoop.fs.s3a.access.key", "YCAJEHO1oeFs63PxS3wi3c8tE")
        .config(
            "spark.hadoop.fs.s3a.secret.key", "YCOe4r1c0Fx7dghS7AmU7lrnzae3SPkqq6KIrxto"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .appName("test-app")
        .getOrCreate()
    )

    df = spark.read.parquet(
        "s3a://data-ice-lake-05/master/data/source/messenger-yp/events/event_type=message/date=2022-01-15"
    )

    df.show(100)


if __name__ == "__main__":
    main()
