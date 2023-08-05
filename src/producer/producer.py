import json
import uuid
from datetime import datetime
from os import getenv
from typing import List

from kafka import KafkaProducer
from pandas import DataFrame, concat, read_parquet
from s3fs import S3FileSystem

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class DataProducer:
    __slots__ = ("kafka", "s3")

    def __init__(self) -> None:
        log.debug("Initializing KafkaProducer instance")

        # Docs: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html?highlight=kafkaproducer
        self.kafka: KafkaProducer = KafkaProducer(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVER"),
            key_serializer=lambda v: json.dumps(v).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.s3 = S3FileSystem(
            key=getenv("AWS_ACCESS_KEY_ID"),
            secret=getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=getenv("AWS_ENDPOINT_URL"),
        )

    def _get_data(self, path: str) -> DataFrame:
        log.debug(f"Getting input data for producing from -> '{path}'")

        files: List[str] = self.s3.ls(path)

        if len(files) > 1:
            df_list = []
            for file in files:
                with self.s3.open(file, "rb") as f:
                    df = read_parquet(f)

                df_list.append(df)

            df = concat(df_list)
            log.debug(f"Loaded frame with shape {df.shape}")
            return df
        else:
            with self.s3.open(*files, "rb") as f:
                df = read_parquet(f)

            log.debug(f"Loaded frame with shape {df.shape}")
            return df

    def produce_data(self, input_data_path: str, output_topic: str) -> ...:
        """"""
        df: DataFrame = self._get_data(path=input_data_path)

        log.info(
            f"Starting producing clients locations data for '{output_topic}' kafka topic"
        )

        log.info("Processing...")

        i = 0
        for row in df.iterrows():
            client_id = str(uuid.uuid1())
            polyline = json.loads(row[1]["polyline"])

            self._send_polyline(client_id, polyline, output_topic)

            if i % 20 == 0:
                self._send_polyline(client_id, polyline, output_topic)
            i += 1

            log.debug(f"send {len(polyline)} points for user {client_id}")

            self.kafka.flush()

        log.info("All data sent. Stopping process...")

        log.debug("Closing kafka producer")
        self.kafka.close()
        log.debug("Done. All success")

    def _send_polyline(self, client_id: str, polyline: dict, topic: str) -> ...:
        i = 0
        for coordinate in polyline:
            lat = coordinate[0]
            lon = coordinate[1]
            timestamp = datetime.timestamp(datetime.now())

            # Docs: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html?highlight=kafkaproducer
            self.kafka.send(
                topic=topic,
                key=client_id,
                value={
                    "timestamp": timestamp,
                    "lat": lat,
                    "lon": lon,
                },
            )

            i += 1
            if i % 10 == 0:
                self.kafka.send(
                    topic=topic,
                    key=client_id,
                    value={
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon,
                    },
                )
