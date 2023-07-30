import json
import time
import uuid
from datetime import datetime
from os import getenv

from kafka import KafkaProducer
from pandas import DataFrame, read_parquet
from s3fs import S3FileSystem

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


class DataProducer:
    def __init__(self) -> None:
        log.debug("Initializing KafkaProducer instance")

        self.kafka: KafkaProducer = KafkaProducer(
            bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVER"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def _get_data(self, path: str) -> DataFrame:
        log.debug(f"Getting input data for producing from -> '{path}'")

        s3 = S3FileSystem(
            key=getenv("AWS_ACCESS_KEY_ID"),
            secret=getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=getenv("AWS_ENDPOINT_URL"),
        )

        with s3.open(path, "rb") as f:
            df = read_parquet(f)

        log.debug(f"Loaded frame with shape {df.shape}")

        return df


class ClientsLocationsProducer(DataProducer):
    __slots__ = ("kafka",)

    def __init__(self) -> None:
        super().__init__()

    def produce_data(self, path_to_data: str, topic_name: str) -> ...:
        df = self._get_data(path=path_to_data)

        log.info(
            f"Starting producing clients locations data for '{topic_name}' kafka topic"
        )

        log.info("Processing...")

        i = 0
        for row in df.iterrows():
            client_id = str(uuid.uuid1())
            polyline = json.loads(row[1]["polyline"])

            self._send_polyline(client_id, polyline, topic_name)

            if i % 20 == 0:
                self._send_polyline(client_id, polyline, topic_name)
            i += 1

            log.debug(f"send {len(polyline)} points for user {client_id}")

            self.kafka.flush()

        log.info("All data sent. Stopping process")

        log.debug("Closing kafka producer")
        self.kafka.close()

    def _send_polyline(self, client_id: str, polyline: dict, topic_name: str) -> ...:
        i = 0
        for coordinate in polyline:
            lat = coordinate[0]
            lon = coordinate[1]
            timestamp = datetime.timestamp(datetime.now())

            message = {
                "client_id": client_id,
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
            }

            self.kafka.send(topic=topic_name, value=message)

            i += 1
            if i % 10 == 0:
                self.kafka.send(topic=topic_name, value=message)


class AdvCampaignProducer(DataProducer):
    __slots__ = ("kafka",)

    def __init__(self) -> None:
        super().__init__()

    def produce_data(self, path_to_data: str, topic_name: str) -> ...:
        log.info(
            f"Starting producing clients locations data for '{topic_name}' kafka topic"
        )

        log.info("Processing...")

        while True:
            df = self._get_data(path=path_to_data)

            # path="s3://data-ice-lake-05/master/data/source/spark-static-stream/adv-campaign-data/date=20230730/part-00000-3d254ef6-2d18-4f65-89de-02b413378fb9-c000.gz.parquet"
            current_time = datetime.now()

            df = df[
                (df["start_time"] <= current_time) & (df["end_time"] >= current_time)
            ]

            if df.empty:
                log.info(
                    "DataFrame is empty. No actual advertisment exists in current time"
                )
                time.sleep(60)
                break

            for row in df.itertuples(name="Row"):
                print(row.id, row.name)

            time.sleep(60)
