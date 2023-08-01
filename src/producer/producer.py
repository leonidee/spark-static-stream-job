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
        """

        {
            "id": "845e6b2c-2fca-11ee-b650-0242ac110002",
            "name": "Loyalty Love",
            "description": "Be a part of our Loyalty program and get a chance to earn points with every purchase. Redeem points for free drinks, pastries, or exclusive merchandise.",
            "provider_id": "845e6b90-2fca-11ee-b650-0242ac110002",
            "provider_name": "Coffee Harmony",
            "start_time": 1690797600.0,
            "end_time": 1690844400.0,
            "point_lat": 55.7355114718314,
            "point_lon": 37.6696475969381,
        }
        """

        SLEEP_TIME = 60

        log.info(
            f"Starting producing clients locations data for '{topic_name}' kafka topic"
        )

        log.info("Processing...")

        i = 0

        while True:
            df = self._get_data(path=path_to_data)

            current_time = datetime.now().timestamp()

            log.debug(f"Iteration {i}")
            log.debug(
                f"Sending message for {datetime.fromtimestamp(current_time)} time"
            )

            df = df[
                (df["start_time"] <= current_time) & (df["end_time"] >= current_time)
            ]

            if df.empty:
                log.info(
                    "DataFrame is empty. No actual advertisment exists in current time"
                )
                time.sleep(SLEEP_TIME)
                i += 1
                continue

            for row in df.itertuples():
                message = dict(
                    id=row.id,
                    name=row.name,
                    description=row.description,
                    provider_id=row.provider_id,
                    provider_name=row.provider_name,
                    start_time=row.start_time,
                    end_time=row.end_time,
                    point_lat=row.point_lat,
                    point_lon=row.point_lon,
                )
                self.kafka.send(topic=topic_name, value=message)

            time.sleep(SLEEP_TIME)
            i += 1
