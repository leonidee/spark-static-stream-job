from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime
from os import getenv

from dotenv import find_dotenv, load_dotenv
from kafka import KafkaProducer
from pandas import DataFrame

sys.path.append("/app")
from src.logger import LogManager

find_dotenv()
load_dotenv()

log = LogManager(level="DEBUG").get_logger(name=__name__)


class DataProducer:
    def __init__(self) -> None:
        self.kafka: KafkaProducer = self._create_producer()

    def _create_producer(self) -> KafkaProducer:
        log.debug("Initializing KafkaProducer instance")

        return KafkaProducer(bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVER"))

    def start_producing(self, df: DataFrame, topic_name: str) -> ...:
        log.info(f"Starting producing data for '{topic_name}' kafka topic")

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
            message_json = json.dumps(message).encode("utf-8")

            self.kafka.send(topic=topic_name, value=message_json)

            i += 1
            if i % 10 == 0:
                self.kafka.send(topic=topic_name, value=message_json)

    def get_data(self, path_to_data: str) -> DataFrame:
        from pandas import read_csv
        from s3fs import S3FileSystem

        log.debug(f"Getting input data from -> '{path_to_data}'")

        s3 = S3FileSystem(
            key=getenv("AWS_ACCESS_KEY_ID"),
            secret=getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=getenv("AWS_ENDPOINT_URL"),
        )

        with s3.open(path_to_data, "r") as f:
            df = read_csv(f)

        log.debug(f"Loaded frame with shape {df.shape}")

        return df


def main() -> ...:
    producer = DataProducer()
    df = producer.get_data(
        path_to_data="s3://data-ice-lake-05/master/data/source/spark-statis-stream/user-data/routes.csv"
    )

    topic_name = "test"

    producer.start_producing(df=df, topic_name=topic_name)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
