import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, time
from os import getenv

from kafka import KafkaProducer
from pandas import DataFrame, read_parquet
from s3fs import S3FileSystem

from src.logger import LogManager

log = LogManager().get_logger(name=__name__)


@dataclass
class AdvCampaign:
    id: str
    name: str
    description: str
    provider_id: str
    provider_name: str
    start_time: datetime
    end_time: datetime
    point_lat: float
    point_lon: float


class QuasiDataGenerator:
    __slots__ = "s3"

    def __init__(self) -> None:
        log.info("Initializing QuasiDataGenerator instance")
        self.s3 = S3FileSystem(
            key=getenv("AWS_ACCESS_KEY_ID"),
            secret=getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=getenv("AWS_ENDPOINT_URL"),
        )

    def generate_adv_campaign_data(self, target_path: str) -> ...:
        log.info("Generating advertisment campaign data")

        adv_campaigns = [
            AdvCampaign(
                id=str(uuid.uuid1()),
                name="Morning Boost",
                description="Jump-start your mornings with a 20% discount on all our coffee drinks from 10:00 AM to 12:00 AM, Monday to Friday.",
                provider_id=str(uuid.uuid1()),
                provider_name="The Daily Grind",
                start_time=datetime.combine(datetime.today(), time(10, 0, 0)),
                end_time=datetime.combine(datetime.today(), time(21, 0, 0)),
                point_lat=55.8790015313034,
                point_lon=37.714565000436,
            ),
            AdvCampaign(
                id=str(uuid.uuid1()),
                name="Cup and Canvas",
                description="Spend a soothing evening at our cafe every Friday where purchasing any drink grants you access to our watercolor workshop. Paint, sip, and relax!",
                provider_id=str(uuid.uuid1()),
                provider_name="Beanstalk Cafe",
                start_time=datetime.combine(datetime.today(), time(10, 30, 0)),
                end_time=datetime.combine(datetime.today(), time(22, 30, 0)),
                point_lat=55.7382386551547,
                point_lon=37.6733061300344,
            ),
            AdvCampaign(
                id=str(uuid.uuid1()),
                name="Loyalty Love",
                description="Be a part of our Loyalty program and get a chance to earn points with every purchase. Redeem points for free drinks, pastries, or exclusive merchandise.",
                provider_id=str(uuid.uuid1()),
                provider_name="Coffee Harmony",
                start_time=datetime.combine(datetime.today(), time(10, 0, 0)),
                end_time=datetime.combine(datetime.today(), time(23, 0, 0)),
                point_lat=55.7355114718314,
                point_lon=37.6696475969381,
            ),
        ]
        log.debug(f"Generated {len(adv_campaigns)} element")

        df = DataFrame(data=adv_campaigns)

        datekey = datetime.today().date().strftime(r"%Y%m%d")
        output_path = f"{target_path}/date={datekey}/part-00000.gz.parquet"

        log.info(f"Writing results -> {output_path}")

        with self.s3.open(
            output_path,
            "wb",
        ) as f:
            df = df.to_parquet(f, compression="gzip")

        log.info("Done. All success")
