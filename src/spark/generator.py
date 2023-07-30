import uuid
from datetime import datetime, time

from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

from src.logger import LogManager
from src.spark.spark import SparkInitializer

log = LogManager().get_logger(name=__name__)


class SparkGenerator(SparkInitializer):
    __slots__ = "spark"

    def __init__(self, app_name: str) -> None:
        super().__init__(app_name)

    def generate_adv_campaign_data(self, target_path: str) -> ...:
        """."""

        data = [
            Row(
                id=str(uuid.uuid1()),
                name="Morning Boost",
                description="Jump-start your mornings with a 20% discount on all our coffee drinks from 10:00 AM to 12:00 AM, Monday to Friday.",
                provider_id=str(uuid.uuid1()),
                provider_name="The Daily Grind",
                start_time=datetime.combine(datetime.today(), time(10, 0, 0)),
                end_time=datetime.combine(datetime.today(), time(12, 0, 0)),
                point_lat=55.8790015313034,
                point_lon=37.714565000436,
            ),
            Row(
                id=str(uuid.uuid1()),
                name="Cup and Canvas",
                description="Spend a soothing evening at our cafe every Friday where purchasing any drink grants you access to our watercolor workshop. Paint, sip, and relax!",
                provider_id=str(uuid.uuid1()),
                provider_name="Beanstalk Cafe",
                start_time=datetime.combine(datetime.today(), time(12, 30, 0)),
                end_time=datetime.combine(datetime.today(), time(14, 30, 0)),
                point_lat=55.7382386551547,
                point_lon=37.6733061300344,
            ),
            Row(
                id=str(uuid.uuid1()),
                name="Loyalty Love",
                description="Be a part of our Loyalty program and get a chance to earn points with every purchase. Redeem points for free drinks, pastries, or exclusive merchandise.",
                provider_id=str(uuid.uuid1()),
                provider_name="Coffee Harmony",
                start_time=datetime.combine(datetime.today(), time(13, 0, 0)),
                end_time=datetime.combine(datetime.today(), time(15, 0, 0)),
                point_lat=55.7355114718314,
                point_lon=37.6696475969381,
            ),
        ]

        df = self.spark.createDataFrame(data)

        OUTPUT_PATH = (
            f"{target_path}/date={datetime.today().date().strftime(r'%Y%m%d')}"
        )

        try:
            df.repartition(1).write.parquet(
                path=OUTPUT_PATH, mode="errorifexists", compression="gzip"
            )
            log.info(f"Done! Results -> {OUTPUT_PATH}")

        except AnalysisException as err:
            log.warning(f"Notice that {str(err)}")
            log.info("Overwriting...")
            df.repartition(1).write.parquet(
                path=OUTPUT_PATH, mode="overwrite", compression="gzip"
            )
            log.info(f"Done! Results -> {OUTPUT_PATH}")
