import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import yaml
from pyspark.sql.utils import AnalysisException, CapturedException  # type: ignore

sys.path.append("/app")
from src.logger import LogManager
from src.spark import StreamCollector

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)
    config = config["apps"]["spark"]


def run_streaming_query(query) -> ...:
    query.awaitTermination()


def log_query_heartbeat(query) -> ...:
    while query.isActive:
        log.info(
            f"'{query.name}' query heartbeat -> Status: {query.status}, Last progress: {query.lastProgress}"
        )
        time.sleep(60)


def main() -> ...:
    collector = StreamCollector(app_name="streaming-test-app", is_debug=True)

    try:
        query = collector.get_streaming_query(
            clients_locations_topic=config["clients-locations-topic"],
            clients_push_topic=config["clients-push-topic"],
            adv_campaign_data_path=config["adv-campaign-data"],
            adv_campaign_update_dt=datetime.today().date(),
            query_name="test-query",
            checkpoints_location=config["checkpoints-location"],
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(run_streaming_query, query)
            executor.submit(log_query_heartbeat, query)

    except KeyboardInterrupt:
        log.warning("Streaming query was manually stopped!")
        query.stop()  # type: ignore
        collector.spark.stop()
        sys.exit(0)

    except (AnalysisException, CapturedException) as err:
        log.error(err)
        collector.spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
