import sys
from datetime import datetime

import yaml
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager
from src.spark import StaticCollector

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    try:
        collector = StaticCollector(app_name="streaming-app")

        collector.persist_clients_locations_stream(
            topic=config["spark"]["topic"]["input"],
        )

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
