import sys
from datetime import datetime

import yaml
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager
from src.spark import StreamCollector

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    try:
        collector = StreamCollector(app_name="test-app")

        df = collector._get_marketing_data(
            path_to_data=f"{config['spark']['adv-campaign-data']}/date={datetime.today().date().strftime(r'%Y%m%d')}",
        )

        df.show(10)
        df.printSchema()

        collector.spark.stop()

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
