import sys

import yaml
from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager
from src.spark import SparkGenerator

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    try:
        gen = SparkGenerator(app_name="adv-campaign-data-generator-app")

        gen.generate_marketing_data(target_path=config["spark"]["adv-campaign-data"])

        gen.spark.stop()

    except (AnalysisException, CapturedException) as err:
        log.error(err)
        gen.spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
