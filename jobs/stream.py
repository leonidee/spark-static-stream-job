import sys

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
        collector = StreamCollector(app_name=config["spark"]["app-name"])

        query = collector.get_stream(
            input_topic=config["spark"]["topic"]["input"],
            output_topic=config["spark"]["topic"]["output"],
            marketing_data_path=config["spark"]["marketing-data-path"],
        )

        query.start().awaitTermination()

        query.stop()
        collector.spark.stop()

    except KeyboardInterrupt:
        log.warning("Streaming process was manually stopped!")
        query.stop()
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
