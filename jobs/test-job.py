import sys

from pyspark.sql.utils import AnalysisException, CapturedException

sys.path.append("/app")
from src.logger import LogManager
from src.spark import StreamCollector

log = LogManager(level="DEBUG").get_logger(name=__name__)


def main() -> ...:
    try:
        collector = StreamCollector(app_name="test-app")
        query = collector.get_query()

        query.start().awaitTermination()

        query.stop()
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
