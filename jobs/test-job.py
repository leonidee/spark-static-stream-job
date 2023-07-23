import sys

sys.path.append("/app")
from src.logger import LogManager
from src.spark.collector import StreamCollector

from pyspark.sql.utils import AnalysisException, CapturedException

log = LogManager(level="DEBUG").get_logger(name=__name__)

def main() -> ...:

    # try:
    collector = StreamCollector(app_name="test-app")

    df = collector.get_marketing_frame()

    df.show(50)
    df.printSchema()

    collector.spark.stop()

    # except (AnalysisException, CapturedException) as err:
    #     log.exception(err)
    #     sys.exit(1)


if __name__ == "__main__":
    main()
