import sys

sys.path.append("/app")
from src.logger import LogManager
from src.producer import DataProducer

log = LogManager(level="DEBUG").get_logger(name=__name__)


def main() -> ...:
    if len(sys.argv) > 2:
        raise IndexError("Too many arguments! Expected 1")

    TOPIC_NAME = str(sys.argv[1])

    producer = DataProducer()

    df = producer.get_data(
        path_to_data="s3://data-ice-lake-05/master/data/source/spark-statis-stream/user-data/routes.csv"
    )

    producer.start_producing(df=df, topic_name=TOPIC_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
