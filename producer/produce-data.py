import sys

import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.producer import DataProducer

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    producer = DataProducer()

    df = producer.get_data(path_to_data=config["producer"]["data-path"])

    producer.start_producing(df=df, topic_name=config["producer"]["topic"]["output"])


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
