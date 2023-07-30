import sys

import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.producer import ClientsLocationsProducer

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    producer = ClientsLocationsProducer()

    try:
        producer.produce_data(
            path_to_data=config["producer"]["data-path"],
            topic_name=config["producer"]["topic"],
        )
    except KeyboardInterrupt:
        log.warning("Producing data process was manualy stopped!")
        producer.kafka.flush()
        producer.kafka.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
