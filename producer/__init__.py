import sys

import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.producer import DataProducer

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)
    config = config["apps"]["producer"]


def main() -> ...:
    producer = DataProducer()

    producer.produce_data(
        input_data_path=config["input-data-path"],
        output_topic=config["output-topic"],
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
