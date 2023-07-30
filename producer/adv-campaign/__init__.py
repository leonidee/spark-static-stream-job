import sys

import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.producer import AdvCampaignProducer

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    producer = AdvCampaignProducer()

    try:
        producer.produce_data(
            path_to_data="s3://data-ice-lake-05/master/data/source/spark-static-stream/adv-campaign-data/date=20230730/part-00000.gz.parquet",
            topic_name="adv-campaign-data",
        )
        producer.kafka.flush()
        producer.kafka.close()

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
