import sys
from datetime import datetime

import yaml

sys.path.append("/app")
from src.logger import LogManager
from src.producer import AdvCampaignProducer

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    producer = AdvCampaignProducer()

    datekey = datetime.today().date().strftime(r"%Y%m%d")
    input_path = (
        f"{config['spark']['adv-campaign-data']}/date={datekey}/part-00000.gz.parquet"
    )
    try:
        producer.produce_data(
            path_to_data=input_path,
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
