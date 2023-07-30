import sys

import yaml

sys.path.append("/app")
from src.generator import QuasiDataGenerator
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)


def main() -> ...:
    generator = QuasiDataGenerator()

    generator.generate_adv_campaign_data(
        target_path=config["spark"]["adv-campaign-data"]
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
