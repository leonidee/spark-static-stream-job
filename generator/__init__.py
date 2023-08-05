import sys

import yaml

sys.path.append("/app")
from src.generator import QuasiDataGenerator
from src.logger import LogManager

log = LogManager().get_logger(name=__name__)

with open("/app/config.yaml") as f:
    config = yaml.safe_load(f)
    config = config["apps"]["generator"]


def main() -> ...:
    generator = QuasiDataGenerator()

    generator.generate_adv_campaign_data(output_path=config["output-data-path"])


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        log.exception(err)
        sys.exit(2)
