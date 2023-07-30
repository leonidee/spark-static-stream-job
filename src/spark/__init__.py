from os import getenv

import dotenv

dotenv.find_dotenv()
dotenv.load_dotenv()

from findspark import find, init

init(getenv("SPARK_HOME"))
find()

from src.spark.collector import StreamCollector
from src.spark.generator import SparkGenerator

__all__ = ["StreamCollector", "SparkGenerator"]
