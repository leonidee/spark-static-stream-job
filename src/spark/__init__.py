from os import getenv

import dotenv

dotenv.find_dotenv()
dotenv.load_dotenv()

import findspark

findspark.init(getenv("SPARK_HOME"))
findspark.find()

from src.spark.collector import StaticCollector, StreamCollector

__all__ = ["StreamCollector", "StaticCollector"]
