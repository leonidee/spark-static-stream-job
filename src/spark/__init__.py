from os import getenv

from dotenv import load_dotenv, find_dotenv
find_dotenv()
load_dotenv()

from findspark import init, find

init(getenv("SPARK_HOME"))
find()

from src.spark.collector import StreamCollector

__all__ = [
    "StreamCollector",
]
