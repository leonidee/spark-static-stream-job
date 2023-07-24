from dotenv import find_dotenv, load_dotenv

find_dotenv()
load_dotenv()


from src.producer.producer import DataProducer

__all__ = [
    "DataProducer",
]
