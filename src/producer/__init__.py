from __future__ import annotations

import sys

import dotenv

dotenv.find_dotenv()
dotenv.load_dotenv()

sys.path.append("/app")

from src.producer.producer import AdvCampaignProducer, ClientsLocationsProducer

__all__ = ["AdvCampaignProducer", "ClientsLocationsProducer"]
