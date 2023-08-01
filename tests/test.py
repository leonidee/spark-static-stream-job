import time
import uuid
from dataclasses import dataclass, field, post_init
from datetime import datetime, time
from os import getenv


@dataclass
class AdvCampaign:
    id: str

    def __init__(self, id, time):
        self.id = id
        self.time = time

    @property
    def time(self):
        return self.time.timestamp()

    @time.setter
    def time(self, v: datetime):
        self.time = v


def main():
    a = AdvCampaign(id="idisa[ifjiweo", time=datetime.now())


if __name__ == "__main__":
    main()
