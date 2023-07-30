from __future__ import annotations

import sys

import dotenv

dotenv.find_dotenv()
dotenv.load_dotenv()

sys.path.append("/app")

from src.generator.generator import QuasiDataGenerator

__all__ = ["QuasiDataGenerator"]
