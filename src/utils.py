import time
from typing import Dict, Any
import logging

logger = logging.getLogger("aggregator.utils")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def now_timestamp() -> float:
    return time.time()
