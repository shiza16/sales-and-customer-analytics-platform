# utils/logger.py
import logging
from .config import LOG_PATH

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_logger(name=__name__):
    return logging.getLogger(name)
