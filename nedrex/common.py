import sys

from loguru import logger
from mongoengine import connect as _connect, disconnect as _disconnect


def connect(*, host="localhost", port=27017):
    _connect(host=host, port=port)

def disconnect():
    _disconnect()


# Reset the logger
logger.remove()

logger.add(
    "repodb.log",
    colorize=True,
    format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
)

logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)
