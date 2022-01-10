import logging
from datetime import datetime
import os


if not os.path.exists(".\logs"):
    os.makedirs(".\logs")

now = datetime.now()
date = now.strftime("%Y%m%d")

logging.basicConfig(
    filename=fr".\logs\{date}.log",
    level=logging.DEBUG,
    format="%(asctime)s || %(message)s",
    datefmt="%Y-%b-%d- %H:%M:%S",
)


def log_msg(msg):
    logging.info(msg)
    print(msg)