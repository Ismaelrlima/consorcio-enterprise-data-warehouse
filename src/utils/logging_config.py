import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger():


    if not os.path.exists("logs"):
        os.makedirs("logs")

    logger = logging.getLogger("DW_PIPELINE")
    logger.setLevel(logging.INFO)


    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


    file_handler = RotatingFileHandler(
        "logs/pipeline.log",
        maxBytes=5_000_000,  
        backupCount=3
    )
    file_handler.setFormatter(formatter)


    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
