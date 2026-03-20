"""
Shared utilities: logging setup and location parsing
"""

import logging
from pathlib import Path
from config import *


def get_logger(name):
    """
    Returns a logger writing errors to both stderr and logs/pipeline.log

    Arguments:
    name: logger name, typically __name__ of the calling module
    """
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.ERROR)

    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(log_dir / LOG_FILE)
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def parse_year(date_str):
    """
    Extracts a year integer from a date string

    Arguments:
    date_str: date string in "YYYY", "YYYY-MM", "YYYY-MM-DD", or "Month YYYY" format

    Returns:
    integer year
    """
    # input format "YYYY"
    if '-' not in date_str and ' ' not in date_str:
        return int(date_str)
    # input format "YYYY-MM" or "YYYY-MM-DD"
    if '-' in date_str:
        return int(date_str.split('-')[0])
    # input format "Month YYYY"
    return int(date_str.split(' ')[-1])


def parse_location(location):
    """
    Formats a location dict to a "city, state, country" string

    Arguments:
    location: location dict from a record's loc or exp field

    Returns:
    location string with available fields joined by commas
    """
    # take full location string if available
    if 'raw' in location:
        if location['raw'][0].isdigit():
            return location['raw']
    # return either (city, state, country) or (city, country)
    if 'city' in location:
        if 'state' in location and 'country' in location:
            return f"{location['city']}, {location['state']}, {location['country']}"
        else:
            return f"{location['city']}, {location['country']}"
    # return (state, country)
    elif 'state' in location:
        return f"{location['state']}, {location['country']}"
    # return country
    else:
        return f"{location['country']}"
