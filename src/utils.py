"""
Shared utilities: logging setup and location parsing
"""

import logging
from pathlib import Path
from rapidfuzz import process as fuzz_process
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
    integer year, or None if date_str is None or unparseable
    """
    if not date_str:
        return None
    try:
        # input format "YYYY"
        if '-' not in date_str and ' ' not in date_str:
            return int(date_str)
        # input format "YYYY-MM" or "YYYY-MM-DD"
        if '-' in date_str:
            return int(date_str.split('-')[0])
        # input format "Month YYYY"
        return int(date_str.split(' ')[-1])
    except (ValueError, IndexError):
        return None


def _spell_correct(name, ref_list):
    """Returns closest match in ref_list if score >= SPELL_THRESHOLD, else name unchanged."""
    result = fuzz_process.extractOne(name.strip(), ref_list, score_cutoff=SPELL_THRESHOLD)
    return result[0] if result else name.strip()


def _classify_raw_part(s):
    """
    Classifies a raw token as 'city' or 'state' by comparing best fuzzy scores
    against CITY_NAMES and STATE_NAMES. Returns (category, corrected_name).
    No threshold — always picks whichever category scores higher.
    """
    city_result = fuzz_process.extractOne(s, CITY_NAMES)
    state_result = fuzz_process.extractOne(s, STATE_NAMES)
    city_score = city_result[1] if city_result else 0
    state_score = state_result[1] if state_result else 0
    if city_score >= state_score:
        return 'city', city_result[0]
    return 'state', state_result[0]


def parse_location(location):
    """
    Formats a location dict to a geocodable string, applying spell correction.

    Parsing priority:
    1. city + (state +) country from structured fields  → spell-corrected
    2. city + (state +) country inferred from raw       → spell-corrected
    3. state + country from structured fields           → spell-corrected
    4. state + country inferred from raw                → spell-corrected
    5. country from structured fields                   → as-is
    6. raw spell-corrected as country, or returned as-is

    Raw inference rules:
    - 3 comma parts → city, state, country
    - 1 comma part  → country
    - 2 comma parts → classify first part as city or state by fuzzy score
    """
    city = location.get('city')
    state = location.get('state')
    country = location.get('country')
    raw = location.get('raw', '')

    # 1. city + (state +) country in structured fields
    if city and country:
        if state:
            return f"{_spell_correct(city, CITY_NAMES)}, {_spell_correct(state, STATE_NAMES)}, {_spell_correct(country, COUNTRY_NAMES)}"
        return f"{_spell_correct(city, CITY_NAMES)}, {_spell_correct(country, COUNTRY_NAMES)}"

    # Pre-parse raw once; classify the ambiguous 2-part case up front
    raw_parts = [p.strip() for p in raw.split(',')] if raw else []
    raw_kind = raw_corrected_first = None
    if len(raw_parts) == 2:
        raw_kind, raw_corrected_first = _classify_raw_part(raw_parts[0])

    # 2. city + state + country or city + country from raw
    if len(raw_parts) == 3:
        return f"{_spell_correct(raw_parts[0], CITY_NAMES)}, {_spell_correct(raw_parts[1], STATE_NAMES)}, {_spell_correct(raw_parts[2], COUNTRY_NAMES)}"
    if len(raw_parts) == 2 and raw_kind == 'city':
        return f"{raw_corrected_first}, {_spell_correct(raw_parts[1], COUNTRY_NAMES)}"

    # 3. state + country in structured fields
    if state and country:
        return f"{_spell_correct(state, STATE_NAMES)}, {_spell_correct(country, COUNTRY_NAMES)}"

    # 4. state + country from raw
    if len(raw_parts) == 2 and raw_kind == 'state':
        return f"{raw_corrected_first}, {_spell_correct(raw_parts[1], COUNTRY_NAMES)}"

    # 5. country in structured fields (no spell correction)
    if country:
        return country

    # 6. spell-correct raw as country, or return as-is for unrecognised formats
    if len(raw_parts) == 1:
        return _spell_correct(raw_parts[0], COUNTRY_NAMES)
    return raw