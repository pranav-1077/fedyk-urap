"""
Reads the augmented JSON produced by augment_json.py and the intermediate
location-year-counts file, looks up MSA data via Geocodio for US locations,
and writes msa-year-counts.json
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from ratelimit import limits, sleep_and_retry
from utils import get_logger, parse_location
from config import *
logger = get_logger(__name__)


@sleep_and_retry
@limits(calls=GEOCODIO_CALLS_PER_SECOND, period=1)
def get_msa_from_coords(lat, lon):
    """
    Reverse geocodes (lat, lon) to MSA name and code via Geocodio

    Arguments:
    lat: latitude
    lon: longitude

    Returns:
    dict with msa_name and msa_code, or empty dict if not found
    """
    try:
        url = (
            f"https://api.geocod.io/v1.7/reverse"
            f"?q={lat},{lon}&fields=census&api_key={GEOCODIO_API_KEY}"
        )
        response = requests.get(url)
        data = response.json()

        if 'results' not in data or not data['results']:
            return {}

        result = data['results'][0]
        fields = result.get('fields')
        census = fields.get('census') if fields else None
        if census:
            census_data = census.get('2025') or {}
            if 'metro_micro_statistical_area' in census_data:
                msa = census_data['metro_micro_statistical_area']
                if msa:
                    return {
                        'msa_name': msa.get('name'),
                        'msa_code': msa.get('area_code')
                    }
        return {}
    except Exception as e:
        logger.error("MSA lookup failed for (%s, %s): %s", lat, lon, e)
        return {}


def build_us_coord_mapping(augmented_path):
    """
    Extracts US locations with coordinates but no MSA data from augmented JSON

    Arguments:
    augmented_path: path to augmented JSON file

    Returns:
    dict of {location_str: {lat, lon}}
    """
    coord_mapping = {}

    with open(augmented_path, 'r') as f:
        records = json.load(f)

    for record in records:
        # Collect loc dicts from both the top-level loc and each exp entry
        loc_dicts = []
        if 'loc' in record:
            loc_dicts.append(record['loc'])
        for exp in record.get('exp', []):
            if 'loc' in exp:
                loc_dicts.append(exp['loc'])

        for loc_dict in loc_dicts:
            if 'lat' not in loc_dict or 'msa_name' in loc_dict:
                continue
            if loc_dict.get('country_code', '') != 'us':
                continue
            loc_str = parse_location(loc_dict)
            if loc_str not in coord_mapping:
                coord_mapping[loc_str] = {
                    'lat': loc_dict['lat'],
                    'lon': loc_dict['lon'],
                }

    return coord_mapping


def lookup_msa_for_all(coord_mapping):
    """
    Parallelized Geocodio reverse geocoding for all US locations

    Arguments:
    coord_mapping: dict of {location_str: {lat, lon}}

    Returns:
    dict of {location_str: {msa_name, msa_code}}
    """
    msa_mapping = {}

    with ThreadPoolExecutor(max_workers=GEOCODIO_MAX_WORKERS) as executor:
        future_to_loc = {
            executor.submit(get_msa_from_coords, data['lat'], data['lon']): loc
            for loc, data in coord_mapping.items()
        }
        for future in as_completed(future_to_loc):
            result = future.result()
            if result:
                msa_mapping[future_to_loc[future]] = result

    return msa_mapping


def build_msa_year_counts(msa_mapping, count_mapping):
    """
    Aggregates location-year counts up to MSA-year counts

    Arguments:
    msa_mapping: dict of {location_str: {msa_name, msa_code}}
    count_mapping: dict of {location_str: {year_str: count}}

    Returns:
    dict of {msa_name: {year: count}}
    """
    msa_year_counts = {}

    for loc, msa_data in msa_mapping.items():
        msa_name = msa_data.get('msa_name')
        if not msa_name or loc not in count_mapping:
            continue
        if msa_name not in msa_year_counts:
            msa_year_counts[msa_name] = {}
        for year_str, count in count_mapping[loc].items():
            # year_str is a string key from JSON; cast to int for the output dict
            year = int(year_str)
            msa_year_counts[msa_name][year] = (
                msa_year_counts[msa_name].get(year, 0) + count
            )

    return msa_year_counts


def main():
    # map locations to lat/lon 
    coord_mapping = build_us_coord_mapping(AUGMENTED_PATH)
    if not coord_mapping:
        return

    # open mapping between location-year and counts
    with open(LOCATION_YEAR_COUNTS_PATH, 'r') as f:
        count_mapping = json.load(f)

    # perform parallelized reverse geocoding
    msa_mapping = lookup_msa_for_all(coord_mapping)
    msa_year_counts = build_msa_year_counts(msa_mapping, count_mapping)

    # write panel data to output
    with open(MSA_YEAR_COUNTS_PATH, 'w') as f:
        json.dump(msa_year_counts, f)


if __name__ == "__main__":
    main()