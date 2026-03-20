"""
Augments JSON location data with coordinates via Nominatim (OpenStreetMap)
Produces augmented JSON and an intermediate location-year-counts file for
optional downstream MSA enrichment (see get_msa.py)
"""

import json
import ijson
from utils import get_logger, parse_location, parse_year
from config import *
logger = get_logger(__name__)


def collect_locations():
    """
    Streams input JSON and returns unique locations and per-location year counts

    Returns:
    locations: sorted list of unique location strings
    count_mapping: dict of {location: {year: count}}
    """
    locations = set()
    count_mapping = {}

    with open(INPUT_PATH, 'r') as file:
        for record in ijson.items(file, 'item'):

            # parse loc field from json entry 
            if 'loc' in record:
                locations.add(parse_location(record['loc']))

            # parse loc field from json entry exp array
            for curr_exp in record.get('exp', []):
                if 'loc' not in curr_exp:
                    continue
                curr_loc = parse_location(curr_exp['loc'])
                locations.add(curr_loc)

                # parse time info from exp array entry
                if 'start' in curr_exp and 'end' in curr_exp:
                    start_yr = parse_year(curr_exp['start'])
                    end_yr   = parse_year(curr_exp['end'])
                    if curr_loc not in count_mapping:
                        count_mapping[curr_loc] = {}
                    for yr in range(start_yr, end_yr + 1):
                        count_mapping[curr_loc][yr] = count_mapping[curr_loc].get(yr, 0) + 1

    return sorted(locations), count_mapping


def request_locations(locations):
    """
    Geocodes all unique locations serially via Nominatim

    Arguments:
    locations: sorted list of unique location strings

    Returns:
    coord_mapping: dict of {location: {lat, lon, formatted_address, country_code}}
    """
    coord_mapping = {}

    for loc in locations:
        try:
            result = geocode(loc)
            if result is None:
                continue
            country_code = result.raw.get('address', {}).get('country_code', '').lower()
            coord_mapping[loc] = {
                'lat': result.latitude,
                'lon': result.longitude,
                'formatted_address': result.address,
                'country_code': country_code,
            }
        except Exception as e:
            logger.error("Geocoding failed for '%s': %s", loc, e)

    return coord_mapping


def augment_json(coord_mapping):
    """
    Streams input JSON and writes an augmented copy with coordinates injected

    Arguments:
    coord_mapping: dict of {location: {lat, lon, formatted_address, country_code}}
    """
    with open(INPUT_PATH, 'r') as infile, \
         open(AUGMENTED_PATH, 'w') as outfile:
        outfile.write('[')
        first = True

        for record in ijson.items(infile, 'item'):
            if 'loc' in record:
                loc_str = parse_location(record['loc'])
                if loc_str in coord_mapping:
                    record['loc'].update({
                        'lat': coord_mapping[loc_str]['lat'],
                        'lon': coord_mapping[loc_str]['lon'],
                        'formatted_address': coord_mapping[loc_str]['formatted_address'],
                    })

            for exp in record.get('exp', []):
                if 'loc' in exp:
                    loc_str = parse_location(exp['loc'])
                    if loc_str in coord_mapping:
                        exp['loc'].update({
                            'lat': coord_mapping[loc_str]['lat'],
                            'lon': coord_mapping[loc_str]['lon'],
                            'formatted_address': coord_mapping[loc_str]['formatted_address'],
                        })

            if not first:
                outfile.write(',\n')
            first = False
            json.dump(record, outfile)

        outfile.write(']')


def main():
    # parse locations from input json
    locations, count_mapping = collect_locations()

    # perform geocoding and augment input json
    coord_mapping = request_locations(locations)
    augment_json(coord_mapping)

    # store mapping between location-year and counts for downstream panel analysis
    serializable = {
        loc: {str(yr): count for yr, count in years.items()}
        for loc, years in count_mapping.items()
    }
    with open(LOCATION_YEAR_COUNTS_PATH, 'w') as f:
        json.dump(serializable, f)


if __name__ == "__main__":
    main()
