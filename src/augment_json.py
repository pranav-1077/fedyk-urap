"""
Augments JSON location data + constructs MSA panel data using Google Maps + Geocodio APIs
"""
import os
import json
import ijson
import googlemaps
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from ratelimit import limits, sleep_and_retry
from dotenv import load_dotenv

# configuration
load_dotenv()
INPUT_PATH = 'data/sample_1000_20240401.json'
AUGMENTED_JSON_PATH = 'data/augmented-sample.json'
MSA_YEAR_COUNTS_PATH = 'data/msa-year-counts.json'
gmaps = googlemaps.Client(key=os.getenv('GOOGLE_API_KEY'))

def parse_location(location):
    """
    Parses out location information for a single location dict

    Arguments:
    location: dict corresponding to single location json

    Returns:
    string of location information
    """
    # if full address provided use full address
    if 'raw' in location:
        if location['raw'][0].isdigit():
            return location['raw']
    # otherwise use city, state, country parsing based on availability
    if 'city' in location:
        if 'state' in location and 'country' in location:
            return f"{location['city']}, {location['state']}, {location['country']}"
        else:
            return f"{location['city']}, location['country']"
    elif 'state' in location:
        return f"{location['state']}, {location['country']}"
    else:
        return f"{location['country']}"

def collect_locations():
    """
    Iterates through input json file and tracks unique locations + location counts

    Returns:
    locations: set of unique location strings
    count_mapping: dict of {location: {year: count, ...}, ...}
    """
    locations = set() 
    count_mapping = {} 

    with open(INPUT_PATH, 'r') as file:
        records = ijson.items(file, 'item') 
        for record in records:
            id = record['data'][0]['val']

            # parse user level location
            if 'loc' in record:
                curr_loc = parse_location(record['loc']) 
                if curr_loc not in locations:
                    locations.add(curr_loc) 

            # parse experience level location
            if 'exp' in record:
                for curr_exp in record['exp']:
                    if 'loc' in curr_exp:
                        curr_loc = parse_location(curr_exp['loc']) 
                        if curr_loc not in locations:
                            locations.add(curr_loc)

                        # parse year range for experience locations
                        if 'start' in curr_exp and 'end' in curr_exp:
                            start_str = curr_exp['start']
                            end_str = curr_exp['end'] 
                            if ' ' not in start_str and '-' not in start_str:
                                start_yr = int(start_str) 
                            elif '-' in start_str:
                                start_yr = int(start_str.split('-')[0])
                            else:
                                start_yr = int(start_str.split(' ')[-1])
                            if ' ' not in end_str and '-' not in end_str:
                                end_yr = int(end_str)
                            elif '-' in end_str:
                                end_yr = int(end_str.split('-')[0])
                            else:
                                end_yr = int(end_str.split(' ')[-1]) 
                            if curr_loc not in count_mapping:
                                count_mapping[curr_loc] = {}
                            for curr_year in range(start_yr, end_yr+1):
                                if curr_year in count_mapping[curr_loc]:
                                    count_mapping[curr_loc][curr_year] += 1
                                else:
                                    count_mapping[curr_loc][curr_year] = 1  
    return locations, count_mapping 

def request_locations(locations, count_mapping):
    """
    Run a parallelized queue to get coordinate values using Google Maps,
    then lookup MSA for US locations via Geocodio.

    Arguments:
    locations: set of unique locations
    count_mapping: dict of {location: {year: count, ...}, ...}

    Returns:
    coord_mapping: dict mapping locations to coordinates
    msa_year_counts: dict tracking MSA-year counts
    """
    coord_mapping = {}
    msa_year_counts = {}

    # enable concurrency for parallelized API requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_loc = {executor.submit(encode_location, loc): loc for loc in locations}

        for future in as_completed(future_to_loc):
            loc = future_to_loc[future]
            result = future.result()
            if result:
                coord_mapping[loc] = result
                msa_name = result.get('msa_name')

                # store msa data if possible
                if msa_name and loc in count_mapping:
                    for year, count in count_mapping[loc].items():
                        key = (msa_name, year)
                        if key in msa_year_counts:
                            msa_year_counts[key] += count
                        else:
                            msa_year_counts[key] = count

    return coord_mapping, msa_year_counts

@sleep_and_retry
@limits(calls=15, period=1)
def get_msa_from_coords(lat, lon):
    """
    Get MSA data from Geocodio using reverse geocoding (direct API call).

    Arguments:
    lat: latitude
    lon: longitude

    Returns:
    dict with msa_name and msa_code, or empty dict if not found
    """
    try:
        url = f"https://api.geocod.io/v1.7/reverse?q={lat},{lon}&fields=census&api_key={os.getenv('GEOCODIO_API_KEY')}"
        response = requests.get(url)
        data = response.json()

        if 'results' not in data or not data['results']:
            return {}

        # parse msa information if possible otherwise return blank dicts
        result = data['results'][0]
        fields = result.get('fields')
        census = fields.get('census')
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
        print(f"Error getting MSA for ({lat}, {lon}): {e}")
        return {}

@sleep_and_retry
@limits(calls=40, period=1)
def encode_location(location):
    """
    Geocode a location using Google Maps (global), then get MSA from Geocodio (US only).

    Arguments:
    location: single location str

    Returns:
    dict containing lat, lon, and MSA (if US address), or None if geocoding fails
    """
    try:
        results = gmaps.geocode(location)
        if not results:
            return None

        result = results[0]
        lat = result['geometry']['location']['lat']
        lon = result['geometry']['location']['lng']

        output = {
            'lat': lat,
            'lon': lon,
            'formatted_address': result.get('formatted_address', '')
        }

        if location.endswith("United States"):
            msa_data = get_msa_from_coords(lat, lon)
            output.update(msa_data)

        return output
    except Exception as e:
        print(f"Error geocoding '{location}': {e}")
        return None


def augment_json(coord_mapping):
    """
    Create augmented JSON with coordinates added to location fields.
    Uses streaming to avoid loading entire file into memory.

    Arguments:
    coord_mapping: dict mapping location strings to coordinate data
    """
    with open(INPUT_PATH, 'r') as infile, open(AUGMENTED_JSON_PATH, 'w') as outfile:
        outfile.write('[')
        first = True

        for record in ijson.items(infile, 'item'):
            # Augment main loc field
            if 'loc' in record:
                loc_str = parse_location(record['loc'])
                if loc_str in coord_mapping:
                    record['loc']['lat'] = coord_mapping[loc_str].get('lat')
                    record['loc']['lon'] = coord_mapping[loc_str].get('lon')
                    if 'msa_name' in coord_mapping[loc_str]:
                        record['loc']['msa_name'] = coord_mapping[loc_str]['msa_name']
                    if 'msa_code' in coord_mapping[loc_str]:
                        record['loc']['msa_code'] = coord_mapping[loc_str]['msa_code']

            # Augment exp loc fields
            if 'exp' in record:
                for exp in record['exp']:
                    if 'loc' in exp:
                        loc_str = parse_location(exp['loc'])
                        if loc_str in coord_mapping:
                            exp['loc']['lat'] = coord_mapping[loc_str].get('lat')
                            exp['loc']['lon'] = coord_mapping[loc_str].get('lon')
                            if 'msa_name' in coord_mapping[loc_str]:
                                exp['loc']['msa_name'] = coord_mapping[loc_str]['msa_name']
                            if 'msa_code' in coord_mapping[loc_str]:
                                exp['loc']['msa_code'] = coord_mapping[loc_str]['msa_code']

            if not first:
                outfile.write(',\n')
            first = False
            json.dump(record, outfile)
        outfile.write(']')


def write_msa_year_counts(msa_year_counts):
    """
    Write MSA year counts to JSON file.

    Arguments:
    msa_year_counts: dict with (msa_name, year) tuple keys and count values
    """
    output = {}
    for (msa_name, year), count in msa_year_counts.items():
        if msa_name not in output:
            output[msa_name] = {}
        output[msa_name][year] = count

    with open(MSA_YEAR_COUNTS_PATH, 'w') as file:
        json.dump(output, file)    


if __name__ == "__main__":
    locations, count_mapping = collect_locations()
    coord_mapping, msa_year_counts = request_locations(locations, count_mapping)
    augment_json(coord_mapping)
    write_msa_year_counts(msa_year_counts)
    print(f"Parsed and augmented input json")
