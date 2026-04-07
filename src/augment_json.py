"""
Augments JSON location data with coordinates via Nominatim (OpenStreetMap)
Produces augmented JSON and an intermediate location-year-counts file for
optional downstream MSA enrichment (see get_msa.py)
"""

import json
import time
import sqlite3
import threading
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
import ijson
from utils import get_logger, get_week_key, parse_location, parse_year
from config import *
logger = get_logger(__name__)

# script-specific variables
last_mapbox_t = 0.0
mapbox_delay = None 


def init_db():
    """Opens the SQLite cache database, enables WAL mode, and creates tables if they don't exist."""
    con = sqlite3.connect(GEOCODE_CACHE_DB, check_same_thread=False)
    con.execute('PRAGMA journal_mode=WAL')
    con.execute('''
        CREATE TABLE IF NOT EXISTS geocode_cache (
            location          TEXT PRIMARY KEY,
            lat               REAL,
            lon               REAL,
            formatted_address TEXT,
            country_code      TEXT
        )
    ''')
    con.execute('''
        CREATE TABLE IF NOT EXISTS mapbox_usage (
            week_start TEXT PRIMARY KEY,
            count      INTEGER NOT NULL DEFAULT 0
        )
    ''')
    con.commit()
    return con



def load_mapbox_usage(con):
    """Returns the number of Mapbox requests made in the current ISO week."""
    row = con.execute(
        'SELECT count FROM mapbox_usage WHERE week_start = ?', (get_week_key(),)
    ).fetchone()
    return row[0] if row else 0


def save_mapbox_usage(con, write_lock, delta):
    """Increments the Mapbox request count for the current ISO week by delta."""
    if delta == 0:
        return
    with write_lock:
        con.execute(
            '''INSERT INTO mapbox_usage (week_start, count) VALUES (?, ?)
               ON CONFLICT(week_start) DO UPDATE SET count = count + excluded.count''',
            (get_week_key(), delta)
        )
        con.commit()


def load_cache():
    """Loads all cached geocoding results from the SQLite database into a dict."""
    con = init_db()
    rows = con.execute(
        'SELECT location, lat, lon, formatted_address, country_code FROM geocode_cache'
    ).fetchall()
    con.close()
    return {
        row[0]: {
            'lat': row[1],
            'lon': row[2],
            'formatted_address': row[3],
            'country_code': row[4],
        }
        for row in rows
    }


def write_cache_entry(con, write_lock, loc, result):
    """Writes a single geocoding result to the SQLite cache under a write lock."""
    with write_lock:
        con.execute(
            '''INSERT OR REPLACE INTO geocode_cache
               (location, lat, lon, formatted_address, country_code)
               VALUES (?, ?, ?, ?, ?)''',
            (loc, result['lat'], result['lon'], result['formatted_address'], result['country_code'])
        )
        con.commit()


def call_mapbox(loc, retries=None):
    """
    Single Mapbox geocoding request, rate-limited across all threads.
    Delay is seeded from MAPBOX_MIN_DELAY and updated from X-Rate-Limit-Limit headers.
    Retries on 429s sleeping until X-Rate-Limit-Reset, up to MAPBOX_MAX_RETRIES times.
    """
    global last_mapbox_t, mapbox_delay
    if retries is None:
        retries = MAPBOX_MAX_RETRIES

    with MAPBOX_RATE_LOCK:
        if mapbox_delay is None:
            mapbox_delay = MAPBOX_MIN_DELAY
        gap = mapbox_delay - (time.perf_counter() - last_mapbox_t)
        if gap > 0:
            time.sleep(gap)
        last_mapbox_t = time.perf_counter()

    url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{quote(loc, safe="")}.json'
    resp = requests.get(url, params={'access_token': MAPBOX_ACCESS_TOKEN, 'limit': 1})

    if resp.status_code == 429:
        if retries > 0:
            reset_t = int(resp.headers.get('X-Rate-Limit-Reset', time.time() + 1))
            time.sleep(max(reset_t - time.time(), 0) + 0.1)
            return call_mapbox(loc, retries - 1)
        resp.raise_for_status()

    resp.raise_for_status()

    # update delay from header so all threads self-tune to the actual limit
    limit_header = resp.headers.get('X-Rate-Limit-Limit')
    if limit_header:
        with MAPBOX_RATE_LOCK:
            mapbox_delay = 60.0 / max(int(limit_header), 1)

    features = resp.json().get('features', [])
    if not features:
        return None

    feature = features[0]
    lon, lat = feature['geometry']['coordinates']
    country_code = next(
        (ctx.get('short_code', '').lower() for ctx in feature.get('context', [])
         if ctx.get('id', '').startswith('country.')),
        ''
    )
    return {
        'lat': lat,
        'lon': lon,
        'formatted_address': feature.get('place_name', ''),
        'country_code': country_code,
    }


def geocode_nominatim(loc):
    """Single Nominatim geocoding request using the existing rate-limited client."""
    try:
        result = geocode(loc)
        if result is None:
            return None
        return {
            'lat': result.latitude,
            'lon': result.longitude,
            'formatted_address': result.address,
            'country_code': result.raw.get('address', {}).get('country_code', '').lower(),
        }
    except Exception as e:
        logger.error("Nominatim failed for '%s': %s", loc, e)
        return None


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
                    end_yr = parse_year(curr_exp['end'])
                    if start_yr is None or end_yr is None:
                        continue
                    if curr_loc not in count_mapping:
                        count_mapping[curr_loc] = {}
                    for yr in range(start_yr, end_yr + 1):
                        count_mapping[curr_loc][yr] = count_mapping[curr_loc].get(yr, 0) + 1

    return sorted(locations), count_mapping


def request_locations(locations):
    """
    Geocodes all unique locations, skipping cache hits.
    Primary: Mapbox via parallel workers, capped at MAPBOX_WEEKLY_LIMIT requests per week.
    Falls back to serial Nominatim once the weekly budget is exhausted.
    Results are written to the SQLite cache incrementally under a write lock.

    Arguments:
    locations: sorted list of unique location strings

    Returns:
    coord_mapping: dict of {location: {lat, lon, formatted_address, country_code}}
    """
    cache = load_cache()
    coord_mapping = dict(cache)
    misses = [loc for loc in locations if loc not in cache]

    con = init_db()
    write_lock = threading.Lock()

    week_usage = load_mapbox_usage(con)
    remaining = max(MAPBOX_WEEKLY_LIMIT - week_usage, 0)

    print(f"  mapbox budget  : {remaining:,} / {MAPBOX_WEEKLY_LIMIT:,} remaining this week")
    print(f"  cache hits     : {len(locations) - len(misses)}/{len(locations)}")
    print(f"  to geocode     : {len(misses)}")

    if not misses:
        con.close()
        return coord_mapping

    mapbox_count = 0
    count_lock = threading.Lock()
    nominatim_queue = []
    nominatim_lock = threading.Lock()
    limit_announced = threading.Event()

    def try_claim_slot():
        nonlocal mapbox_count
        with count_lock:
            if mapbox_count >= remaining:
                return False
            mapbox_count += 1
            return True

    def geocode_one(loc):
        if not try_claim_slot():
            if not limit_announced.is_set():
                print('\n  Mapbox weekly limit reached — switching to Nominatim')
                limit_announced.set()
            with nominatim_lock:
                nominatim_queue.append(loc)
            return
        try:
            result = call_mapbox(loc)
            if result is None:
                return
            coord_mapping[loc] = result
            write_cache_entry(con, write_lock, loc, result)
        except Exception as e:
            logger.error("Mapbox failed for '%s': %s", loc, e)

    if remaining == 0:
        print(f"  Mapbox weekly limit reached — using Nominatim for all {len(misses)} locations")
        nominatim_queue.extend(misses)
    else:
        with ThreadPoolExecutor(max_workers=MAPBOX_MAX_WORKERS) as executor:
            futures = [executor.submit(geocode_one, loc) for loc in misses]
            for f in as_completed(futures):
                f.result()
        save_mapbox_usage(con, write_lock, mapbox_count)

    if nominatim_queue:
        print(f"  Nominatim fallback : {len(nominatim_queue)} locations")
        for loc in nominatim_queue:
            result = geocode_nominatim(loc)
            if result:
                coord_mapping[loc] = result
                write_cache_entry(con, write_lock, loc, result)

    con.close()
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
    """Runs the full pipeline: collect locations, geocode, augment JSON, write counts, print stats."""
    t_start = time.perf_counter()

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

    elapsed = time.perf_counter() - t_start
    n = len(locations)
    avg_speed = n / elapsed if elapsed > 0 else float('inf')
    print(f"\n  total time : {elapsed:.2f}s")
    print(f"  locations  : {n}")
    print(f"  avg speed  : {avg_speed:.1f} locations/s")


if __name__ == "__main__":
    main()