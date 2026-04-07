import os
import threading
import certifi
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geonamescache
import pycountry

# ===============================================================================
# Environment
# ===============================================================================
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
load_dotenv()

# ===============================================================================
# Paths
# ===============================================================================
INPUT_PATH = 'input_data/sample_1000_20240401.json'
_stem = os.path.splitext(os.path.basename(INPUT_PATH))[0]
AUGMENTED_PATH = f'output_data/augmented-{_stem}.json'
MSA_YEAR_COUNTS_PATH = f'output_data/msa-year-counts-{_stem}.json'
LOCATION_YEAR_COUNTS_PATH = f'output_data/location-year-counts-{_stem}.json'
GEOCODE_CACHE_DB = 'output_data/geocode_cache.db'

# ===============================================================================
# API keys
# ===============================================================================
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')

# ===============================================================================
# Nomatim
# ===============================================================================
NOMINATIM_USER_AGENT = 'fedyk-urap-location-enrichment/1.0'
NOMINATIM_MIN_DELAY = 1.1
NOMINATIM_ERROR_WAIT = 5.0
geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=NOMINATIM_MIN_DELAY,
    error_wait_seconds=NOMINATIM_ERROR_WAIT,
    return_value_on_exception=None,
)

# ===============================================================================
# Mapbox
# ===============================================================================
MAPBOX_ACCESS_TOKEN = os.getenv('MAPBOX_ACCESS_TOKEN')
MAPBOX_MAX_WORKERS = 10
MAPBOX_MIN_DELAY = 0.1    # free tier: 10 req/s; updated dynamically from response headers
MAPBOX_MAX_RETRIES = 3    # retries on transient 429s before giving up
MAPBOX_WEEKLY_LIMIT = 20_000
MAPBOX_RATE_LOCK = threading.Lock()

# ===============================================================================
# Geocodio
# ===============================================================================
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')
GEOCODIO_CALLS_PER_SECOND = 15
GEOCODIO_MAX_WORKERS = 10

# ===============================================================================
# Logging
# ===============================================================================
LOG_DIR = 'logs'
LOG_FILE = 'output.log'

# ===============================================================================
# Spell checking 
# ===============================================================================
_gc = geonamescache.GeonamesCache()
CITY_NAMES = sorted({c['name'] for c in _gc.get_cities().values()})
COUNTRY_NAMES = sorted({c['name'] for c in _gc.get_countries().values()})
STATE_NAMES = sorted({s.name for s in pycountry.subdivisions})
SPELL_THRESHOLD = 80  # min fuzzy score (0–100) to accept a correction