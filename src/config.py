import os
import certifi
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


# --- environment ---
os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
load_dotenv()

# --- Paths ---
INPUT_PATH = 'input_data/sample_5_20240401.json'

_stem = os.path.splitext(os.path.basename(INPUT_PATH))[0]
AUGMENTED_PATH = f'output_data/augmented-{_stem}.json'
MSA_YEAR_COUNTS_PATH = f'output_data/msa-year-counts-{_stem}.json'
LOCATION_YEAR_COUNTS_PATH = f'output_data/location-year-counts-{_stem}.json'

# --- API Keys ---
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')

# --- Nominatim ---
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

# --- Geocodio ---
GEOCODIO_CALLS_PER_SECOND = 15
GEOCODIO_MAX_WORKERS = 10

# --- Logging ---
LOG_DIR = 'logs'
LOG_FILE = 'output.log'
