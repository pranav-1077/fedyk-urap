# Location Data Augmentation Pipeline

Augments JSON profile records with geographic coordinates and optionally enriches US locations with MSA (Metropolitan Statistical Area) data.

## Pipeline

```
augment_json.py          get_msa.py (optional)
     │                        │
     ▼                        ▼
collect unique          read augmented JSON
locations from     ──►  + location-year counts
input JSON              look up MSA via Geocodio
     │                        │
geocode via                   ▼
Nominatim              msa-year-counts.json
     │
     ▼
augmented-sample.json
location-year-counts.json
```

**Step 1** streams the input, parses and spell-corrects each location string, geocodes each unique location once via Nominatim (free, ~1 req/s), and writes coordinates into the records. Results are cached in `output_data/geocode_cache.json` so repeat runs only geocode new locations.

**Step 2** is run separately when MSA data is needed — it reads Step 1's output and reverse-geocodes US coordinates via Geocodio (parallelized, up to 15 req/s).

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add GEOCODIO_API_KEY (only needed for get_msa.py)
```

## Configuration

All paths and API settings live in `src/config.py`. API keys are loaded from `.env` at import time.

## Run

```bash
# Step 1: geocode all locations
python src/augment_json.py

# Step 2: MSA enrichment (optional)
python src/get_msa.py
```

Each run of Step 1 prints a summary to terminal:

```
  cache hits: 42/50  |  to geocode: 8

  total time : 9.34s
  locations  : 50
  avg speed  : 5.4 locations/s
```

## Location Parsing

`src/utils.py` parses raw location dicts into geocodable strings using the following priority:

1. `city + state + country` or `city + country` from structured fields (spell-corrected)
2. Same patterns inferred from the `raw` string (spell-corrected)
3. `state + country` from structured fields (spell-corrected)
4. `state + country` inferred from `raw` (spell-corrected)
5. `country` from structured fields (as-is)
6. `raw` spell-corrected as country, or returned as-is

Raw strings are parsed by comma count: 3 parts → city/state/country, 1 part → country, 2 parts → first token classified as city or state by fuzzy matching. Spell correction uses [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz) against reference lists from `geonamescache` and `pycountry`.

## File Structure

```
├── src/
│   ├── config.py                       # Paths, API keys, and settings
│   ├── utils.py                        # Shared parsing and logging utilities
│   ├── augment_json.py                 # Step 1
│   └── get_msa.py                      # Step 2
├── input_data/
│   └── sample_1000_20240401.json       # Input
├── output_data/
│   ├── geocode_cache.json              # Persistent geocoding cache
│   ├── augmented-sample.json           # Output: records with lat/lon
│   ├── location-year-counts.json       # Intermediate: for get_msa.py
│   └── msa-year-counts.json            # Output: MSA panel data
└── requirements.txt
```
