# Location Data Augmentation Pipeline

Augments JSON profile records with geographic coordinates and optionally enriches US locations with MSA (Metropolitan Statistical Area) data.

## Pipeline

```
Step 1: augment_json.py          Step 2: get_msa.py (optional)
         │                                  │
         ▼                                  ▼
 collect & parse                    read augmented JSON
 unique locations       ────►       look up MSA via Geocodio
         │                                  │
 geocode via Mapbox                         ▼
 (parallel, ~10 req/s)             msa-year-counts.json
         │
         │ if weekly limit hit
         ▼
 geocode via Nominatim
 (serial, 1 req/s)
         │
         ▼
 augmented output + location-year counts
```

**Step 1** parses and spell-corrects each location string, then geocodes unique locations via Mapbox (parallel, up to 10 req/s). Usage is self-tracked against a 20,000 request weekly cap — once reached, remaining locations fall back to Nominatim. All results are cached in a local SQLite database so repeat runs only geocode new locations.

**Step 2** is optional — reads Step 1's output and enriches US locations with MSA data via Geocodio.

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Required keys in `.env`:

| Key | Used by |
|---|---|
| `MAPBOX_ACCESS_TOKEN` | Step 1 |
| `GEOCODIO_API_KEY` | Step 2 only |

## Run

```bash
python src/augment_json.py   # Step 1
python src/get_msa.py        # Step 2 (optional)
```

Each run prints a progress summary:

```
  mapbox budget  : 18,432 / 20,000 remaining this week
  cache hits     : 42/50
  to geocode     : 8

  total time : 9.34s
  locations  : 50
  avg speed  : 5.4 locations/s
```

If the weekly Mapbox limit is reached mid-run, a notice is printed and remaining locations are routed to Nominatim automatically.

## Location Parsing

Each location is parsed from structured fields (city, state, country) where available, falling back to the raw string. Ambiguous raw strings are classified by fuzzy matching against a global city and state reference list. All resolved components are spell-corrected before geocoding.

## File Structure

```
├── src/
│   ├── config.py              # Paths, API keys, and settings
│   ├── utils.py               # Location parsing and logging
│   ├── augment_json.py        # Step 1
│   └── get_msa.py             # Step 2
├── input_data/
│   └── sample_5_20240401.json
├── output_data/
│   ├── geocode_cache.db       # Persistent geocoding cache
│   ├── augmented-*.json       # Records with coordinates
│   ├── location-year-counts-*.json
│   └── msa-year-counts-*.json
└── requirements.txt
```
