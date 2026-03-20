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

**Step 1** streams the input, geocodes each unique location once via Nominatim (free, ~1 req/s), and writes coordinates into the records.
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
│   ├── augmented-sample.json           # Output: records with lat/lon
│   ├── location-year-counts.json       # Intermediate: for get_msa.py
│   └── msa-year-counts.json            # Output: MSA panel data
└── requirements.txt
```
