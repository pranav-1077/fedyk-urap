# Location Data Augmentation Pipeline

## Overview

A Python utility that augments JSON records containing location data with geographic coordinates and Metropolitan Statistical Area (MSA) information. The pipeline processes professional profile data, geocodes locations globally using Google Maps, and enriches US locations with MSA data via Geocodio.

**Key features:**
- Global geocoding via Google Maps API
- MSA enrichment for US locations via Geocodio API
- Rule-based location parsing (city, state, country)
- Memory-efficient streaming with ijson for large files
- Avoid making repeat API requests for duplicate locations by tracking unique locations only
- Parallelized API requests with rate limiting
- Generates MSA-year count aggregations for panel data analysis

## Codebase Structure

```
fedyk-urap-takehome/
├── data/
│   ├── sample_1000_20240401.json   # Input data
│   ├── augmented-sample.json       # Output: augmented records
│   └── msa-year-counts.json        # Output: MSA panel data
├── src/
│   └── augment_json.py             # Main script
├── requirements.txt
├── .env.example
└── README.md
```

## Implementation Details

### Key Functions

| Function | Purpose |
|----------|---------|
| `parse_location()` | Formats location dict to string (city, state, country) |
| `collect_locations()` | Extracts unique locations + builds year counts |
| `encode_location()` | Geocodes single location (Google Maps + Geocodio) |
| `get_msa_from_coords()` | Reverse geocodes coordinates to get MSA |
| `request_locations()` | Orchestrates parallel geocoding |
| `augment_json()` | Writes augmented JSON with coordinates |
| `write_msa_year_counts()` | Writes MSA panel data |

### Rate Limiting

- Google Maps: 40 requests/second (enforced via `@limits` decorator)
- Geocodio: 15 requests/second (called only for US addresses)

## Setup & How to Run

### 1. Create virtual environment
python version: 3.12.0
```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure API keys

```bash
cp .env.example .env
```

Edit `.env` with your API keys:
```
GOOGLE_API_KEY=your_google_maps_api_key
GEOCODIO_API_KEY=your_geocodio_api_key
```

- Google Maps API: [Google Cloud Console](https://console.cloud.google.com/) (enable Geocoding API)
- Geocodio API: [dash.geocod.io](https://dash.geocod.io) (2,500 free lookups/day)

### 4. Run

```bash
python src/augment_json.py
```

### Output

- `data/augmented-sample.json` - Input records with added `lat`, `lon`, `msa_name`, `msa_code` fields
- `data/msa-year-counts.json` - Aggregated counts by MSA and year
