# Data Processing Project

## Project Overview
This project aims to process and integrate air quality data and taxi data in New York City, providing a foundation for subsequent data analysis and modeling.

## Project Structure

### Code Structure
```
src/
└── data_processing/          # Data processing related code
    ├── air_handler/          # Air quality data processing
    │   ├── main.py           # Air quality data processing main entry
    │   ├── OpenAQFetcher.py  # OpenAQ API data fetching
    │   ├── DataConverter.py  # Data format conversion
    │   ├── DataOrganizer.py  # Data organization and time format unification
    │   └── MissingValueHandler.py  # Missing value handling
    ├── taxi_handler/         # Taxi data processing
    │   ├── main.py           # Taxi data processing main entry
    │   └── DataCleaner.py    # Data cleaning
    ├── tool/                 # Utility classes
    │   └── __init__.py
    └── main.py               # Main pipeline entry
```

### Data Structure
```
data/
├── air_data/                 # Air quality data
│   └── openaq_data_*/        # Air quality data organized by time range
│       ├── pm25/             # PM2.5 data
│       │   └── *.csv         # CSV files organized by station ID
│       └── location_mapping.csv  # Station ID to location mapping
└── taxi_data/                # Taxi data
    ├── original_data/        # Original data
    │   ├── map_table/        # Taxi zone ID to real location mapping
    │   │   └── *.csv
    │   ├── green_tripdata_*.csv  # Green taxi data
    │   └── yellow_tripdata_*.csv # Yellow taxi data
    ├── green_tripdata_*.csv  # Cleaned green taxi data
    └── yellow_tripdata_*.csv # Cleaned yellow taxi data
```

## Project Progress

### Completed Work
1. **Air Quality Data Processing**
   - Fetch air quality data for New York City from OpenAQ API
   - Convert Parquet format to CSV format
   - Organize data by time range, parameter type, and station ID
   - Handle missing values (using file-wide average for filling)
   - Unify time format to YYYYMMDDHHMMSS (e.g., 20260101002758)

2. **Taxi Data Processing**
   - Convert Parquet format to CSV format
   - Clean data, keeping only key fields
   - Filter invalid data (trip distance > 0, passenger count > 0, etc.)
   - Distinguish between green taxi (lpep_*) and yellow taxi (tpep_*) time fields
   - Unify time format to YYYYMMDDHHMMSS

3. **Data Processing Pipeline**
   - Created complete pipeline script (main.py)
   - Integrated air quality data and taxi data processing workflows
   - Support for command-line parameters (--end-date, --days)
   - Implemented incremental data processing (only process uncleaned files)

## How to Run

### Run the Main Pipeline
```bash
# Basic usage
python3 src/data_processing/main.py

# Specify end date and lookback days
python3 src/data_processing/main.py --end-date 2026-03-30 --days 7
```

### Run Air Quality Data Processing Separately
```bash
python3 src/data_processing/air_handler/main.py --end-date 2026-03-30 --days 7
```

### Run Taxi Data Processing Separately
```bash
# Check and process uncleaned files
python3 src/data_processing/taxi_handler/main.py --check-only

# Force reprocessing of all files
python3 src/data_processing/taxi_handler/main.py --clean-only
```

## Data Format Description

### Air Quality Data (CSV)
- `location_id`: Station ID
- `datetime_hour`: Time
- `value`: Air quality value
- `unit`: Unit
- `averaging_period`: Averaging period
- `latitude`: Latitude
- `longitude`: Longitude

### Taxi Data (CSV)
- `VendorID`: Vendor ID
- `lpep_pickup_datetime`/`tpep_pickup_datetime`
- `lpep_dropoff_datetime`/`tpep_dropoff_datetime`
- `passenger_count`: Passenger count
- `trip_distance`: Trip distance
- `PULocationID`: Pickup location ID
- `DOLocationID`: Dropoff location ID

## Notes
- Air quality data is sourced from the OpenAQ API, which may be subject to API rate limits
- Taxi data needs to be placed in the `data/taxi_data/original_data/` directory first
- Taxi data can be obtained from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Ensure necessary Python dependencies are installed before running

## Future Work
- Improve geographic location association
- Develop prediction models
- Build data visualization interface
- Write detailed project documentation
