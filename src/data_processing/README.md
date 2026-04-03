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
    │   ├── DataDownloader.py # Air quality data downloader (2-month periods)
    │   ├── DataMerger.py     # Air quality data merger
    │   ├── DataOrganizer.py  # Data organization and time format unification
    │   ├── MissingValueHandler.py  # Missing value handling
    │   └── Structure.py      # Data structure definitions
    ├── taxi_handler/         # Taxi data processing
    │   ├── main.py           # Taxi data processing main entry
    │   ├── DataDownloader.py # Taxi data downloader
    │   ├── DataCleaner.py    # Data cleaning
    │   └── DataMerger.py     # Taxi data merger
    ├── tool/                 # Utility classes
    │   └── Parquet2Csv.py    # Parquet to CSV converter
    └── main.py               # Main pipeline entry
```

### Data Structure
```
data/
├── air_data/                 # Air quality data
│   ├── original_data/        # Original air quality data (2-month periods)
│   │   └── openaq_data_*.parquet
│   └── openaq_data_*/        # Air quality data organized by time range
│       ├── pm25/             # PM2.5 data
│       │   └── *.csv         # CSV files organized by station ID
│       └── location_mapping.csv  # Station ID to location mapping
├── taxi_data/                # Taxi data
│   ├── original_data/        # Original taxi data
│   │   ├── download_link.json # Download links for taxi data
│   │   ├── map_table/        # Taxi zone ID to real location mapping
│   │   │   └── *.csv
│   │   ├── *.parquet         # Original Parquet files
│   │   └── *.csv             # Original CSV files
│   ├── green_tripdata_*.csv  # Cleaned green taxi data
│   ├── yellow_tripdata_*.csv # Cleaned yellow taxi data
│   └── taxi_data_*/          # Merged taxi data (by time range)
│       ├── green_tripdata_merged.csv
│       └── yellow_tripdata_merged.csv
├── temp_data/                # Temporary data files
└── ...
```

## Project Progress

### Completed Work
1. **Air Quality Data Processing**
   - Air quality data downloader (DataDownloader.py)
     - Split large date ranges into 2-month periods to avoid API rate limits
     - Save original data to `air_data/original_data/`
     - Skip existing files to avoid re-downloading
     - Add 10-second delay between requests only when new files are downloaded
   - Air quality data merger (DataMerger.py)
     - Merge multiple 2-month period files into one
     - Sort by `datetime_hour` for chronological order
   - Fetch air quality data for New York City from OpenAQ API
   - Convert Parquet format to CSV format
   - Organize data by time range, parameter type, and station ID
   - Handle missing values (using file-wide average for filling)
   - Unify time format to YYYYMMDDHHMMSS (e.g., 20260101002758)

2. **Taxi Data Processing**
   - Taxi data downloader (DataDownloader.py)
     - Download taxi data based on time range and taxi type
     - Calculate actual month range based on date range (not hardcoded)
     - Avoid duplicate downloads (skip existing files)
   - Taxi data merger (DataMerger.py)
     - Merge cleaned monthly files into single files per taxi type
     - Sort by pickup time for chronological order
     - Create output folder with time range and task timestamp
   - Convert Parquet format to CSV format
   - Clean data, keeping only key fields
   - Filter invalid data (trip distance > 0, passenger count > 0, etc.)
   - Distinguish between green taxi (lpep_*) and yellow taxi (tpep_*) time fields
   - Unify time format to YYYYMMDDHHMMSS

3. **Data Processing Pipeline**
   - Created complete pipeline script (main.py)
   - Integrated air quality data and taxi data processing workflows
   - Support for command-line parameters (--end-date, --days, --download-taxi)
   - Implemented incremental data processing (only process uncleaned files)
   - Log system:
     - Each run creates a new timestamped log directory
     - Separate log files for air and taxi data
     - Streaming log output (real-time)
   - Unified task timestamp:
     - Air and taxi data use the same task timestamp for output folders
     - Makes it easy to match air and taxi data from the same run

## How to Run

### Log Files
Each run of the main pipeline generates a new log directory with timestamp:
```
logs/
├── 20260331_143022/          # Timestamp format: YYYYMMDD_HHMMSS
│   ├── air_data.log            # Air quality data processing logs
│   └── taxi_data.log           # Taxi data processing logs
└── ...
```

Logs are ignored by Git and won't be committed to the repository.

### Run the Main Pipeline
```bash
# Basic usage
python3 src/data_processing/main.py

# Specify end date and lookback days
python3 src/data_processing/main.py --end-date YYYY-MM-DD --days DAYS

# Download and process taxi data
python3 src/data_processing/main.py --download-taxi --end-date YYYY-MM-DD --days DAYS
```

### Run Air Quality Data Processing Separately
```bash
python3 src/data_processing/air_handler/main.py --end-date YYYY-MM-DD --days DAYS
```

### Run Taxi Data Processing Separately
```bash
# Download taxi data first
python3 src/data_processing/taxi_handler/main.py --download --end-date YYYY-MM-DD --days DAYS

# Download specific taxi types
python3 src/data_processing/taxi_handler/main.py --download --end-date YYYY-MM-DD --days DAYS --taxi-types yellow

# Default: check and process only unprocessed files
python3 src/data_processing/taxi_handler/main.py

# Force reprocessing of all files
python3 src/data_processing/taxi_handler/main.py --clean
```

### Use Taxi Downloader Directly
```bash
python3 src/data_processing/taxi_handler/DataDownloader.py --start-date YYYY-MM-DD --days DAYS --taxi-types yellow green
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
- Air quality data downloader splits large date ranges into 2-month periods to avoid rate limits
- Taxi data can be automatically downloaded using the `--download` or `--download-taxi` flags
- Taxi data can also be manually obtained from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Both air and taxi data downloaders will skip files that already exist to avoid duplicate downloads
- Both air and taxi data produce merged output files sorted by time
- Log files are generated in timestamped directories for each run
- Air and taxi output folders from the same run share the same task timestamp for easy matching
- Ensure necessary Python dependencies are installed before running

## Future Work
- Improve geographic location association
- Develop prediction models
- Build data visualization interface
- Write detailed project documentation
