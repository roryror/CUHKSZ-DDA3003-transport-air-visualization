#!/usr/bin/env python3
"""
Taxi Data Processing Tool
Cleans and processes NYC TLC taxi data
"""
import pandas as pd
import os
from pathlib import Path
import argparse
import glob
from datetime import datetime, timedelta
import sys

sys.path.append(str(Path(__file__).parent))
from TaxiDownloader import TaxiDownloader

def download_taxi_data(download_links_path: str, output_dir: str, start_date: str, days: int, taxi_types: list):
    """
    Download taxi data
    
    Parameters:
    -----------
    download_links_path : str
        Path to download_link.json
    output_dir : str
        Output directory
    start_date : str
        Start date in YYYY-MM-DD format
    days : int
        Number of days to look back
    taxi_types : list
        List of taxi types to download
    """
    downloader = TaxiDownloader(download_links_path, output_dir)
    downloader.download(start_date, days, taxi_types)

def convert_parquet_to_csv(input_dir: str):
    """
    Convert all Parquet files in the directory to CSV files
    
    Parameters:
    -----------
    input_dir : str
        Input directory path
    """
    input_base = Path(input_dir)
    
    # Check if directory exists
    if not input_base.exists():
        print(f"Error: Directory {input_dir} does not exist!")
        return
    
    # Find all Parquet files
    parquet_files = list(input_base.glob("*.parquet"))
    
    if not parquet_files:
        print(f"No Parquet files found in {input_dir}")
        return
    
    print(f"Found {len(parquet_files)} Parquet files")
    
    converted_count = 0
    error_count = 0
    
    for parquet_file in parquet_files:
        print(f"\nConverting file: {parquet_file.name}")
        
        try:
            # Read Parquet file
            df = pd.read_parquet(parquet_file)
            
            # Build output CSV filename
            csv_file = parquet_file.with_suffix(".csv")
            
            # Check if CSV file already exists
            if csv_file.exists():
                print(f"  CSV file already exists, skipping conversion")
                continue
            
            # Save as CSV file
            df.to_csv(csv_file, index=False, encoding='utf-8')
            print(f"  Conversion successful: {csv_file.name}")
            converted_count += 1
            
        except Exception as e:
            print(f"  Conversion failed: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n" + "="*50)
    print("Conversion Summary:")
    print(f"  Successfully converted: {converted_count} files")
    print(f"  Failed to convert: {error_count} files")
    print(f"  Total files processed: {len(parquet_files)}")

def clean_taxi_data(input_dir: str, output_dir: str):
    """
    Clean taxi data, keeping only required fields
    
    Parameters:
    -----------
    input_dir : str
        Input directory path
    output_dir : str
        Output directory path
    """
    input_base = Path(input_dir)
    output_base = Path(output_dir)
    
    # Ensure output directory exists
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Check if input directory exists
    if not input_base.exists():
        print(f"Error: Directory {input_dir} does not exist!")
        return
    
    # Find all CSV files starting with green or yellow
    csv_files = list(input_base.glob("green_*.csv")) + list(input_base.glob("yellow_*.csv"))
    
    if not csv_files:
        print(f"No CSV files starting with green or yellow found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} taxi data files")
    
    processed_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        print(f"\nProcessing file: {csv_file.name}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, low_memory=False)
            
            # Determine time field names
            if 'green' in csv_file.name.lower():
                # Green taxis use lpep time fields
                pickup_col = 'lpep_pickup_datetime'
                dropoff_col = 'lpep_dropoff_datetime'
            else:
                # Yellow taxis use tpep time fields
                pickup_col = 'tpep_pickup_datetime'
                dropoff_col = 'tpep_dropoff_datetime'
            
            # Required fields to keep
            keep_columns = [
                'VendorID',
                pickup_col,
                dropoff_col,
                'passenger_count',
                'trip_distance',
                'PULocationID',
                'DOLocationID'
            ]
            
            # Check if all required columns exist
            missing_columns = [col for col in keep_columns if col not in df.columns]
            if missing_columns:
                print(f"  Warning: Missing columns: {missing_columns}")
                # Keep only existing columns
                available_columns = [col for col in keep_columns if col in df.columns]
                if not available_columns:
                    print(f"  Error: No required columns found")
                    error_count += 1
                    continue
            else:
                available_columns = keep_columns
            
            # Keep only required columns
            cleaned_df = df[available_columns].copy()
            
            # Filter invalid data
            # 1. Trip distance greater than 0
            cleaned_df = cleaned_df[cleaned_df['trip_distance'] > 0]
            
            # 2. Passenger count greater than 0 (if column exists)
            if 'passenger_count' in cleaned_df.columns:
                cleaned_df = cleaned_df[cleaned_df['passenger_count'] > 0]
            
            # 3. Pickup time before dropoff time
            if pickup_col in cleaned_df.columns and dropoff_col in cleaned_df.columns:
                # Convert to datetime type
                cleaned_df[pickup_col] = pd.to_datetime(cleaned_df[pickup_col], errors='coerce')
                cleaned_df[dropoff_col] = pd.to_datetime(cleaned_df[dropoff_col], errors='coerce')
                
                # Filter invalid times and records where pickup time > dropoff time
                cleaned_df = cleaned_df.dropna(subset=[pickup_col, dropoff_col])
                cleaned_df = cleaned_df[cleaned_df[pickup_col] < cleaned_df[dropoff_col]]
                
                # Convert time format to unified format: 20260101002758
                cleaned_df[pickup_col] = cleaned_df[pickup_col].dt.strftime('%Y%m%d%H%M%S')
                cleaned_df[dropoff_col] = cleaned_df[dropoff_col].dt.strftime('%Y%m%d%H%M%S')
            
            # 4. Filter records with location ID 0
            for loc_col in ['PULocationID', 'DOLocationID']:
                if loc_col in cleaned_df.columns:
                    cleaned_df = cleaned_df[cleaned_df[loc_col] > 0]
            
            # Build output filename
            output_file = output_base / csv_file.name
            
            # Save cleaned data
            cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"  Cleaning successful: {output_file.name}")
            print(f"  Original data: {len(df):,} rows")
            print(f"  Cleaned data: {len(cleaned_df):,} rows")
            print(f"  Kept fields: {available_columns}")
            processed_count += 1
            
        except Exception as e:
            print(f"  Processing failed: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n" + "="*50)
    print("Cleaning Summary:")
    print(f"  Successfully processed: {processed_count} files")
    print(f"  Failed to process: {error_count} files")
    print(f"  Total files processed: {len(csv_files)}")

def check_and_clean_taxi_data(original_dir: str, output_dir: str):
    """
    Check and clean taxi data
    Only process files that haven't been cleaned yet
    
    Parameters:
    -----------
    original_dir : str
        Original data directory path
    output_dir : str
        Output directory path
    """
    original_base = Path(original_dir)
    output_base = Path(output_dir)
    
    # Ensure output directory exists
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Check if original directory exists
    if not original_base.exists():
        print(f"Error: Directory {original_dir} does not exist!")
        return
    
    # Find all CSV files starting with green or yellow
    csv_files = list(original_base.glob("green_*.csv")) + list(original_base.glob("yellow_*.csv"))
    
    if not csv_files:
        print(f"No CSV files starting with green or yellow found in {original_dir}")
        return
    
    # Identify files to process (not existing in output directory)
    files_to_process = []
    for csv_file in csv_files:
        output_file = output_base / csv_file.name
        if not output_file.exists():
            files_to_process.append(csv_file)
    
    if not files_to_process:
        print("All taxi data files have been cleaned, no need to process")
        return
    
    print(f"Found {len(files_to_process)} taxi data files that need cleaning")
    
    processed_count = 0
    error_count = 0
    
    for csv_file in files_to_process:
        print(f"\nProcessing file: {csv_file.name}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, low_memory=False)
            
            # Determine time field names
            if 'green' in csv_file.name.lower():
                # Green taxis use lpep time fields
                pickup_col = 'lpep_pickup_datetime'
                dropoff_col = 'lpep_dropoff_datetime'
            else:
                # Yellow taxis use tpep time fields
                pickup_col = 'tpep_pickup_datetime'
                dropoff_col = 'tpep_dropoff_datetime'
            
            # Required fields to keep
            keep_columns = [
                'VendorID',
                pickup_col,
                dropoff_col,
                'passenger_count',
                'trip_distance',
                'PULocationID',
                'DOLocationID'
            ]
            
            # Check if all required columns exist
            missing_columns = [col for col in keep_columns if col not in df.columns]
            if missing_columns:
                print(f"  Warning: Missing columns: {missing_columns}")
                # Keep only existing columns
                available_columns = [col for col in keep_columns if col in df.columns]
                if not available_columns:
                    print(f"  Error: No required columns found")
                    error_count += 1
                    continue
            else:
                available_columns = keep_columns
            
            # Keep only required columns
            cleaned_df = df[available_columns].copy()
            
            # Filter invalid data
            # 1. Trip distance greater than 0
            cleaned_df = cleaned_df[cleaned_df['trip_distance'] > 0]
            
            # 2. Passenger count greater than 0 (if column exists)
            if 'passenger_count' in cleaned_df.columns:
                cleaned_df = cleaned_df[cleaned_df['passenger_count'] > 0]
            
            # 3. Pickup time before dropoff time
            if pickup_col in cleaned_df.columns and dropoff_col in cleaned_df.columns:
                # Convert to datetime type
                cleaned_df[pickup_col] = pd.to_datetime(cleaned_df[pickup_col], errors='coerce')
                cleaned_df[dropoff_col] = pd.to_datetime(cleaned_df[dropoff_col], errors='coerce')
                
                # Filter invalid times and records where pickup time > dropoff time
                cleaned_df = cleaned_df.dropna(subset=[pickup_col, dropoff_col])
                cleaned_df = cleaned_df[cleaned_df[pickup_col] < cleaned_df[dropoff_col]]
                
                # Convert time format to unified format: 20260101002758
                cleaned_df[pickup_col] = cleaned_df[pickup_col].dt.strftime('%Y%m%d%H%M%S')
                cleaned_df[dropoff_col] = cleaned_df[dropoff_col].dt.strftime('%Y%m%d%H%M%S')
            
            # 4. Filter records with location ID 0
            for loc_col in ['PULocationID', 'DOLocationID']:
                if loc_col in cleaned_df.columns:
                    cleaned_df = cleaned_df[cleaned_df[loc_col] > 0]
            
            # Build output filename
            output_file = output_base / csv_file.name
            
            # Save cleaned data
            cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
            
            print(f"  Cleaning successful: {output_file.name}")
            print(f"  Original data: {len(df):,} rows")
            print(f"  Cleaned data: {len(cleaned_df):,} rows")
            print(f"  Kept fields: {available_columns}")
            processed_count += 1
            
        except Exception as e:
            print(f"  Processing failed: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n" + "="*50)
    print("Cleaning Summary:")
    print(f"  Successfully processed: {processed_count} files")
    print(f"  Failed to process: {error_count} files")
    print(f"  Total files processed: {len(files_to_process)}")

def main():
    parser = argparse.ArgumentParser(description="Taxi Data Processing Tool")
    parser.add_argument("--input-dir", type=str, default="./data/taxi_data/original_data/", help="Input directory")
    parser.add_argument("--output-dir", type=str, default="./data/taxi_data/", help="Output directory")
    parser.add_argument("--download-links", type=str, default="./data/taxi_data/original_data/download_link.json", help="Path to download_link.json")
    parser.add_argument("--download", action="store_true", help="Download taxi data first")
    parser.add_argument("--end-date", type=str, help="End date, format: YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--taxi-types", type=str, nargs="+", default=["yellow", "green"], help="Taxi types to download (yellow, green)")
    parser.add_argument("--convert-only", action="store_true", help="Only convert Parquet files, do not clean data")
    parser.add_argument("--clean", action="store_true", help="Force clean all data (reprocess everything)")
    
    args = parser.parse_args()
    
    # Ensure directories exist
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.download:
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now()
        
        start_date = (end_date - timedelta(days=args.days)).strftime("%Y-%m-%d")
        
        print(f"Downloading taxi data:")
        print(f"  End date: {end_date.strftime('%Y-%m-%d')}")
        print(f"  Lookback days: {args.days}")
        print(f"  Start date: {start_date}")
        print(f"  Taxi types: {args.taxi_types}")
        
        download_taxi_data(args.download_links, args.input_dir, start_date, args.days, args.taxi_types)
    
    if args.clean:
        # Force clean all data
        if not args.convert_only:
            # Convert Parquet files to CSV
            convert_parquet_to_csv(args.input_dir)
        
        # Clean taxi data
        clean_taxi_data(args.input_dir, args.output_dir)
    else:
        # Default: convert any new Parquet files and check unprocessed files
        if not args.convert_only:
            # Convert Parquet files to CSV (only convert new ones)
            convert_parquet_to_csv(args.input_dir)
        
        if not args.convert_only:
            # Check and clean unprocessed files
            check_and_clean_taxi_data(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()
