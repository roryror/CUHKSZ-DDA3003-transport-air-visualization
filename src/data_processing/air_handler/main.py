#!/usr/bin/env python3
"""
Air Quality Data Processing Pipeline
Implements air quality data fetching, conversion, organization, and missing value handling
"""
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

class AirDataPipeline:
    def __init__(self):
        self.base_dir = Path("./data/temp_data")
        self.organized_dir = Path("./data/air_data")
        self.api_key = "1b98793765584b56e2138f8ffc9b858f8fac77df971256fb75e7d3b734efdec8"
        
        # NYC bounding box (min_lon, min_lat, max_lon, max_lat)
        self.nyc_bbox = (-74.018707, 40.641819, -73.8244, 40.868)
        
        # Ensure directories exist
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.organized_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch_data(self, date_from: str, date_to: str) -> str:
        """
        Fetch data from OpenAQ API
        """
        print("\n" + "="*60)
        print("Step 1: Fetch data from OpenAQ API")
        print("="*60)
        
        from AirDataDownloader import AirDataDownloader
        from AirDataMerger import merge_air_data
        
        # Step 1: Download monthly data
        downloader = AirDataDownloader(
            api_key=self.api_key,
            output_dir="./data/air_data/original_data/"
        )
        downloaded_files = downloader.download(date_from, date_to, self.nyc_bbox)
        
        if not downloaded_files:
            print("No files downloaded")
            return None
        
        # Step 2: Merge monthly files
        merged_file = merge_air_data(
            input_dir="./data/air_data/original_data/",
            output_dir="./data/temp_data/",
            start_date=date_from,
            end_date=date_to
        )
        
        if not merged_file:
            print("Merge failed")
            return None
        
        return merged_file
    
    def convert_to_csv(self, parquet_file: str) -> str:
        """
        Convert Parquet file to CSV
        """
        print("\n" + "="*60)
        print("Step 2: Convert Parquet file to CSV")
        print("="*60)
        
        csv_file = parquet_file.replace(".parquet", ".csv")
        
        # Check if file already exists
        if Path(csv_file).exists():
            print(f"File {csv_file} already exists, skipping conversion")
            return csv_file
        
        # Call Parquet2Csv.py for conversion
        cmd = ["python3", "src/data_processing/tool/Parquet2Csv.py", "--input", parquet_file]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Print command output
        print(f"Command output: {result.stdout}")
        if result.stderr:
            print(f"Command error: {result.stderr}")
        
        if result.returncode != 0:
            print(f"Conversion failed: {result.stderr}")
            return None
        
        print(f"Conversion successful: {csv_file}")
        return csv_file
    
    def organize_data(self, csv_file: str) -> str:
        """
        Organize data into structured directory
        """
        print("\n" + "="*60)
        print("Step 3: Organize data into structured directory")
        print("="*60)
        
        # Call DataOrganizer.py to organize data
        cmd = ["python3", "src/data_processing/air_handler/DataOrganizer.py", "--input", csv_file]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Print command output
        print(f"Command output: {result.stdout}")
        if result.stderr:
            print(f"Command error: {result.stderr}")
        
        if result.returncode != 0:
            print(f"Organization failed: {result.stderr}")
            return None
        
        # Extract output directory
        for line in result.stdout.split('\n'):
            if "Output directory:" in line:
                output_dir = line.split("Output directory:")[1].strip()
                print(f"Organization successful: {output_dir}")
                return output_dir
        
        return None
    
    def handle_missing_values(self, organized_dir: str):
        """
        Handle missing values
        """
        print("\n" + "="*60)
        print("Step 4: Handle missing values")
        print("="*60)
        
        # Call MissingValueHandler.py to handle missing values
        cmd = ["python3", "src/data_processing/air_handler/MissingValueHandler.py", "--input-dir", organized_dir]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Print command output
        print(f"Command output: {result.stdout}")
        if result.stderr:
            print(f"Command error: {result.stderr}")
        
        if result.returncode != 0:
            print(f"Missing value handling failed: {result.stderr}")
            return False
        
        print("Missing value handling successful")
        return True
    
    def cleanup(self, parquet_file: str, csv_file: str):
        """
        Delete original data files
        """
        print("\n" + "="*60)
        print("Step 5: Clean up original data files")
        print("="*60)
        
        # Delete Parquet file
        if parquet_file and Path(parquet_file).exists():
            Path(parquet_file).unlink()
            print(f"Deleted: {parquet_file}")
        
        # Delete CSV file
        if csv_file and Path(csv_file).exists():
            Path(csv_file).unlink()
            print(f"Deleted: {csv_file}")
        
        print("Cleanup completed")
    
    def run(self, date_from: str, date_to: str):
        """
        Run the complete pipeline
        """
        print("Starting air quality data processing pipeline")
        print(f"Time range: {date_from} to {date_to}")
        
        # Step 1: Fetch data
        parquet_file = self.fetch_data(date_from, date_to)
        if not parquet_file:
            print("Pipeline failed: Data fetching failed")
            return False
        
        # Step 2: Convert to CSV
        csv_file = self.convert_to_csv(parquet_file)
        if not csv_file:
            print("Pipeline failed: Conversion failed")
            return False
        
        # Step 3: Organize data
        organized_dir = self.organize_data(csv_file)
        if not organized_dir:
            print("Pipeline failed: Organization failed")
            return False
        
        # Step 4: Handle missing values
        if not self.handle_missing_values(organized_dir):
            print("Pipeline failed: Missing value handling failed")
            return False
        
        # Step 5: Cleanup
        self.cleanup(parquet_file, csv_file)
        
        print("\n" + "="*60)
        print("Air quality data processing pipeline completed!")
        print(f"Processing results saved in: {organized_dir}")
        print("="*60)
        return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Air Quality Data Processing Pipeline")
    parser.add_argument("--end-date", type=str, help="End date, format: YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    
    args = parser.parse_args()
    
    # Calculate end date
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").strftime("%Y-%m-%dT23:59:59Z")
    else:
        end_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
    
    # Calculate start date
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
    start_date = (end_date_obj - timedelta(days=args.days)).strftime("%Y-%m-%dT00:00:00Z")
    
    print(f"Specified parameters:")
    print(f"  End date: {end_date}")
    print(f"  Lookback days: {args.days}")
    print(f"  Start date: {start_date}")
    
    # Run pipeline
    pipeline = AirDataPipeline()
    pipeline.run(start_date, end_date)

if __name__ == "__main__":
    main()
