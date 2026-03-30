#!/usr/bin/env python3
"""
Data Processing Pipeline
Integrates air quality data and taxi data processing workflows
"""
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

class DataPipeline:
    def __init__(self):
        self.taxi_original_dir = Path("./data/taxi_data/original_data")
        self.taxi_output_dir = Path("./data/taxi_data")
        
        # Ensure directories exist
        self.taxi_original_dir.mkdir(parents=True, exist_ok=True)
        self.taxi_output_dir.mkdir(parents=True, exist_ok=True)
    
    def process_air_data(self, date_from: str, date_to: str):
        """
        Process air quality data
        """
        print("\n" + "="*70)
        print("Starting air quality data processing")
        print("="*70)
        
        # Call air quality data processing pipeline
        cmd = [
            "python3", "src/data_processing/air_handler/main.py",
            "--end-date", date_to[:10],
            "--days", str((datetime.strptime(date_to[:10], "%Y-%m-%d") - datetime.strptime(date_from[:10], "%Y-%m-%d")).days)
        ]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Air quality data processing failed: {result.stderr}")
            return False
        
        print("Air quality data processing successful")
        return True
    
    def process_taxi_data(self):
        """
        Check and process taxi data
        """
        print("\n" + "="*70)
        print("Starting taxi data processing")
        print("="*70)
        
        # Call taxi data processing script
        cmd = ["python3", "src/data_processing/taxi_handler/main.py", "--check-only"]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Taxi data processing failed: {result.stderr}")
            return False
        
        print("Taxi data processing successful")
        return True
    
    def run(self, date_from: str, date_to: str):
        """
        Run the complete pipeline
        """
        print("Starting data processing pipeline")
        print(f"Time range: {date_from} to {date_to}")
        
        # Step 1: Process air quality data
        if not self.process_air_data(date_from, date_to):
            print("Pipeline failed: Air quality data processing failed")
            return False
        
        # Step 2: Process taxi data
        if not self.process_taxi_data():
            print("Pipeline failed: Taxi data processing failed")
            return False
        
        print("\n" + "="*70)
        print("Data processing pipeline completed!")
        print("="*70)
        return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Processing Pipeline")
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
    pipeline = DataPipeline()
    pipeline.run(start_date, end_date)

if __name__ == "__main__":
    main()
