#!/usr/bin/env python3
"""
Taxi Data Processing Pipeline
Implements taxi data downloading, conversion, and cleaning
"""
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

class TaxiDataPipeline:
    def __init__(self, input_dir: str = "./data/taxi_data/original_data/", 
                 output_dir: str = "./data/taxi_data/",
                 download_links_path: str = "./data/taxi_data/original_data/download_link.json"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.download_links_path = download_links_path
        
        # Ensure directories exist
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_data(self, end_date: datetime, days: int, taxi_types: list):
        """
        Download taxi data
        """
        print("\n" + "="*60)
        print("Step 1: Download taxi data")
        print("="*60)
        
        start_date = (end_date - timedelta(days=days)).strftime("%Y-%m-%d")
        
        print(f"Downloading taxi data:")
        print(f"  End date: {end_date.strftime('%Y-%m-%d')}")
        print(f"  Lookback days: {days}")
        print(f"  Start date: {start_date}")
        print(f"  Taxi types: {taxi_types}")
        
        cmd = [
            "python3", "src/data_processing/taxi_handler/DataDownloader.py",
            "--start-date", start_date,
            "--days", str(days),
            "--taxi-types"
        ] + taxi_types
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"Taxi data download failed with return code: {result.returncode}")
            return False
        
        return True
    
    def convert_parquet_to_csv(self):
        """
        Convert all Parquet files in the directory to CSV files
        """
        print("\n" + "="*60)
        print("Step 2: Convert Parquet files to CSV")
        print("="*60)
        
        cmd = [
            "python3", "src/data_processing/tool/Parquet2Csv.py",
            "--input-dir", str(self.input_dir)
        ]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"Parquet to CSV conversion failed with return code: {result.returncode}")
            return False
        
        return True
    
    def clean_data(self, force_all: bool = False):
        """
        Clean taxi data
        
        Parameters:
        -----------
        force_all : bool
            If True, clean all files; if False, only clean unprocessed files
        """
        print("\n" + "="*60)
        print("Step 3: Clean taxi data")
        print("="*60)
        
        cmd = [
            "python3", "src/data_processing/taxi_handler/DataCleaner.py",
            "--input-dir", str(self.input_dir),
            "--output-dir", str(self.output_dir)
        ]
        
        if not force_all:
            cmd.append("--check-only")
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"Taxi data cleaning failed with return code: {result.returncode}")
            return False
        
        return True
    
    def merge_data(self, start_date: datetime, end_date: datetime):
        """
        Merge taxi data files
        
        Parameters:
        -----------
        start_date : datetime
            Start date of the time range
        end_date : datetime
            End date of the time range
        """
        print("\n" + "="*60)
        print("Step 4: Merge taxi data")
        print("="*60)
        
        cmd = [
            "python3", "src/data_processing/taxi_handler/DataMerger.py",
            "--input-dir", str(self.output_dir),
            "--output-dir", str(self.output_dir),
            "--start-date", start_date.strftime("%Y-%m-%d"),
            "--end-date", end_date.strftime("%Y-%m-%d")
        ]
        
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"Data merging failed with return code: {result.returncode}")
            return False
        
        return True
    
    def run(self, end_date: datetime = None, days: int = 30, 
            taxi_types: list = None, download: bool = False,
            convert_only: bool = False, clean_all: bool = False):
        """
        Run the complete taxi data processing pipeline
        
        Parameters:
        -----------
        end_date : datetime
            End date for downloading
        days : int
            Number of days to look back
        taxi_types : list
            List of taxi types to download
        download : bool
            Whether to download taxi data first
        convert_only : bool
            Only convert Parquet files, do not clean data
        clean_all : bool
            Force clean all data (reprocess everything)
        """
        if end_date is None:
            end_date = datetime.now()
        
        if taxi_types is None:
            taxi_types = ["yellow", "green"]
        
        start_date = end_date - timedelta(days=days)
        
        print("Starting taxi data processing pipeline")
        
        if download:
            if not self.download_data(end_date, days, taxi_types):
                return False
        
        if not convert_only:
            if not self.convert_parquet_to_csv():
                return False
        
        if not convert_only:
            if not self.clean_data(force_all=clean_all):
                return False
        
        if not convert_only:
            if not self.merge_data(start_date, end_date):
                return False
        
        print("\n" + "="*60)
        print("Taxi data processing pipeline completed!")
        print("="*60)
        return True

def main():
    import argparse
    
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
    
    # Calculate end date
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = datetime.now()
    
    # Create and run pipeline
    pipeline = TaxiDataPipeline(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        download_links_path=args.download_links
    )
    
    pipeline.run(
        end_date=end_date,
        days=args.days,
        taxi_types=args.taxi_types,
        download=args.download,
        convert_only=args.convert_only,
        clean_all=args.clean
    )

if __name__ == "__main__":
    main()
