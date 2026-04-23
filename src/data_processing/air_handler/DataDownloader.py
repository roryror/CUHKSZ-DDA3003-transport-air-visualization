#!/usr/bin/env python3
"""
Air Quality Data Downloader
Downloads air quality data from OpenAQ API, split by 2 months
"""
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import subprocess


class DataDownloader:
    def __init__(self, api_key: str, output_dir: str = "./data/air_data/original_data/"):
        """
        Initialize DataDownloader
        
        Parameters:
        -----------
        api_key : str
            OpenAQ API key
        output_dir : str
            Output directory for downloaded files
        """
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def calculate_month_ranges(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Calculate date ranges split by 2 months
        
        Parameters:
        -----------
        start_date : datetime
            Start date
        end_date : datetime
            End date
        
        Returns:
        --------
        List[Dict]
            List of dicts with 'start' and 'end' datetime objects for each 2-month period
        """
        month_ranges = []
        
        current_date = start_date.replace(day=1)
        
        while current_date <= end_date:
            period_start = current_date
            # Calculate end of 2-month period
            if current_date.month >= 11:
                # Handle December case
                period_end = current_date.replace(year=current_date.year + 1, month=(current_date.month + 2) % 12 or 12, day=1) - timedelta(days=1)
            else:
                period_end = current_date.replace(month=current_date.month + 2, day=1) - timedelta(days=1)
            
            # Adjust to actual date range
            actual_start = max(period_start, start_date)
            actual_end = min(period_end, end_date)
            
            # Generate period identifier
            start_str = actual_start.strftime("%Y-%m")
            end_str = actual_end.strftime("%Y-%m")
            period_name = f"{start_str}_to_{end_str}"
            
            month_ranges.append({
                'start': actual_start,
                'end': actual_end,
                'period_name': period_name
            })
            
            # Move to next 2-month period
            if current_date.month >= 11:
                current_date = current_date.replace(year=current_date.year + 1, month=(current_date.month + 2) % 12 or 12, day=1)
            else:
                current_date = current_date.replace(month=current_date.month + 2, day=1)
        
        return month_ranges
    
    def get_file_path(self, period_name: str) -> Path:
        """
        Get file path for a specific period
        
        Parameters:
        -----------
        period_name : str
            Period name in format "YYYY-MM_to_YYYY-MM"
        
        Returns:
        --------
        Path
            File path
        """
        return self.output_dir / f"openaq_data_{period_name}.parquet"
    
    def download_period(
        self,
        start_date: datetime,
        end_date: datetime,
        period_name: str,
        bbox: tuple,
        max_retries: int = 4,
    ) -> tuple[Optional[Path], bool]:
        """
        Download data for a single 2-month period
        
        Parameters:
        -----------
        start_date : datetime
            Start date of the period
        end_date : datetime
            End date of the period
        period_name : str
            Period name
        bbox : tuple
            Bounding box (min_lon, min_lat, max_lon, max_lat)
        
        Returns:
        --------
        tuple[Optional[Path], bool]
            (Path to downloaded file, whether it was newly downloaded)
        """
        output_file = self.get_file_path(period_name)
        
        # Check if file already exists
        if output_file.exists():
            print(f"  File already exists, skipping: {output_file.name}")
            return output_file, False
        
        print(f"  Downloading data for {period_name}...")
        
        # Format dates for API
        date_from = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_to = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Call OpenAQFetcher.py
        cmd = [
            "python3", "src/data_processing/air_handler/OpenAQFetcher.py",
            "--api-key", self.api_key,
            "--bbox", str(bbox[0]), str(bbox[1]), str(bbox[2]), str(bbox[3]),
            "--date-from", date_from,
            "--date-to", date_to
        ]

        temp_file = Path(f"./data/temp_data/openaq_data_{date_from[:10].replace('-', '')}_{date_to[:10].replace('-', '')}.parquet")

        for attempt in range(1, max_retries + 1):
            result = subprocess.run(cmd, capture_output=True, text=True)

            if temp_file.exists():
                temp_file.rename(output_file)
                print(f"  Successfully downloaded: {output_file.name}")
                return output_file, True

            stderr = (result.stderr or "").strip()
            combined_output = "\n".join(part for part in [result.stdout.strip(), stderr] if part)

            if "429" in combined_output:
                wait_seconds = 30 * attempt
                print(f"  OpenAQ rate limit hit for {period_name} (attempt {attempt}/{max_retries})")
                print(f"  Waiting {wait_seconds} seconds before retry...")
                time.sleep(wait_seconds)
                continue

            print(f"  Download failed for {period_name}")
            if stderr:
                print(f"  Error: {stderr}")
            return None, False

        print(f"  Download failed for {period_name} after {max_retries} attempts")
        if result.stderr:
            print(f"  Error: {result.stderr}")
        return None, False
    
    def download(self, start_date_str: str, end_date_str: str, bbox: tuple) -> List[Path]:
        """
        Download air quality data
        
        Parameters:
        -----------
        start_date_str : str
            Start date in format "YYYY-MM-DDTHH:MM:SSZ"
        end_date_str : str
            End date in format "YYYY-MM-DDTHH:MM:SSZ"
        bbox : tuple
            Bounding box (min_lon, min_lat, max_lon, max_lat)
        
        Returns:
        --------
        List[Path]
            List of downloaded file paths
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%SZ")
        
        print("\n" + "="*60)
        print("Step 1: Download air quality data (2-month periods)")
        print("="*60)
        
        # Calculate month ranges
        month_ranges = self.calculate_month_ranges(start_date, end_date)
        print(f"Date range split into {len(month_ranges)} 2-month period(s)")
        
        downloaded_files = []
        success_count = 0
        fail_count = 0
        
        for i, month_range in enumerate(month_ranges):
            file_path, was_downloaded = self.download_period(
                month_range['start'],
                month_range['end'],
                month_range['period_name'],
                bbox
            )
            
            if file_path:
                downloaded_files.append(file_path)
                success_count += 1
            else:
                fail_count += 1
            
            # Add delay between requests only if we actually downloaded a file
            if i < len(month_ranges) - 1 and was_downloaded:
                delay_time = 10  # 10 seconds delay
                print(f"  Waiting {delay_time} seconds before next request...")
                time.sleep(delay_time)
        
        print("\n" + "="*50)
        print("Download Summary:")
        print(f"  Successfully downloaded: {success_count} files")
        print(f"  Failed to download: {fail_count} files")
        print(f"  Total files: {len(month_ranges)}")
        print("="*50)
        
        return downloaded_files


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Air Quality Data Downloader")
    parser.add_argument("--api-key", type=str, required=True, help="OpenAQ API key")
    parser.add_argument("--output-dir", type=str, default="./data/air_data/original_data/", help="Output directory")
    parser.add_argument("--start-date", type=str, required=True, help="Start date, format: YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("--end-date", type=str, required=True, help="End date, format: YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("--bbox", type=float, nargs=4, required=True, help="Bounding box: min_lon min_lat max_lon max_lat")
    
    args = parser.parse_args()
    
    downloader = DataDownloader(args.api_key, args.output_dir)
    downloader.download(args.start_date, args.end_date, tuple(args.bbox))


if __name__ == "__main__":
    main()
