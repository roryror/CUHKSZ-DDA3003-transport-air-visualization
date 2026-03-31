#!/usr/bin/env python3
"""
Taxi Data Downloader
Downloads NYC TLC taxi trip data based on time range and taxi type
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import urllib.request


class TaxiDownloader:
    def __init__(self, download_links_path: str, output_dir: str):
        """
        Initialize TaxiDownloader
        
        Parameters:
        -----------
        download_links_path : str
            Path to download_link.json file
        output_dir : str
            Output directory for downloaded files
        """
        self.download_links_path = Path(download_links_path)
        self.output_dir = Path(output_dir)
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.download_links_path, 'r', encoding='utf-8') as f:
            self.download_links = json.load(f)
    
    def calculate_month_range(self, start_date: datetime, days: int) -> List[str]:
        """
        Calculate the range of months to download based on start date and days
        
        Parameters:
        -----------
        start_date : datetime
            Start date
        days : int
            Number of days to look back
        
        Returns:
        --------
        List[str]
            List of year-month strings in format "YYYY-MM"
        """
        months = []
        
        if days < 30:
            num_months = 2
        else:
            num_months = 6
        
        current_date = start_date.replace(day=1)
        
        # Find available months, going backwards if current month not available
        while len(months) < num_months:
            year_str = str(current_date.year)
            month_str = f"{current_date.month:02d}"
            
            if year_str in self.download_links and month_str in self.download_links[year_str]:
                year_month = current_date.strftime("%Y-%m")
                months.append(year_month)
            
            # Move to previous month for next iteration
            prev_month = current_date.month - 1
            prev_year = current_date.year
            if prev_month < 1:
                prev_month = 12
                prev_year -= 1
            current_date = current_date.replace(year=prev_year, month=prev_month)
            
            # Check if we've gone too far back
            if prev_year < 2016:
                break
        
        return months
    
    def get_download_urls(self, year_months: List[str], taxi_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Get download URLs for specified year-months and taxi types
        
        Parameters:
        -----------
        year_months : List[str]
            List of year-month strings in format "YYYY-MM"
        taxi_types : Optional[List[str]]
            List of taxi types to download (yellow, green), None for all
        
        Returns:
        --------
        List[Dict]
            List of download info dicts with 'year_month', 'type', 'url'
        """
        if taxi_types is None:
            taxi_types = ['yellow', 'green']
        
        download_list = []
        
        for year_month in year_months:
            year, month = year_month.split('-')
            
            if year not in self.download_links:
                continue
            
            if month not in self.download_links[year]:
                continue
            
            for item in self.download_links[year][month]:
                if item['type'] in taxi_types:
                    download_list.append({
                        'year_month': year_month,
                        'type': item['type'],
                        'url': item['url'].strip()
                    })
        
        return download_list
    
    def download_file(self, url: str, output_path: Path) -> bool:
        """
        Download a single file
        
        Parameters:
        -----------
        url : str
            URL to download
        output_path : Path
            Output file path
        
        Returns:
        --------
        bool
            True if download successful
        """
        if output_path.exists():
            print(f"File already exists, skipping: {output_path.name}")
            return True
        
        try:
            print(f"Downloading: {url}")
            urllib.request.urlretrieve(url, output_path)
            print(f"Downloaded successfully: {output_path.name}")
            return True
        except Exception as e:
            print(f"Download failed for {url}: {e}")
            return False
    
    def download(self, start_date_str: str, days: int, taxi_types: Optional[List[str]] = None) -> List[Path]:
        """
        Download taxi data
        
        Parameters:
        -----------
        start_date_str : str
            Start date in format "YYYY-MM-DD"
        days : int
            Number of days to look back
        taxi_types : Optional[List[str]]
            List of taxi types to download (yellow, green), None for all
        
        Returns:
        --------
        List[Path]
            List of downloaded file paths
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = start_date + timedelta(days=days)
        
        year_months = self.calculate_month_range(end_date, days)
        print(f"Calculated month range: {year_months}")
        
        download_list = self.get_download_urls(year_months, taxi_types)
        
        if not download_list:
            print("No files to download")
            return []
        
        print(f"Found {len(download_list)} files to download")
        
        downloaded_files = []
        success_count = 0
        fail_count = 0
        
        for item in download_list:
            url = item['url']
            filename = url.split('/')[-1]
            output_path = self.output_dir / filename
            
            if self.download_file(url, output_path):
                downloaded_files.append(output_path)
                success_count += 1
            else:
                fail_count += 1
        
        print("\n" + "="*50)
        print("Download Summary:")
        print(f"  Successfully downloaded: {success_count} files")
        print(f"  Failed to download: {fail_count} files")
        print(f"  Total files: {len(download_list)}")
        
        return downloaded_files


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Taxi Data Downloader")
    parser.add_argument("--download-links", type=str, default="./data/taxi_data/original_data/download_link.json", help="Path to download_link.json")
    parser.add_argument("--output-dir", type=str, default="./data/taxi_data/original_data/", help="Output directory")
    parser.add_argument("--start-date", type=str, required=True, help="Start date, format: YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--taxi-types", type=str, nargs="+", default=["yellow", "green"], help="Taxi types to download (yellow, green)")
    
    args = parser.parse_args()
    
    downloader = TaxiDownloader(args.download_links, args.output_dir)
    downloader.download(args.start_date, args.days, args.taxi_types)


if __name__ == "__main__":
    main()
