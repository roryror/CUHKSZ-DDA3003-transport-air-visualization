#!/usr/bin/env python3
"""
Taxi Data Merger
Merges multiple taxi data CSV files into single files per taxi type
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse

def merge_taxi_data(input_dir: str, output_dir: str, start_date: datetime, end_date: datetime, task_timestamp: str = None):
    """
    Merge taxi data files into single files per taxi type
    
    Parameters:
    -----------
    input_dir : str
        Input directory containing cleaned CSV files
    output_dir : str
        Base output directory
    start_date : datetime
        Start date of the time range
    end_date : datetime
        End date of the time range
    task_timestamp : str
        Task timestamp to use for folder name (optional)
    """
    input_base = Path(input_dir)
    output_base = Path(output_dir)
    
    # Create merged directory name
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    # Get operation time - use task timestamp if provided
    if task_timestamp:
        current_time = task_timestamp
    else:
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    merged_dir = output_base / f"taxi_data_{start_str}_{end_str}_{current_time}"
    merged_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n" + "="*60)
    print("Step 4: Merge taxi data")
    print("="*60)
    print(f"Creating merged directory: {merged_dir}")
    
    # Find all CSV files
    csv_files = list(input_base.glob("green_*.csv")) + list(input_base.glob("yellow_*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    # Group files by taxi type
    green_files = [f for f in csv_files if "green" in f.name.lower()]
    yellow_files = [f for f in csv_files if "yellow" in f.name.lower()]
    
    # Process green taxis
    if green_files:
        print(f"\nProcessing green taxi files: {len(green_files)} files")
        merge_and_save(green_files, merged_dir, "green")
    
    # Process yellow taxis
    if yellow_files:
        print(f"\nProcessing yellow taxi files: {len(yellow_files)} files")
        merge_and_save(yellow_files, merged_dir, "yellow")
    
    print(f"\n" + "="*60)
    print("Data merging completed!")
    print(f"Merged files saved to: {merged_dir}")
    print("="*60)

def merge_and_save(files: list, output_dir: Path, taxi_type: str):
    """
    Merge multiple CSV files and save to output directory
    
    Parameters:
    -----------
    files : list
        List of CSV files to merge
    output_dir : Path
        Output directory
    taxi_type : str
        Taxi type (green or yellow)
    """
    dfs = []
    
    for csv_file in files:
        print(f"  Reading: {csv_file.name}")
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            dfs.append(df)
        except Exception as e:
            print(f"  Error reading {csv_file.name}: {e}")
    
    if not dfs:
        print(f"  No valid data to merge for {taxi_type} taxis")
        return
    
    # Merge all DataFrames
    print(f"  Merging {len(dfs)} DataFrames...")
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Determine time field names
    if taxi_type == "green":
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
    else:
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
    
    # Sort by pickup time
    if pickup_col in merged_df.columns:
        print(f"  Sorting by pickup time...")
        merged_df[pickup_col] = pd.to_datetime(merged_df[pickup_col], format='%Y%m%d%H%M%S')
        merged_df = merged_df.sort_values(by=pickup_col)
        # Convert back to string format
        merged_df[pickup_col] = merged_df[pickup_col].dt.strftime('%Y%m%d%H%M%S')
    
    # Save merged file
    output_file = output_dir / f"{taxi_type}_tripdata_merged.csv"
    print(f"  Saving merged file: {output_file.name}")
    merged_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"  Successfully merged: {len(merged_df):,} rows")

def main():
    parser = argparse.ArgumentParser(description="Taxi Data Merger")
    parser.add_argument("--input-dir", type=str, default="./data/taxi_data/", help="Input directory with cleaned data")
    parser.add_argument("--output-dir", type=str, default="./data/taxi_data/", help="Output base directory")
    parser.add_argument("--start-date", type=str, required=True, help="Start date, format: YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, required=True, help="End date, format: YYYY-MM-DD")
    parser.add_argument("--task-timestamp", type=str, help="Task timestamp to use for folder name")
    
    args = parser.parse_args()
    
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    
    merge_taxi_data(args.input_dir, args.output_dir, start_date, end_date, args.task_timestamp)

if __name__ == "__main__":
    main()
