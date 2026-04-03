#!/usr/bin/env python3
"""
Air Quality Data Merger
Merges multiple monthly air quality Parquet files into one
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse


def merge_air_data(input_dir: str, output_dir: str, start_date: str, end_date: str) -> str:
    """
    Merge monthly air quality data files
    
    Parameters:
    -----------
    input_dir : str
        Input directory containing monthly Parquet files
    output_dir : str
        Output directory for merged file
    start_date : str
        Start date in format "YYYY-MM-DDTHH:MM:SSZ"
    end_date : str
        End date in format "YYYY-MM-DDTHH:MM:SSZ"
    
    Returns:
    --------
    str
        Path to merged file
    """
    input_base = Path(input_dir)
    output_base = Path(output_dir)
    
    output_base.mkdir(parents=True, exist_ok=True)
    
    start_obj = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    end_obj = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
    
    start_str = start_obj.strftime("%Y%m%d")
    end_str = end_obj.strftime("%Y%m%d")
    
    output_file = output_base / f"openaq_data_{start_str}_{end_str}.parquet"
    
    print("\n" + "="*60)
    print("Step 1.5: Merge monthly air quality data")
    print("="*60)
    
    # Find all Parquet files in input directory
    parquet_files = list(input_base.glob("openaq_data_*.parquet"))
    
    if not parquet_files:
        print(f"No Parquet files found in {input_dir}")
        return None
    
    print(f"Found {len(parquet_files)} files")
    
    # Merge all files
    all_dfs = []
    for parquet_file in parquet_files:
        print(f"  Reading: {parquet_file.name}")
        df = pd.read_parquet(parquet_file)
        all_dfs.append(df)
    
    print(f"  Merging {len(all_dfs)} files...")
    merged_df = pd.concat(all_dfs, ignore_index=True)
    
    # Sort by datetime if available
    datetime_cols = ['datetime', 'date', 'time']
    for col in datetime_cols:
        if col in merged_df.columns:
            print(f"  Sorting by {col}...")
            merged_df = merged_df.sort_values(by=col)
            break
    
    print(f"  Saving merged file: {output_file.name}")
    merged_df.to_parquet(output_file, index=False)
    
    print(f"  Merge complete: {len(merged_df):,} rows")
    
    return str(output_file)


def main():
    parser = argparse.ArgumentParser(description="Air Quality Data Merger")
    parser.add_argument("--input-dir", type=str, default="./data/air_data/original_data/", help="Input directory with monthly files")
    parser.add_argument("--output-dir", type=str, default="./data/temp_data/", help="Output directory")
    parser.add_argument("--start-date", type=str, required=True, help="Start date, format: YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("--end-date", type=str, required=True, help="End date, format: YYYY-MM-DDTHH:MM:SSZ")
    
    args = parser.parse_args()
    
    merge_air_data(args.input_dir, args.output_dir, args.start_date, args.end_date)


if __name__ == "__main__":
    main()
