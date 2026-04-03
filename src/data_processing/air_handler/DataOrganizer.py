#!/usr/bin/env python3
"""
Air Quality Data Organization Tool
Organizes OpenAQ data by time range, parameter type, and station ID
"""
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import argparse
import re

def organize_data(input_file: str, output_base_dir: str = "./data/air_data", task_timestamp: str = None):
    """
    Organize data into structured directories
    
    Parameters:
    -----------
    input_file : str
        Input CSV file path
    output_base_dir : str
        Output base directory
    task_timestamp : str
        Task timestamp to use for folder name (optional)
    """
    # Ensure output directory exists
    output_base = Path(output_base_dir)
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Read data
    print(f"Reading file: {input_file}")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Failed to read file: {e}")
        return
    
    print(f"Data basic info: {len(df)} rows, {len(df.columns)} columns")
    
    # Extract time range
    if 'datetime_hour' in df.columns:
        # Convert to datetime type
        df['datetime_hour'] = pd.to_datetime(df['datetime_hour'])
        
        # Get min and max time
        min_time = df['datetime_hour'].min()
        max_time = df['datetime_hour'].max()
        
        # Format time strings
        min_time_str = min_time.strftime('%Y%m%d')
        max_time_str = max_time.strftime('%Y%m%d')
        
        # Get operation time - use task timestamp if provided
        if task_timestamp:
            operation_time = task_timestamp
        else:
            operation_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create main folder name
        folder_name = f"openaq_data_{min_time_str}_{max_time_str}_{operation_time}"
        main_folder = output_base / folder_name
        main_folder.mkdir(parents=True, exist_ok=True)
        
        print(f"Created main folder: {main_folder.name}")
        print(f"Time range: {min_time_str} to {max_time_str}")
    else:
        print("Error: 'datetime_hour' column is missing in the data")
        return
    
    # Extract location_id to other attributes mapping
    location_cols = ['location_id', 'location_name', 'timezone', 'latitude', 'longitude', 'country_iso', 'isMobile', 'isMonitor']
    location_map_df = df[location_cols].drop_duplicates()
    
    # Generate mapping file
    mapping_file = main_folder / "location_mapping.csv"
    location_map_df.to_csv(mapping_file, index=False, encoding='utf-8')
    print(f"\nGenerated mapping file: {mapping_file.name}")
    print(f"  Contains mapping information for {len(location_map_df)} stations")
    
    # Group by parameter
    parameters = df['parameter'].unique()
    print(f"Found {len(parameters)} parameters: {list(parameters)}")
    
    # Columns to keep
    keep_cols = ['datetime_hour', 'location_id', 'parameter', 'unit', 'value_mean', 'value_min', 'value_max', 'value_count']
    
    for param in parameters:
        # Create parameter folder
        param_folder = main_folder / param
        param_folder.mkdir(parents=True, exist_ok=True)
        print(f"  Created parameter folder: {param}")
        
        # Group by location_id
        param_df = df[df['parameter'] == param]
        location_ids = param_df['location_id'].unique()
        
        for location_id in location_ids:
            # Filter data
            location_df = param_df[param_df['location_id'] == location_id]
            
            # Keep only needed columns
            location_df = location_df[keep_cols]
            
            # Convert time format to unified format: 20260101002758
            location_df['datetime_hour'] = location_df['datetime_hour'].dt.strftime('%Y%m%d%H%M%S')
            
            # Create filename (only contains location_id)
            csv_filename = f"{location_id}.csv"
            csv_path = param_folder / csv_filename
            
            # Save data
            location_df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"    Saved station {location_id}: {csv_filename} ({len(location_df)} data records)")
    
    print(f"\nData organization completed!")
    print(f"Output directory: {main_folder.absolute()}")
    return main_folder

def batch_organize(input_dir: str, output_base_dir: str = "./data/air_data"):
    """
    Batch organize all CSV files in a directory
    """
    input_base = Path(input_dir)
    
    # Find all CSV files
    csv_files = list(input_base.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    for csv_file in csv_files:
        print(f"\nProcessing file: {csv_file.name}")
        organize_data(str(csv_file), output_base_dir)

def main():
    parser = argparse.ArgumentParser(description="Data Organization Tool")
    parser.add_argument("--input", type=str, help="Input CSV file path")
    parser.add_argument("--input-dir", type=str, help="Input directory (batch processing)")
    parser.add_argument("--output", type=str, default="./data/air_data/", help="Output base directory")
    parser.add_argument("--task-timestamp", type=str, help="Task timestamp to use for folder name")
    
    args = parser.parse_args()
    
    if args.input:
        organize_data(args.input, args.output, args.task_timestamp)
    elif args.input_dir:
        batch_organize(args.input_dir, args.output)
    else:
        print("Error: Must specify either --input or --input-dir parameter")
        parser.print_help()

if __name__ == "__main__":
    main()
