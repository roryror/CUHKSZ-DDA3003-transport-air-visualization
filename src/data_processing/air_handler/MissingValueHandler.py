#!/usr/bin/env python3
"""
Missing Value Handling Tool
Scans all CSV files under data/air_data directory and fills missing values in value columns with file-wide averages
"""
import pandas as pd
import os
from pathlib import Path
import argparse
import glob

def handle_missing_values(input_dir: str):
    """
    Handle missing values in all CSV files under the directory
    
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
    
    # Find all CSV files
    csv_files = list(input_base.rglob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    processed_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        print(f"\nProcessing file: {csv_file.relative_to(input_base)}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Check if contains value-related columns
            value_columns = [col for col in df.columns if 'value' in col.lower()]
            
            if not value_columns:
                print(f"  Warning: No value-related columns in file")
                continue
            
            # For each value-related column, fill missing values and 0 values (including 0.0) with file-wide average
            for col in value_columns:
                # Calculate average excluding 0 (including 0.0) and NaN values
                non_zero_values = df[col][(df[col] != 0) & (df[col].notna())]
                if len(non_zero_values) > 0:
                    mean_value = non_zero_values.mean()
                else:
                    mean_value = 0  # Fallback if all values are 0 or NaN
                
                # Count missing values and 0 values (including 0.0)
                missing_count = df[col].isnull().sum()
                zero_count = (df[col] == 0).sum()
                total_fill_count = missing_count + zero_count
                
                if total_fill_count > 0:
                    # Fill missing values and 0 values (including 0.0)
                    df[col] = df[col].apply(lambda x: mean_value if pd.isna(x) or x == 0 else x)
                    print(f"  Filled column {col}: {missing_count} missing values, {zero_count} zero values (including 0.0), average: {mean_value:.4f}")
                else:
                    print(f"  Column {col}: No missing or zero values")
            
            # Save modified data (overwrite original file)
            df.to_csv(csv_file, index=False, encoding='utf-8')
            print(f"  ✓ Saved successfully")
            processed_count += 1
            
        except Exception as e:
            print(f"  ✗ Processing failed: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n" + "="*50)
    print("Processing Summary:")
    print(f"  Successfully processed: {processed_count} files")
    print(f"  Failed to process: {error_count} files")
    print(f"  Total files processed: {len(csv_files)}")

def main():
    parser = argparse.ArgumentParser(description="Missing Value Handling Tool")
    parser.add_argument("--input-dir", type=str, default="./data/air_data/", help="Input directory")
    
    args = parser.parse_args()
    
    handle_missing_values(args.input_dir)

if __name__ == "__main__":
    main()
