import pandas as pd
import os
import glob
from pathlib import Path
import time

def convert_parquet_to_csv(input_dir="../data/", overwrite=False):
    """
    Convert all Parquet files in the specified directory to CSV files with the same name
    
    Parameters:
    -----------
    input_dir : str
        Input directory path containing Parquet files
    overwrite : bool
        Whether to overwrite existing CSV files, default False (skip already converted files)
    """
    # Ensure correct path is used
    data_dir = Path(input_dir)
    
    # Check if directory exists
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist!")
        return
    
    print(f"Scanning directory: {data_dir.absolute()}")
    
    # Find all Parquet files
    parquet_files = list(data_dir.glob("*.parquet")) + list(data_dir.glob("*.parq"))
    
    if not parquet_files:
        print("No Parquet files found (.parquet or .parq)")
        return
    
    print(f"Found {len(parquet_files)} Parquet files")
    
    converted_count = 0
    skipped_count = 0
    error_count = 0
    
    for parquet_path in parquet_files:
        # Build corresponding CSV filename
        csv_filename = parquet_path.stem + ".csv"
        csv_path = parquet_path.parent / csv_filename
        
        # Check if CSV file already exists and not overwriting
        if csv_path.exists() and not overwrite:
            print(f"Skipping: {parquet_path.name} → CSV file already exists")
            skipped_count += 1
            continue
        
        try:
            # Record start time
            start_time = time.time()
            
            # Read Parquet file
            print(f"Converting: {parquet_path.name}...", end="", flush=True)
            df = pd.read_parquet(parquet_path)
            
            # Save as CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            
            # Print statistics
            print(f"  Done! Rows: {len(df):,}, Columns: {len(df.columns)}, Time: {elapsed_time:.2f}s")
            print(f"        Saved to: {csv_path.name}")
            
            converted_count += 1
            
        except Exception as e:
            print(f"  Error!")
            print(f"        Conversion failed: {e}")
            error_count += 1
    
    # Print summary
    print("\n" + "="*50)
    print("Conversion Summary:")
    print(f"  Successfully converted: {converted_count} files")
    print(f"  Skipped existing: {skipped_count} files")
    print(f"  Failed to convert: {error_count} files")
    print(f"  Total files processed: {len(parquet_files)} files")


def convert_with_progress(input_dir="../data/"):
    """
    Version with progress display
    """
    data_dir = Path(input_dir)
    
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist!")
        return
    
    # Find files
    parquet_files = list(data_dir.glob("*.parquet")) + list(data_dir.glob("*.parq"))
    
    if not parquet_files:
        print("No Parquet files found")
        return
    
    total_files = len(parquet_files)
    
    print(f"Starting batch conversion of {total_files} Parquet files")
    print("="*60)
    
    for i, parquet_path in enumerate(parquet_files, 1):
        csv_path = parquet_path.parent / (parquet_path.stem + ".csv")
        
        # Progress display
        print(f"[{i}/{total_files}] Processing: {parquet_path.name}")
        
        if csv_path.exists():
            print(f"    Skipping - CSV file already exists: {csv_path.name}")
            continue
        
        try:
            # Read and convert
            df = pd.read_parquet(parquet_path)
            
            # Optional: Show data preview
            if i == 1:  # Only show detailed info for the first file
                print(f"    Data preview: {len(df)} rows × {len(df.columns)} columns")
                print(f"    Columns: {list(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            
            # Save as CSV
            df.to_csv(csv_path, index=False)
            print(f"    ✓ Saved: {csv_path.name}")
            
        except Exception as e:
            print(f"    ✗ Conversion failed: {e}")
    
    print("="*60)
    print("Batch conversion completed!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert Parquet files to CSV")
    parser.add_argument("--input", type=str, help="Input Parquet file path")
    parser.add_argument("--input-dir", type=str, default="../data/", help="Input directory")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV files")
    
    args = parser.parse_args()
    
    if args.input:
        # Process single file
        input_path = Path(args.input)
        output_csv = input_path.with_suffix('.csv')
        
        print(f"Converting: {args.input}...")
        try:
            df = pd.read_parquet(args.input)
            df.to_csv(output_csv, index=False, encoding='utf-8')
            print(f"Conversion successful: {output_csv}")
        except Exception as e:
            print(f"Conversion failed: {e}")
    else:
        # Process directory
        convert_parquet_to_csv(args.input_dir, overwrite=args.overwrite)
