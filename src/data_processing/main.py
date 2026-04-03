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
from concurrent.futures import ThreadPoolExecutor

class DataPipeline:
    def __init__(self):
        pass
    
    def process_air_data(self, date_from: str, date_to: str, log_file: Path):
        """
        Process air quality data with streaming logs
        """
        # Call air quality data processing pipeline
        cmd = [
            "python3", "src/data_processing/air_handler/main.py",
            "--end-date", date_to[:10],
            "--days", str((datetime.strptime(date_to[:10], "%Y-%m-%d") - datetime.strptime(date_from[:10], "%Y-%m-%d")).days)
        ]
        
        return self._run_command_with_streaming_logs(cmd, log_file)
    
    def process_taxi_data(self, date_from: str, date_to: str, download: bool = False, log_file: Path = None):
        """
        Check and process taxi data with streaming logs
        """
        days = (datetime.strptime(date_to[:10], "%Y-%m-%d") - datetime.strptime(date_from[:10], "%Y-%m-%d")).days
        
        # Build taxi handler command
        taxi_cmd = ["python3", "src/data_processing/taxi_handler/main.py"]
        
        if download:
            taxi_cmd.extend([
                "--download",
                "--end-date", date_to[:10],
                "--days", str(days),
                "--taxi-types", "yellow", "green"
            ])
        
        return self._run_command_with_streaming_logs(taxi_cmd, log_file)
    
    def _run_command_with_streaming_logs(self, cmd: list, log_file: Path):
        """
        Run a command and stream output to log file in real-time
        
        Parameters:
        -----------
        cmd : list
            Command to execute
        log_file : Path
            Log file path
            
        Returns:
        --------
        bool
            True if command succeeded
        """
        import subprocess
        import os
        
        # Set environment to disable Python output buffering
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        
        # Open log file in append mode
        with open(log_file, 'w', encoding='utf-8') as log:
            # Write command to log
            log.write(f"Executing command: {' '.join(cmd)}\n")
            log.flush()
            
            # Start the process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=env
            )
            
            # Stream output line by line
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                if line:
                    # Write to log file
                    log.write(line)
                    log.flush()
            
            # Wait for process to finish
            process.wait()
            
            return process.returncode == 0
    
    def run(self, date_from: str, date_to: str, download_taxi: bool = False):
        """
        Run the complete pipeline
        """
        # Create log directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = Path(f"./logs/{timestamp}")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Starting data processing pipeline")
        print(f"Time range: {date_from} to {date_to}")
        print(f"Download taxi data: {download_taxi}")
        print(f"Logs will be saved to: {log_dir}")
        
        air_log_file = log_dir / "air_data.log"
        taxi_log_file = log_dir / "taxi_data.log"
        
        # Run both processes in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit tasks
            air_future = executor.submit(self.process_air_data, date_from, date_to, air_log_file)
            taxi_future = executor.submit(self.process_taxi_data, date_from, date_to, download_taxi, taxi_log_file)
            
            # Get results
            air_success = air_future.result()
            taxi_success = taxi_future.result()
        
        # Check results
        if not air_success:
            print(f"Pipeline failed: Air quality data processing failed. See logs at {air_log_file}")
            return False
        
        if not taxi_success:
            print(f"Pipeline failed: Taxi data processing failed. See logs at {taxi_log_file}")
            return False
        
        print("\n" + "="*70)
        print("Data processing pipeline completed!")
        print(f"Logs saved to: {log_dir}")
        print("="*70)
        return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Processing Pipeline")
    parser.add_argument("--end-date", type=str, help="End date, format: YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="Number of days to look back")
    parser.add_argument("--download-taxi", action="store_true", help="Download taxi data before processing")
    
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
    print(f"  Download taxi data: {args.download_taxi}")
    
    # Run pipeline
    pipeline = DataPipeline()
    pipeline.run(start_date, end_date, args.download_taxi)

if __name__ == "__main__":
    main()
