"""
Fetch air quality data from OpenAQ API and convert to required format
Using OpenAQ Python SDK
"""
from openaq import OpenAQ
import pandas as pd
import json
from datetime import datetime
import time
import os

from Structure import LocationInfo
from Structure import SensorInfo

class OpenAQFetcher:
    def __init__(self, api_key: str, location_ids: list[str] = None, bbox: tuple[float, float, float, float] = None, date_range: tuple[str, str] = None):
        """
        Initialize OpenAQFetcher
        
        Parameters:
            api_key: OpenAQ API key
            location_ids: List of station IDs (optional, choose either this or bbox)
            bbox: Bounding box tuple (min_lon, min_lat, max_lon, max_lat) (optional, choose either this or location_ids)
            date_range: Date range tuple (date_from, date_to), format as string "YYYY-MM-DDTHH:MM:SSZ"
        """
        self.api_key = api_key
        self.client = OpenAQ(api_key=api_key)
        self.location_ids = location_ids or []
        self.bbox = bbox
        self.date_range = date_range
        self.map_location_infos: dict[str, LocationInfo] = {}
        self.raw_data: list[dict] = []
        self.aggregated_data = None  # Type is pd.DataFrame or None

    def Handle(self):
        ret = 0
        try:
            ret = self.batchGetLocationInfo()
            if ret != 0:
                print(f"Handle batchGetLocationInfo error: {ret}")
                return ret
            ret = self.batchGetSensorInfo()
            if ret != 0:
                print(f"Handle batchGetSensorInfo error: {ret}")
                return ret
            ret = self.batchGetMeasurementInfo()
            if ret != 0:
                print(f"Handle batchGetMeasurementInfo error: {ret}")
                return ret
            ret = self.processRawData()
            if ret != 0:
                print(f"Handle processRawData error: {ret}")
                return ret
            ret = self.aggregateHourlyData()
            if ret != 0:
                print(f"Handle aggregateHourlyData error: {ret}")
                return ret  
        finally:
            # Ensure client connection is closed
            self.client.close()
        return 0


    """
    Get the aggregated information of a specific location from OpenAQ API
    @return: 0 if successful, other value if failed
    """
    def batchGetLocationInfo(self, timeout: int = 10) -> int:
        """
        Get station information
        If bbox is provided, use it to get all stations within the range
        If location_ids is provided, get specified stations
        """
        if self.bbox:
            # Use bounding box to get all stations within range
            print(f"Using bounding box to get stations: {self.bbox}")
            try:
                page = 1
                limit = 1000
                has_more = True
                
                while has_more:
                    locations_response = self.client.locations.list(
                        bbox=self.bbox,
                        limit=limit,
                        page=page
                    )
                    
                    if locations_response.results:
                        for location_obj in locations_response.results:
                            location_id = str(location_obj.id)
                            location_info = {
                                'id': location_obj.id,
                                'name': location_obj.name,
                                'timezone': location_obj.timezone,
                                'coordinates': {
                                    'latitude': location_obj.coordinates.latitude,
                                    'longitude': location_obj.coordinates.longitude
                                } if location_obj.coordinates else {}
                            }
                            self.map_location_infos[location_id] = LocationInfo(location_id=location_id, location_info=location_info)
                        
                        print(f"  Page {page}: Got {len(locations_response.results)} stations, total {len(self.map_location_infos)}")
                        
                        # Check if there's more data
                        if len(locations_response.results) < limit:
                            has_more = False
                        else:
                            page += 1
                            time.sleep(0.3)  # Avoid too frequent requests
                    else:
                        has_more = False
                
                print(f"Total stations obtained: {len(self.map_location_infos)}")
            except Exception as e:
                print(f"batchGetLocationInfo (bbox) error: {e}")
                return -1
        elif self.location_ids:
            # Use location_ids to get specified stations
            for location_id in self.location_ids:
                location_info = None
                try:
                    location_response = self.client.locations.get(int(location_id))
                    if location_response.results:
                        # Convert SDK response to dictionary format (maintain compatibility)
                        location_obj = location_response.results[0]
                        location_info = {
                            'id': location_obj.id,
                            'name': location_obj.name,
                            'timezone': location_obj.timezone,
                            'coordinates': {
                                'latitude': location_obj.coordinates.latitude,
                                'longitude': location_obj.coordinates.longitude
                            } if location_obj.coordinates else {}
                        }
                    else:
                        print(f"getLocationInfo: Location {location_id} not found")
                        return -1
                except Exception as e:
                    print(f"getLocationInfo error: {e}")
                    return -1
                self.map_location_infos[location_id] = LocationInfo(location_id=location_id, location_info=location_info)
        else:
            print("Error: Must provide either location_ids or bbox")
            return -1
        return 0

    """
    Get the sensor information of a specific location from OpenAQ API
    @return: 0 if successful, other value if failed
    """
    def batchGetSensorInfo(self, timeout: int = 10) -> int:
        for location_id, location_info in self.map_location_infos.items():
            try:
                sensors_response = self.client.locations.get(int(location_id))
                if sensors_response.results:
                    location_obj = sensors_response.results[0]
                    sensor_list: list[SensorInfo] = []
                    for sensor_obj in location_obj.sensors:
                        # Convert SDK response to dictionary format (maintain compatibility)
                        sensor_param = {
                            'id': sensor_obj.parameter.id,
                            'name': sensor_obj.parameter.name,
                            'units': sensor_obj.parameter.units,
                            'display_name': sensor_obj.parameter.display_name
                        }
                        sensor_list.append(SensorInfo(sensor_id=str(sensor_obj.id), sensor_param=sensor_param))
                    location_info.sensor_list = sensor_list
                else:
                    print(f"batchGetSensorInfo: Location {location_id} not found")
                    return -1
            except Exception as e:
                print(f"batchGetSensorInfo error: {e}")
                return -1
        return 0

    """
    Get the measurement information of a specific sensor from OpenAQ API
    @return: 0 if successful, other value if failed
    """
    def batchGetMeasurementInfo(self, timeout: int = 10) -> int:
        """
        Get measurement data. Use OpenAQ SDK's pagination mechanism to get all data
        SDK supports datetime_from and datetime_to parameters for date filtering
        """
        for location_id, location_item in self.map_location_infos.items():
            for sensor in location_item.sensor_list:
                all_measurements = []
                page = 1
                limit = 1000  # API limit is 1000 per request
                has_more = True
                
                print(f"    Getting data for sensor {sensor.sensor_id} (paginated. Date range: {self.date_range[0]} to {self.date_range[1]})...")
                
                while has_more:
                    try:
                        # Use SDK's measurements.list() method, supports date filtering and pagination
                        measurements_response = self.client.measurements.list(
                            sensors_id=int(sensor.sensor_id),
                            datetime_from=self.date_range[0],
                            datetime_to=self.date_range[1],
                            limit=limit,
                            page=page
                        )
                        
                        results = measurements_response.results
                        
                        if results:
                            # Convert SDK response objects to dictionary format (maintain compatibility)
                            for measurement_obj in results:
                                measurement_dict = {
                                    'period': {
                                        'datetimeFrom': {
                                            'utc': measurement_obj.period.datetime_from.utc,
                                            'local': measurement_obj.period.datetime_from.local
                                        },
                                        'datetimeTo': {
                                            'utc': measurement_obj.period.datetime_to.utc if measurement_obj.period.datetime_to else None,
                                            'local': measurement_obj.period.datetime_to.local if measurement_obj.period.datetime_to else None
                                        }
                                    },
                                    'parameter': {
                                        'id': measurement_obj.parameter.id,
                                        'name': measurement_obj.parameter.name,
                                        'units': measurement_obj.parameter.units,
                                        'display_name': measurement_obj.parameter.display_name
                                    },
                                    'value': measurement_obj.value,
                                    'coordinates': {
                                        'latitude': measurement_obj.coordinates.latitude if measurement_obj.coordinates else None,
                                        'longitude': measurement_obj.coordinates.longitude if measurement_obj.coordinates else None
                                    }
                                }
                                all_measurements.append(measurement_dict)
                            
                            print(f"        Page {page}: Got {len(results)} records, total {len(all_measurements)}")
                            
                            # Check if there's more data
                            # If current page has less than limit, it's the last page
                            if len(results) < limit:
                                has_more = False
                                print(f"        All data obtained (current page has less than limit)")
                            else:
                                # Continue to next page
                                page += 1
                                time.sleep(0.3)  # Avoid too frequent requests
                        else:
                            has_more = False
                            print(f"        No data on page {page}, stopping pagination")
                            
                    except Exception as e:
                        print(f"        Error: {e}")
                        has_more = False
                
                # Save all obtained data
                sensor.measurement_list = all_measurements
                if all_measurements:
                    # Display data time range
                    first_time = all_measurements[0].get('period', {}).get('datetimeFrom', {}).get('utc', 'N/A')
                    last_time = all_measurements[-1].get('period', {}).get('datetimeFrom', {}).get('utc', 'N/A')
                    print(f"        ✓ Total {len(all_measurements)} records obtained")
                    print(f"          Data time range: {first_time} to {last_time}")
                    print(f"          Target date range: {self.date_range[0]} to {self.date_range[1]}")
                else:
                    print(f"        ✗ No data obtained (may be no data for this time period)")
                    
        return 0

    """
    Process the records of a specific sensor from OpenAQ API
    @return: 0 if successful, other value if failed
    """
    def processRawData(self, timeout: int = 10) -> int:
        # Convert date range strings to datetime objects for comparison
        try:
            date_from_dt = pd.to_datetime(self.date_range[0])
            date_to_dt = pd.to_datetime(self.date_range[1])
            print(f"\n  Starting to process raw data, filtering date range: {self.date_range[0]} to {self.date_range[1]}")
        except Exception as e:
            print(f"Warning: Unable to parse date range: {e}, will not perform date filtering")
            date_from_dt = None
            date_to_dt = None
        
        filtered_count = 0
        total_count = 0
        
        for location_id, location_item in self.map_location_infos.items():
            for sensor in location_item.sensor_list:
                if not sensor.measurement_list:
                    continue
                for measurement in sensor.measurement_list:
                    total_count += 1
                    
                    # Parse period field to get time
                    period = measurement.get('period', {})
                    datetime_from = period.get('datetimeFrom', {})
                    datetime_to = period.get('datetimeTo', {})
                    
                    # Get UTC time string
                    datetime_utc_str = datetime_from.get('utc', '')
                    
                    # If date range is specified, filter data
                    if date_from_dt is not None and date_to_dt is not None and datetime_utc_str:
                        try:
                            measurement_dt = pd.to_datetime(datetime_utc_str)
                            # Check if within specified date range (inclusive)
                            if measurement_dt < date_from_dt or measurement_dt > date_to_dt:
                                filtered_count += 1
                                continue  # Skip data outside range
                        except Exception as e:
                            # If time parsing fails, keep data (may be format issue)
                            pass
                    # Parse period field to get time
                    period = measurement.get('period', {})
                    datetime_from = period.get('datetimeFrom', {})
                    datetime_to = period.get('datetimeTo', {})
                    
                    # Parse parameter field
                    param = measurement.get('parameter', {})
                    
                    record = {
                        'location_id': location_id,
                        'location_name': location_item.location_info.get('name', '') if location_item.location_info else '',
                        'parameter': param.get('name', ''),
                        'value': measurement.get('value'),
                        'unit': param.get('units', ''),
                        'datetimeUtc': datetime_from.get('utc', ''),
                        'datetimeLocal': datetime_from.get('local', ''),
                        'timezone': location_item.location_info.get('timezone', '') if location_item.location_info else '',
                        'latitude': location_item.location_info.get('coordinates', {}).get('latitude') if location_item.location_info.get('coordinates') else None,
                        'longitude': location_item.location_info.get('coordinates', {}).get('longitude') if location_item.location_info.get('coordinates') else None,
                        'country_iso': location_item.location_info.get('country', {}).get('code', '') if location_item.location_info.get('country') else '',
                        'isMobile': location_item.location_info.get('isMobile', False) if location_item.location_info else False,
                        'isMonitor': location_item.location_info.get('isMonitor', False) if location_item.location_info else False,
                        'owner_name': location_item.location_info.get('owner', {}).get('name', '') if location_item.location_info.get('owner') else '',
                        'provider': location_item.location_info.get('provider', {}).get('name', '') if location_item.location_info.get('provider') else ''
                    }
                    self.raw_data.append(record)
        
        # Display filtering results
        if filtered_count > 0:
            print(f"\n  Date filtering results:")
            print(f"    Raw data: {total_count:,} records")
            print(f"    Filtered out: {filtered_count:,} records (outside {self.date_range[0]} to {self.date_range[1]} range)")
            print(f"    Kept data: {len(self.raw_data):,} records (within date range)")
        elif total_count > 0:
            print(f"\n  All {total_count:,} records are within specified date range")
        else:
            print(f"\n  Warning: No data obtained")
        
        return 0

    """
    Aggregate the raw data by hour
    @return: 0 if successful, other value if failed
    """
    def aggregateHourlyData(self):
        """Aggregate data by hour"""
        # Convert to DataFrame
        df = pd.DataFrame(self.raw_data)
        if df is None or df.empty:
            return -1

        # Use datetimeUtc as time base
        if 'datetimeUtc' in df.columns and df['datetimeUtc'].notna().any():
            df = df.copy()
            # Convert datetimeUtc to datetime type first
            df['datetimeUtc'] = pd.to_datetime(df['datetimeUtc'], errors='coerce')
            df['datetime_hour'] = df['datetimeUtc'].dt.floor('h')  # Use 'h' instead of 'H'
            # Fill null values to avoid groupby issues
            group_cols = [
                'datetime_hour',
                'location_id',
                'parameter',
                'unit'
            ]
            
            # Use only existing non-null columns
            existing_cols = []
            for col in group_cols:
                if col in df.columns:
                    if col == 'datetime_hour' or df[col].notna().any():
                        existing_cols.append(col)
            if len(existing_cols) < 2:
                print("  Warning: Insufficient grouping columns, skipping aggregation")
                return -1
            
            # Fill potentially empty columns
            for col in ['location_name', 'timezone', 'country_iso', 'owner_name', 'provider']:
                if col in df.columns:
                    df[col] = df[col].fillna('')
            for col in ['latitude', 'longitude']:
                if col in df.columns:
                    df[col] = df[col].fillna(0).infer_objects(copy=False)
            for col in ['isMobile', 'isMonitor']:
                if col in df.columns:
                    df[col] = df[col].fillna(False)
            
            # Add other columns to grouping (if exist)
            optional_cols = ['location_name', 'timezone', 'latitude', 'longitude', 
                            'country_iso', 'isMobile', 'isMonitor', 'owner_name', 'provider']
            for col in optional_cols:
                if col in df.columns and col not in existing_cols:
                    existing_cols.append(col)
            try:
                hourly_agg = df.groupby(existing_cols, dropna=False).agg({
                    'value': ['mean', 'min', 'max', 'count']
                }).reset_index()
                # Flatten column names
                hourly_agg.columns = existing_cols + ['value_mean', 'value_min', 'value_max', 'value_count']
                print(f"  Aggregated: {len(hourly_agg)} records")
                self.aggregated_data = hourly_agg
                return 0
            except Exception as e:
                print(f"  Aggregation error: {e}")
                return -1
        
        return 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch air quality data from OpenAQ API")
    parser.add_argument("--api-key", type=str, default="1b98793765584b56e2138f8ffc9b858f8fac77df971256fb75e7d3b734efdec8", help="OpenAQ API key")
    parser.add_argument("--bbox", type=float, nargs=4, help="Bounding box (min_lon, min_lat, max_lon, max_lat)")
    parser.add_argument("--location-ids", type=str, nargs='+', help="List of station IDs")
    parser.add_argument("--date-from", type=str, default="2025-06-01T00:00:00Z", help="Start date, format: YYYY-MM-DDTHH:MM:SSZ")
    parser.add_argument("--date-to", type=str, default="2025-11-30T23:59:59Z", help="End date, format: YYYY-MM-DDTHH:MM:SSZ")
    
    args = parser.parse_args()
    
    # Default to NYC bounding box
    if not args.bbox and not args.location_ids:
        # NYC bounding box (min_lon, min_lat, max_lon, max_lat)
        args.bbox = (-74.018707, 40.641819, -73.8244, 40.868)
    
    # Use bounding box or location_ids to get stations
    fetcher = OpenAQFetcher(
        api_key=args.api_key,
        bbox=tuple(args.bbox) if args.bbox else None,
        location_ids=args.location_ids,
        date_range=(args.date_from, args.date_to)
    )
    
    ret = fetcher.Handle()
    if ret != 0:
        print(f"\nProcessing failed, error code: {ret}")
        exit(1)
    
    if fetcher.aggregated_data is not None and (isinstance(fetcher.aggregated_data, pd.DataFrame) and not fetcher.aggregated_data.empty):
        date_str = args.date_from[:10].replace("-", "") + "_" + args.date_to[:10].replace("-", "")
        # Use relative path
        filename = f"data/temp_data/openaq_data_{date_str}.parquet"
        fetcher.aggregated_data.to_parquet(filename, index=False, engine='pyarrow')
        print(f"Aggregated data saved to {filename}")
    else:
        print("Data fetching failed")
