from typing import Any

class SensorInfo:
    def __init__(self, sensor_id: str = "", sensor_param: Any = None, measurement_list: list[Any] = None):
        self.sensor_id = sensor_id
        self.sensor_param = sensor_param
        self.measurement_list = measurement_list if measurement_list is not None else []

class LocationInfo:
    def __init__(self, location_id: str = "", location_info: Any = None, sensor_list: list[SensorInfo] = None):
        self.location_id = location_id
        self.location_info = location_info
        self.sensor_list = sensor_list if sensor_list is not None else []