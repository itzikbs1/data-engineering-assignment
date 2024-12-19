from datetime import datetime
from typing import Dict
from ..models.location import Location
from ..models.coordinates import Coordinates
from ..models.measurement import Measurement
from ..models.sensor import Sensor


class DataTransformer:
    """Handles data transformation from API response to models"""

    @staticmethod
    def to_location(raw_data: Dict) -> Location:
        coordinates = Coordinates(
            latitude=raw_data['coordinates']['latitude'],
            longitude=raw_data['coordinates']['longitude']
        )

        return Location(
            id=raw_data['id'],
            name=raw_data['name'],
            country_code=raw_data['country']['code'],
            country_name=raw_data['country']['name'],
            city=raw_data.get('city'),
            coordinates=coordinates,
            timezone=raw_data.get('timezone'),
            owner_name=raw_data.get('owner', {}).get('name'),
            provider_name=raw_data.get('provider', {}).get('name'),
            is_mobile=raw_data.get('isMobile', False),
            is_monitor=raw_data.get('isMonitor', False),
            sensor_ids=raw_data.get('sensor_ids', []),
            last_update=raw_data.get('last_update')
        )

    @staticmethod
    def to_sensor(raw_data: Dict, location_id: int) -> Sensor:
        """Convert raw sensor data to Sensor model"""
        return Sensor(
            id=raw_data['id'],
            location_id=location_id,
            name=raw_data['name'],
            parameter_name=raw_data['parameter']['name'],
            parameter_display_name=raw_data['parameter']['displayName'],
            units=raw_data['parameter']['units']
        )
    @staticmethod
    def to_measurement(raw_data: Dict, location_id: int) -> Measurement:
        """Convert raw measurement data to Measurement model"""
        coordinates = None
        if 'coordinates' in raw_data:
            coordinates = Coordinates(
                latitude=raw_data['coordinates']['latitude'],
                longitude=raw_data['coordinates']['longitude']
            )

        return Measurement(
            location_id=location_id,
            sensor_id=raw_data['sensorsId'],
            value=raw_data['value'],
            datetime=datetime.strptime(
                raw_data['datetime']['utc'],
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            coordinates=coordinates
        )