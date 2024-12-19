import json
from datetime import datetime
from typing import List, Optional
from .connection import DatabaseConnection
from ..models.location import Location
from ..models.sensor import Sensor
from ..models.measurement import Measurement
from config.logging_config import setup_logging

logger = setup_logging(__name__)


class Repository:
    def __init__(self):
        self.db = DatabaseConnection()

    def initialize_tables(self) -> None:
        """Initialize database tables"""
        try:
            self.db.init_db()
            logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database tables: {str(e)}")
            raise

    def save_location(self, location: Location) -> None:
        """Save location to database"""
        query = """
        INSERT INTO locations (
            id, name, country_code, country_name, city, coordinates, 
            timezone, owner_name, provider_name, is_mobile, is_monitor,
            sensor_ids, last_update
        ) VALUES (
            %(id)s, %(name)s, %(country_code)s, %(country_name)s, 
            %(city)s, %(coordinates)s, %(timezone)s, %(owner_name)s, 
            %(provider_name)s, %(is_mobile)s, %(is_monitor)s,
            %(sensor_ids)s, %(last_update)s
        ) ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            country_code = EXCLUDED.country_code,
            country_name = EXCLUDED.country_name,
            city = EXCLUDED.city,
            coordinates = EXCLUDED.coordinates,
            timezone = EXCLUDED.timezone,
            owner_name = EXCLUDED.owner_name,
            provider_name = EXCLUDED.provider_name,
            is_mobile = EXCLUDED.is_mobile,
            is_monitor = EXCLUDED.is_monitor,
            sensor_ids = EXCLUDED.sensor_ids,
            last_update = EXCLUDED.last_update;
        """
        try:
            with self.db.get_cursor() as cursor:
                # Convert location data to dict and ensure sensor_ids exists
                location_data = location.model_dump()
                location_data['coordinates'] = json.dumps(location_data['coordinates'])

                cursor.execute(query, location_data)
                logger.info(f"Saved location: {location.name} with {len(location_data['sensor_ids'])} sensors")
                # location_data['sensor_ids'] = json.dumps(location_data.get('sensor_ids', []))
                #
                # cursor.execute(query, location_data)
                # logger.info(f"Saved location: {location.name} with {len(location_data['sensor_ids'])} sensors")

        except Exception as e:
            logger.error(f"Error saving location {location.name}: {str(e)}")
            raise

    def save_sensor(self, sensor: Sensor) -> None:
        """Save single sensor to database"""
        query = """
        INSERT INTO sensors (
            id, location_id, name, parameter_name, 
            parameter_display_name, units
        ) VALUES (
            %(id)s, %(location_id)s, %(name)s, %(parameter_name)s, 
            %(parameter_display_name)s, %(units)s
        ) ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            parameter_name = EXCLUDED.parameter_name,
            parameter_display_name = EXCLUDED.parameter_display_name,
            units = EXCLUDED.units;
        """
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(query, sensor.model_dump())
                logger.info(f"Saved sensor: {sensor.name} for location {sensor.location_id}")
        except Exception as e:
            logger.error(f"Error saving sensor {sensor.id}: {str(e)}")
            raise

    def save_sensors(self, sensors: List[Sensor]) -> None:
        """Save multiple sensors to database"""
        try:
            for sensor in sensors:
                self.save_sensor(sensor)
            logger.info(f"Saved {len(sensors)} sensors")
        except Exception as e:
            logger.error(f"Error saving sensors batch: {str(e)}")
            raise

    def save_measurements(self, measurements: List[Measurement]) -> None:
        """Save measurements to database"""
        query = """
        INSERT INTO measurements 
        (location_id, sensor_id, value, datetime, coordinates)
        VALUES (
            %(location_id)s, %(sensor_id)s, %(value)s, %(datetime)s, %(coordinates)s
        );
        """
        try:
            with self.db.get_cursor() as cursor:
                for measurement in measurements:
                    measurement_dict = measurement.model_dump()
                    measurement_dict['id'] = json.dumps(measurement_dict['id'])
                    measurement_dict['coordinates'] = json.dumps(measurement_dict['coordinates'])
                    measurement_dict['created_at'] = json.dumps(measurement_dict['created_at'])
                    cursor.execute(query, measurement_dict)
                logger.info(f"Saved {len(measurements)} measurements")
        except Exception as e:
            logger.error(f"Error saving measurements: {str(e)}")
            raise