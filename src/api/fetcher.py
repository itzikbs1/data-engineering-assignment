from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
from .client import OpenAQClient
from .transformer import DataTransformer
from ..models.location import Location
from ..models.sensor import Sensor
from ..models.measurement import Measurement
from config.logging_config import setup_logging

logger = setup_logging(__name__)


class OpenAQFetcher:
    def __init__(self):
        self.client = OpenAQClient()
        self.transformer = DataTransformer()
        self.recent_threshold = datetime.utcnow() - timedelta(hours=24)
        self.max_retries = 3
        self.base_delay = 5

    def fetch_locations_page(self, page: int, limit: int = 100) -> Optional[Dict]:
        """Fetch locations data from OpenAQ API with pagination"""
        for attempt in range(self.max_retries):
            try:
                response = self.client.get_locations(limit, page)

                if response and 'results' in response:
                    return response
                elif response and response.get('status') == 429:  # Rate limit
                    wait_time = self.base_delay * (attempt + 1)
                    logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error("Failed to fetch locations")
                    return None

            except Exception as e:
                logger.error(f"Error fetching locations page {page}: {str(e)}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.base_delay)

        return None

    def fetch_all_locations(self) -> List[Dict]:
        """Fetch all locations with pagination"""
        all_locations = []
        page = 1
        limit = 100
        delay_between_requests = 1

        while True:
            logger.info(f"Fetching locations page {page}")
            data = self.fetch_locations_page(page, limit)

            if not data or 'results' not in data:
                break

            locations_of_page = data['results']
            if not locations_of_page:
                break

            all_locations.extend(locations_of_page)
            logger.info(f"Fetched {len(locations_of_page)} locations from page {page}")

            if len(locations_of_page) < limit:  # Last page
                break

            page += 1
            time.sleep(delay_between_requests)
            #TODO: delete later
            # if page == 3:
            #     break

        logger.info(f"Total locations fetched: {len(all_locations)}")
        return all_locations

    def is_location_active(self, location: Dict) -> bool:
        """Check if location has recent data (within last 48 hours)"""
        if not location.get('datetimeLast'):
            return False

        last_update = datetime.strptime(
            location['datetimeLast']['utc'],
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return datetime.utcnow() - last_update <= timedelta(hours=48)

    def is_measurement_recent(self, measurement: Dict) -> bool:
        """Check if a measurement is recent (within last 24 hours)"""
        measurement_time = datetime.strptime(
            measurement['datetime']['utc'],
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return measurement_time >= self.recent_threshold

    def fetch_sensors(self, location_id: int) -> List[Sensor]:
        """Fetch sensors for a location"""
        try:
            response = self.client.get_location_sensors(location_id)
            if not response or 'results' not in response:
                return []

            return [
                self.transformer.to_sensor(sensor, location_id)
                for sensor in response['results']
            ]
        except Exception as e:
            logger.error(f"Error fetching sensors for location {location_id}: {str(e)}")
            return []

    def fetch_measurements(self, location_id: int) -> List[Measurement]:
        """Fetch and filter recent measurements for a location"""
        try:
            response = self.client.get_measurements(location_id)
            if not response or 'results' not in response:
                return []

            # Filter recent measurements
            recent_measurements = [
                measurement for measurement in response['results']
                if self.is_measurement_recent(measurement)
            ]

            return [
                self.transformer.to_measurement(measurement, location_id)
                for measurement in recent_measurements
            ]
        except Exception as e:
            logger.error(f"Error fetching measurements for location {location_id}: {str(e)}")
            return []

    def get_active_locations(self, max_locations: Optional[int] = None) -> List[Location]:
        """Get active locations with recent measurements following specific order"""

        # 1. Fetch all locations first
        all_locations = self.fetch_all_locations()
        logger.info(f"1. Fetched total {len(all_locations)} locations")

        # 2. Filter active locations (last 48 hours)
        active_locations = [
            loc for loc in all_locations
            if self.is_location_active(loc)
        ]
        logger.info(f"2. Found {len(active_locations)} active locations")

        # 3. Apply max_locations limit on active locations if specified
        if max_locations:
            active_locations = active_locations[:max_locations]
            logger.info(f"3. Limited to {len(active_locations)} locations")

        # 4. For each active location, get measurements and check recency
        locations_with_recent_data = []

        for location in active_locations:
            # Get latest measurements
            measurements_response = self.client.get_measurements(location['id'])

            if measurements_response and 'results' in measurements_response:
                # Filter recent measurements (last 24 hours)
                recent_measurements = []
                location['sensor_ids'] = []
                for m in measurements_response['results']:
                    if self.is_measurement_recent(m):
                        recent_measurements.append(m)
                        location['sensor_ids'].append(m["sensorsId"])

                # Only include location if it has recent measurements
                if recent_measurements:
                    # Get sensors for this location
                    sensors = self.fetch_sensors(location['id'])

                    # Add sensor_ids and last_update to location data
                    # location['sensor_ids'] = [sensor.id for sensor in sensors]

                    location['last_update'] = datetime.strptime(
                        location['datetimeLast']['utc'],
                        "%Y-%m-%dT%H:%M:%SZ"
                    )

                    # Transform location using DataTransformer
                    location_obj = self.transformer.to_location(location)
                    locations_with_recent_data.append(location_obj)
                    logger.info(f"Location {location['name']} has {len(recent_measurements)} recent measurements")

        logger.info(f"4. Found {len(locations_with_recent_data)} locations with recent measurements")

        return locations_with_recent_data