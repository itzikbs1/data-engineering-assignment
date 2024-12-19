from typing import List, Optional
from datetime import datetime, timedelta
from ..api.fetcher import OpenAQFetcher
from ..database.repository import Repository
from config.logging_config import setup_logging
# import json

logger = setup_logging(__name__)


class ETLProcessor:
    def __init__(self):
        self.fetcher = OpenAQFetcher()
        self.repository = Repository()

    def process_location(self, location) -> bool:
        """Process a single location with its sensors and measurements"""
        try:
            # Save location
            self.repository.save_location(location)
            logger.info(f"Saved location {location.name} with {len(location.sensor_ids)} sensors")

            # Fetch and save sensors
            sensors = self.fetcher.fetch_sensors(location.id)
            if sensors:
                self.repository.save_sensors(sensors)
                logger.info(f"Saved {len(sensors)} sensors for location {location.name}")
            else:
                logger.warning(f"No sensors found for location {location.name}")

            # Fetch and save recent measurements
            measurements = self.fetcher.fetch_measurements(location.id)
            if measurements:
                self.repository.save_measurements(measurements)
                logger.info(f"Saved {len(measurements)} recent measurements for location {location.name}")
            else:
                logger.warning(f"No recent measurements found for location {location.name}")

            return True

        except Exception as e:
            logger.error(f"Error processing location {location.name}: {str(e)}")
            return False

    def process(self, max_locations: Optional[int] = None) -> None:
        """Main ETL process"""
        logger.info("Starting ETL process")
        start_time = datetime.now()

        try:
            # Get all active locations with recent measurements
            active_locations = self.fetcher.get_active_locations(max_locations)

            if not active_locations:
                logger.warning("No active locations found with recent measurements")
                return

            # Process each location
            total_locations = len(active_locations)
            successful = 0

            for index, location in enumerate(active_locations, 1):
                logger.info(f"Processing location {index}/{total_locations}: {location.name}")

                if self.process_location(location):
                    successful += 1

                # Log progress every 10 locations
                if index % 10 == 0:
                    logger.info(f"Progress: {index}/{total_locations} locations processed")

            # Log completion statistics
            duration = datetime.now() - start_time
            self.log_etl_statistics(total_locations, successful, duration)

        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            raise

    def log_etl_statistics(self, total: int, successful: int, duration: timedelta) -> None:
        """Log detailed ETL statistics"""
        success_rate = (successful / total * 100) if total > 0 else 0
        logger.info("ETL Process Statistics:")
        logger.info("------------------------")
        logger.info(f"Total locations attempted: {total}")
        logger.info(f"Successfully processed: {successful}")
        logger.info(f"Failed to process: {total - successful}")
        logger.info(f"Success rate: {success_rate:.2f}%")
        logger.info(f"Total processing time: {duration}")
        logger.info(f"Average time per location: {duration / total if total > 0 else 0}")
        logger.info("------------------------")