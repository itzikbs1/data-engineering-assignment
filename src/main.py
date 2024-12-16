import logging
from fetcher import OpenAQFetcher
from db_handler import OpenAQDatabaseHandler


def main():
    db_config = {
        'dbname': 'openaq_db',
        'user': 'postgres',
        'password': 'abL148#N',
        'host': 'localhost',
        'port': '5433'
    }

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting script with name: {__name__}")

    try:
        # Initialize database handler
        db_handler = OpenAQDatabaseHandler(db_config)

        # Create database tables
        logger.info("Initializing database...")
        db_handler.init_db()

        # Initialize API fetcher
        api_key = 'dd4f85c5a1381de52161632b664322cf4bdb10fe24e37da6c745144b7eb018fb'
        fetcher = OpenAQFetcher(api_key)

        # Fetch data
        logger.info("Starting data collection...")
        locations_data = fetcher.fetch_all_locations_with_measurements(max_locations=1000)

        total_locations = len(locations_data)
        logger.info(f"Successfully fetched {total_locations} locations")
        locations_processed = 0
        locations_with_measurements = 0

        # Store each location and its measurements
        for index, location in enumerate(locations_data, 1):
            try:
                logger.info(f"Processing location {index}/{total_locations}: {location['name']}")
                db_handler.store_location(location)
                locations_processed += 1

                if location.get('latest_measurements'):
                    logger.info(
                        f"Storing {len(location['latest_measurements'])} measurements for location: {location['name']}")
                    db_handler.store_measurements(location['latest_measurements'])
                    locations_with_measurements += 1

                # Log progress every 50 locations
                if index % 50 == 0:
                    logger.info(
                        f"Progress: {index}/{total_locations} locations processed ({(index / total_locations) * 100:.2f}%)")

            except Exception as e:
                logger.error(f"Error processing location {location['name']}: {str(e)}")
                continue

        # Log final statistics
        logger.info("Data collection and storage complete!")
        logger.info(f"Total locations processed: {locations_processed}")
        logger.info(f"Locations with measurements: {locations_with_measurements}")
        logger.info(f"Success rate: {(locations_processed / total_locations) * 100:.2f}%")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
