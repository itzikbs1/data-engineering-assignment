from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import logging
from fetcher import OpenAQFetcher
from db_handler import OpenAQDatabaseHandler


def etl_job():
    """ETL job that will be scheduled"""
    logging.info(f"Starting ETL job at {datetime.now()}")

    db_config = {
        'dbname': 'openaq_db',
        'user': 'postgres',
        'password': 'abL148#N',
        'host': 'postgres',
        'port': '5432'
    }

    try:
        # Initilaize database handler
        db_handler = OpenAQDatabaseHandler(db_config)
        db_handler.init_db()

        # Initialize API fetcher
        api_key = 'dd4f85c5a1381de52161632b664322cf4bdb10fe24e37da6c745144b7eb018fb'
        fetcher = OpenAQFetcher(api_key)

        # Fetch data
        logging.info("Fetching data from OpenAQ API...")
        locations_data = fetcher.fetch_all_locations_with_measurements(max_locations=100)

        total_locations = len(locations_data)
        logging.info(f"Successfully fetched {total_locations} locations")
        locations_processed = 0
        locations_with_measurements = 0

        # Store each location and its measurements
        for index, location in enumerate(locations_data, 1):
            try:
                logging.info(f"Processing location {index}/{total_locations}: {location['name']}")
                db_handler.store_location(location)
                locations_processed += 1

                if location.get('latest_measurements'):
                    db_handler.store_measurements(location['latest_measurements'])
                    locations_with_measurements += 1

            except Exception as e:
                logging.error(f"Error processing location {location['name']}: {str(e)}")
                continue

        # Log completion statistics
        logging.info("ETL job completed!")
        logging.info(f"Total locations processed: {locations_processed}")
        logging.info(f"Locations with measurements: {locations_with_measurements}")
        logging.info(f"Success rate: {(locations_processed / total_locations) * 100:.2f}%")

    except Exception as e:
        logging.error(f"ETL job failed: {str(e)}")


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create scheduler
    scheduler = BlockingScheduler()

    # Schedule job to run every hour
    scheduler.add_job(
        etl_job,
        trigger=IntervalTrigger(minutes=10),
        next_run_time=datetime.now()  # Run immediately when starting
    )

    try:
        logging.info("Starting scheduler...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped")


if __name__ == "__main__":
    main()
