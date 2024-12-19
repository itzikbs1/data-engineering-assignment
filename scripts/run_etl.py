from src.etl.processor import ETLProcessor
from src.database.repository import Repository
from config.logging_config import setup_logging

logger = setup_logging(__name__)


def main():
    try:
        logger.info("Starting ETL process")

        # Initialize database tables
        repository = Repository()
        repository.initialize_tables()
        logger.info("Database tables initialized")

        # Run ETL process
        processor = ETLProcessor()
        processor.process(max_locations=5)  # Start with 5 locations for testing

        logger.info("ETL process completed")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise


if __name__ == "__main__":
    main()