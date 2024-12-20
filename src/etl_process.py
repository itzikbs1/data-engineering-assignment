import logging
from .db import Database
from .api import OpenAQClient
from .db import DataWarehouseTransformer
from .config import get_db_params, get_api_config


class AirQualityETL:
    def __init__(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Get configuration
        self.db_params = get_db_params()
        self.api_config = get_api_config()

        # Initialize components
        self.raw_db = Database(self.db_params)
        self.api = OpenAQClient(
            base_url=self.api_config['BASE_URL'],
            api_key=self.api_config['API_KEY'],
            limit=self.api_config['DEFAULT_LIMIT'],
            delay=self.api_config['REQUEST_DELAY']
        )
        self.warehouse_transformer = DataWarehouseTransformer
        self.transformer = self.warehouse_transformer(self.db_params)

    def run(self) -> None:
        """Main ETL process"""
        try:
            # Step 1: Extract and load raw data
            self.logger.info("Starting raw data extraction and loading...")
            self._extract_and_load_raw_data()

            # Step 2: Transform to data warehouse
            self.logger.info("Starting data warehouse transformation...")
            self.transformer.run_transformation()

            self.logger.info("ETL process completed successfully")

        except Exception as e:
            self.logger.error(f"Error during ETL process: {str(e)}")

    def _extract_and_load_raw_data(self) -> None:
        """Extract data from API and load into raw tables"""
        try:
            self.raw_db.connect()
            self.raw_db.initialize_tables()

            # 1. Extract and load parameters
            self.logger.info("Fetching parameters...")
            parameters_data = self.api.generic_get('parameters')
            if parameters_data:
                self.raw_db.generic_insert('parameters', parameters_data)
                self.logger.info(f"Loaded {len(parameters_data)} parameters")

            # 2. Extract and load locations
            self.logger.info("Fetching locations...")
            locations_data = self.api.generic_get('locations')
            if locations_data:
                self.raw_db.generic_insert('locations', locations_data)
                self.logger.info(f"Loaded {len(locations_data)} locations")

            # 3. Extract and load measurements
            self.logger.info("Fetching measurements...")
            measurements_data = self.api.generic_get('measurements')
            if measurements_data:
                self.raw_db.generic_insert('measurements', measurements_data)
                self.logger.info(f"Loaded {len(measurements_data)} measurements")

        except Exception as e:
            self.logger.error(f"Error in extract and load process: {str(e)}")
            raise
        finally:
            self.raw_db.close()
