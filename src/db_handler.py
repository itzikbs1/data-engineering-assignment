import psycopg2
from psycopg2.extras import execute_batch
import json
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class OpenAQDatabaseHandler:
    def __init__(self, db_params: Dict):
        """Initialize database connection parameters"""
        self.db_params = db_params

    def init_db(self):
            """Create the necessary tables if they don't exist"""
            create_tables_query = """
            --- Raw locations table
            CREATE TABLE IF NOT EXISTS raw_locations (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                country_code VARCHAR(2),
                country_name VARCHAR(255),
                city VARCHAR(255),
                coordinates JSONB,
                timezone VARCHAR(100),
                owner_name VARCHAR(255),
                provider_name VARCHAR(255),
                is_mobile BOOLEAN,
                is_monitor BOOLEAN,
                source_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Raw sensors table
            CREATE TABLE IF NOT EXISTS raw_sensors (
                id INTEGER PRIMARY KEY,
                location_id INTEGER REFERENCES raw_locations(id),
                name VARCHAR(255),
                parameter_name VARCHAR(100),
                parameter_display_name VARCHAR(100),
                units VARCHAR(50),
                source_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Raw measurements table
            CREATE TABLE IF NOT EXISTS raw_measurements (
                id SERIAL PRIMARY KEY,
                location_id INTEGER REFERENCES raw_locations(id),
                sensor_id INTEGER REFERENCES raw_sensors(id),
                value FLOAT,
                datetime TIMESTAMP,
                coordinates JSONB,
                source_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            print("\nAttempting to initialize database...")
            try:
                with psycopg2.connect(**self.db_params) as conn:
                    print(f"Connected to database: {self.db_params['dbname']}")
                    print(f"Using port: {self.db_params['port']}")

                    with conn.cursor() as cur:
                        print("Creating tables...")
                        cur.execute(create_tables_query)
                        conn.commit()
                        print("Tables created and committed")

                        # Verify tables
                        cur.execute("""
                            SELECT table_name 
                            FROM information_schema.tables 
                        """)
                        tables = cur.fetchall()
                        print("Existing tables:", [table[0] for table in tables])

                        # Verify we can query the tables
                        for table in ['raw_locations', 'raw_sensors', 'raw_measurements']:
                            try:
                                cur.execute(f"SELECT COUNT(*) FROM {table}")
                                count = cur.fetchone()[0]
                                print(f"Table {table} exists and has {count} records")
                            except Exception as e:
                                print(f"Error querying {table}: {str(e)}")

            except Exception as e:
                print(f"Error initializing database: {str(e)}")
                raise
    # def init_db(self):
    #     """Create the necessary tables if they don't exist"""
    #     create_tables_query = """
    #     --- Raw locations table
    #     CREATE TABLE IF NOT EXISTS raw_locations (
    #         id INTEGER PRIMARY KEY,
    #         name VARCHAR(255),
    #         country_code VARCHAR(2),
    #         country_name VARCHAR(255),
    #         city VARCHAR(255),
    #         coordinates JSONB,
    #         timezone VARCHAR(100),
    #         owner_name VARCHAR(255),
    #         provider_name VARCHAR(255),
    #         is_mobile BOOLEAN,
    #         is_monitor BOOLEAN,
    #         source_data JSONB,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #     );
    #
    #     -- Raw sensors table
    #     CREATE TABLE IF NOT EXISTS raw_sensors (
    #         id INTEGER PRIMARY KEY,
    #         location_id INTEGER REFERENCES raw_locations(id),
    #         name VARCHAR(255),
    #         parameter_name VARCHAR(100),
    #         parameter_display_name VARCHAR(100),
    #         units VARCHAR(50),
    #         source_data JSONB,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #     );
    #
    #     -- Raw measurements table
    #     CREATE TABLE IF NOT EXISTS raw_measurements (
    #         id SERIAL PRIMARY KEY,
    #         location_id INTEGER REFERENCES raw_locations(id),
    #         sensor_id INTEGER REFERENCES raw_sensors(id),
    #         value FLOAT,
    #         datetime TIMESTAMP,
    #         coordinates JSONB,
    #         source_data JSONB,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #     );
    #     """
    #
    #     try:
    #         logger.info(f"Database parameters: {self.db_params}")
    #         with psycopg2.connect(**self.db_params) as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute(create_tables_query)
    #             conn.commit()
    #             logger.info("Database tables created successfully")
    #
    #         ###########################
    #         # Verify tables were created
    #         with conn.cursor() as cur:
    #             cur.execute("""
    #                 SELECT table_name
    #                 FROM information_schema.tables
    #                 WHERE table_schema = 'public'
    #             """)
    #             tables = cur.fetchall()
    #             print("Created tables:", [table[0] for table in tables])
    #         ###########################
    #
    #     except Exception as e:
    #         logger.error(f"Error creating database tables: {str(e)}")
    #         raise

    def store_location(self, location: Dict):
        """Store a location in the database"""

        insert_query = """
        INSERT INTO raw_locations (
            id, name, country_code, country_name, city, coordinates,
            timezone, owner_name, provider_name, is_mobile, is_monitor, source_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
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
            source_data = EXCLUDED.source_data;
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_query, (
                        location['id'],
                        location['name'],
                        location['country']['code'],
                        location['country']['name'],
                        location.get('locality'),
                        json.dumps(location['coordinates']),
                        location['timezone'],
                        location['owner']['name'],
                        location['provider']['name'],
                        location['isMobile'],
                        location['isMonitor'],
                        json.dumps(location)
                    ))
                conn.commit()
                logger.info(f"Stored location: {location['name']}")

                # Store sensors for this location
                if 'sensors' in location:
                    self.store_sensors(location['id'], location['sensors'])

        except Exception as e:
            logger.error(f"Error storing location {location.get('id')}: {str(e)}")
            raise

    def store_sensors(self, location_id: int, sensors: List[Dict]):
        """Store sensors for a location"""

        insert_query = """
        INSERT INTO raw_sensors (
            id, location_id, name, parameter_name, parameter_display_name, units, source_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (id) DO UPDATE SET
            location_id = EXCLUDED.location_id,
            name = EXCLUDED.name,
            parameter_name = EXCLUDED.parameter_name,
            parameter_display_name = EXCLUDED.parameter_display_name,
            units = EXCLUDED.units,
            source_data = EXCLUDED.source_data;
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    for sensor in sensors:
                        cur.execute(insert_query, (
                            sensor['id'],
                            location_id,
                            sensor['name'],
                            sensor['parameter']['name'],
                            sensor['parameter']['displayName'],
                            sensor['parameter']['units'],
                            json.dumps(sensor)
                        ))
                conn.commit()
                logger.info(f"Stored {len(sensors)} sensors for location {location_id}")
        except Exception as e:
            logger.error(f"Error storing sensors for location {location_id}: {str(e)}")
            raise

    def store_measurements(self, measurements: List[Dict]):
        """Store measurements in batch"""

        insert_query = """
            INSERT INTO raw_measurements (
                location_id, sensor_id, value, datetime, coordinates, source_data
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """

        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    measurement_data = [
                        (
                            m['locationsId'],
                            m['sensorsId'],
                            m['value'],
                            m['datetime']['utc'],
                            json.dumps(m['coordinates']),
                            json.dumps(m)
                        )
                        for m in measurements
                    ]
                    execute_batch(cur, insert_query, measurement_data)
                conn.commit()
                logger.info(f"Stored {len(measurements)} measurements")
        except Exception as e:
            logger.error(f"Error storing measurements: {str(e)}")
            raise
