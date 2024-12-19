from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from config.settings import settings
from config.logging_config import setup_logging

logger = setup_logging(__name__)


class DatabaseConnection:
    def __init__(self):
        self.conn_params = {
            'dbname': settings.DB_NAME,
            'user': settings.DB_USER,
            'password': settings.DB_PASSWORD,
            'host': settings.DB_HOST,
            'port': settings.DB_PORT
        }

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(**self.conn_params)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
                conn.commit() # Commit if no errors
            except Exception as e:
                conn.rollback() # Rollback in case of error
                logger.error(f"Database error: {str(e)}")
                raise

    def init_db(self):
        """Initialize database tables"""
        create_tables_query = """
        CREATE TABLE IF NOT EXISTS locations (
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
            sensor_ids INTEGER[],  -- Added this field
            last_update TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sensors (
            id INTEGER PRIMARY KEY,
            location_id INTEGER REFERENCES locations(id),
            name VARCHAR(255),
            parameter_name VARCHAR(100),
            parameter_display_name VARCHAR(100),
            units VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS measurements (
            id SERIAL PRIMARY KEY,
            location_id INTEGER REFERENCES locations(id),
            sensor_id INTEGER REFERENCES sensors(id),
            value FLOAT,
            datetime TIMESTAMP,
            coordinates JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with self.get_cursor() as cursor:
            cursor.execute(create_tables_query)
            logger.info("Database tables initialized successfully")
