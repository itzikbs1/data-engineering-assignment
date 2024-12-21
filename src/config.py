import os
from typing import Dict
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(env_path)


def get_db_params() -> Dict[str, str]:
    return {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT")
    }


def get_api_config() -> Dict[str, any]:
    return {
        "BASE_URL": os.getenv("API_BASE_URL"),
        "API_KEY": os.getenv("API_KEY"),
        "DEFAULT_LIMIT": int(os.getenv("API_LIMIT", "100")),
        "REQUEST_DELAY": int(os.getenv("API_DELAY", "1"))
    }


# Updated schema definitions based on OpenAQ API v2
TABLE_SCHEMAS = {
    'parameters': {
        'table_name': 'parameters',
        'columns': [
            'parameter_id',  # from 'id' in /parameters
            'name',  # e.g., 'pm25', 'pm10'
            'display_name',  # e.g., 'PM2.5', 'PM10'
            'description',  # full text description
            'preferred_unit'  # e.g., 'µg/m³'
        ],
        'key_field': 'parameter_id',
        'update_fields': ['name', 'display_name', 'description', 'preferred_unit']
    },

    'locations': {
        'table_name': 'locations',
        'columns': [
            'location_id',  # from 'id' in /locations
            'name',  # location name
            'city',  # city name
            'country',  # country code (e.g., 'AU')
            'latitude',  # from coordinates
            'longitude',  # from coordinates
            'is_mobile',  # boolean
            'entity',  # e.g., 'Governmental Organization'
            'is_analysis',  # boolean
            'sensor_type',  # e.g., 'reference grade'
            'first_updated',  # timestamp
            'last_updated'  # timestamp
        ],
        'key_field': 'location_id',
        'update_fields': ['name', 'city', 'is_mobile', 'entity', 'sensor_type', 'last_updated']
    },

    'measurements': {
        'table_name': 'measurements',
        'columns': [
            'measurement_id',  # auto-generated
            'location_id',  # foreign key to locations
            'parameter',  # e.g., 'pm25'
            'value',  # numeric value
            'unit',  # e.g., 'µg/m³'
            'timestamp_utc',  # from date.utc
            'timestamp_local',  # from date.local
            'latitude',  # measurement location
            'longitude',  # measurement location
            'country',  # country code
            'city'  # city name
        ],
        'key_field': None,
        'update_fields': []  # measurements are immutable
    }
}

# Data Warehouse schema definitions
DW_SCHEMAS = {
    'dim_location': {
        'table_name': 'dim_location',
        'columns': [
            'location_key',  # surrogate key
            'location_id',  # natural key from raw
            'location_name',
            'country',
            'city',
            'latitude',
            'longitude'
        ],
        'key_field': 'location_key'
    },

    'dim_time': {
        'table_name': 'dim_time',
        'columns': [
            'time_key',  # surrogate key
            'full_date',
            'year',
            'quarter',
            'month',
            'day',
            'hour',
            'is_weekend'
        ],
        'key_field': 'time_key'
    },

    'dim_parameter': {
        'table_name': 'dim_parameter',
        'columns': [
            'parameter_key',  # surrogate key
            'parameter_id',  # natural key from raw
            'parameter_name',
            'display_name',
            'description',
            'preferred_unit'
        ],
        'key_field': 'parameter_key'
    },

    'fact_air_quality': {
        'table_name': 'fact_air_quality',
        'columns': [
            'measurement_key',  # surrogate key
            'location_key',  # FK to dim_location
            'time_key',  # FK to dim_time
            'parameter_key',  # FK to dim_parameter
            'measurement_value',
            'unit'
        ],
        'key_field': 'measurement_key'
    }
}