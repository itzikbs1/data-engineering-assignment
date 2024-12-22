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


TABLE_SCHEMAS = {
    'parameters': {
        'table_name': 'parameters',
        'columns': [
            'parameter_id',
            'name',
            'display_name',
            'description',
            'preferred_unit'
        ],
        'key_field': 'parameter_id',
        'update_fields': ['name', 'display_name', 'description', 'preferred_unit']
    },

    'locations': {
        'table_name': 'locations',
        'columns': [
            'location_id',
            'name',
            'city',
            'country',
            'latitude',
            'longitude',
            'is_mobile',
            'entity',
            'is_analysis',
            'sensor_type',
            'first_updated',
            'last_updated'
        ],
        'key_field': 'location_id',
        'update_fields': ['name', 'city', 'is_mobile', 'entity', 'sensor_type', 'last_updated']
    },

    'measurements': {
        'table_name': 'measurements',
        'columns': [
            'measurement_id',
            'location_id',
            'parameter',
            'value',
            'unit',
            'timestamp_utc',
            'timestamp_local',
            'latitude',
            'longitude',
            'country',
            'city'
        ],
        'key_field': None,
        'update_fields': []
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