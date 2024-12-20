from typing import List, Dict, Any
from .connectionDB import ConnectionDB
from src.config import TABLE_SCHEMAS


class Database(ConnectionDB):

    def __init__(self, db_params):
        super().__init__(db_params)

        self._valid_location_ids = set()

    def initialize_tables(self) -> None:
        """Initialize raw data tables"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS parameters (
                parameter_id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                display_name VARCHAR(50),
                description TEXT,
                preferred_unit VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS locations (
                location_id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                city VARCHAR(100),
                country VARCHAR(50),
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                is_mobile BOOLEAN,
                entity VARCHAR(100),
                is_analysis BOOLEAN,
                sensor_type VARCHAR(50),
                first_updated TIMESTAMP,
                last_updated TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS measurements (
                measurement_id SERIAL PRIMARY KEY,
                location_id INTEGER REFERENCES locations(location_id),
                parameter VARCHAR(50),
                value DECIMAL(10,2),
                unit VARCHAR(20),
                timestamp_utc TIMESTAMP,
                timestamp_local TIMESTAMP,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                country VARCHAR(2),
                city VARCHAR(100)
            );
            """
        ]

        for query in queries:
            self.execute_query(query)

    def _process_data(self, schema_key: str, data: Dict) -> Dict:
        """Process API data into db format"""
        try:
            if schema_key == 'locations':
                return {
                    'location_id': data['id'],
                    'name': data['name'],
                    'city': data.get('city'),
                    'country': data['country'],
                    'latitude': data['coordinates']['latitude'] if data.get('coordinates') else None,
                    'longitude': data['coordinates']['longitude'] if data.get('coordinates') else None,
                    'is_mobile': data.get('isMobile', False),
                    'entity': data.get('entity'),
                    'is_analysis': data.get('isAnalysis'),
                    'sensor_type': data.get('sensorType'),
                    'first_updated': data.get('firstUpdated'),
                    'last_updated': data.get('lastUpdated')
                }
            elif schema_key == 'parameters':
                return {
                    'parameter_id': data['id'],
                    'name': data['name'],
                    'display_name': data['displayName'],
                    'description': data['description'],
                    'preferred_unit': data['preferredUnit']
                }
            elif schema_key == 'measurements':
                return {
                    # 'measurement_id': data['id'],
                    'location_id': data['locationId'],
                    'parameter': data['parameter'],
                    'value': data['value'],
                    'unit': data['unit'],
                    'timestamp_utc': data['date']['utc'],
                    'timestamp_local': data['date']['local'],
                    'latitude': data['coordinates']['latitude'],
                    'longitude': data['coordinates']['longitude'],
                    'country': data['country'],
                    'city': data.get('city')
                }
        except KeyError as e:
            raise Exception(f"Missing required field in data: {e}")

        return data

    def generic_insert(self, schema_key: str, data_list: List[Dict], additional_data: Dict = None) -> None:
        """Generic insert method that handles all table insertions"""
        schema = TABLE_SCHEMAS[schema_key]

        for data in data_list:

            if schema_key == 'locations':
                self._valid_location_ids.add(data['id'])

            if schema_key == 'measurements':
                if data['locationId'] not in self._valid_location_ids:
                    continue

            try:
                processed_data = self._process_data(schema_key, data)
                if additional_data:
                    processed_data.update(additional_data)
                query, values = self._build_upsert_query(schema, processed_data)
                self.execute_query(query, values)

            except Exception as e:
                print(f"Error inserting {schema_key} data: {str(e)}")
                print(f"Problematic data: {data}")
                continue

    def _build_upsert_query(self, schema: Dict[str, Any], data: Dict[str, Any]) -> tuple:
        """Builds an upsert query based on the schema and data"""
        table_name = schema['table_name']
        columns = schema['columns']
        key_field = schema['key_field']
        update_fields = schema['update_fields']

        columns_data = {k: v for k, v in data.items() if k in columns}
        used_columns = list(columns_data.keys())
        values = [columns_data[col] for col in used_columns]

        placeholders = ', '.join(['%s'] * len(used_columns))
        columns_str = ', '.join(used_columns)

        if key_field:
            update_str = ', '.join(f"{field} = EXCLUDED.{field}" for field in update_fields)

            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({key_field}) DO UPDATE SET
                {update_str}
            """
        else:
            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """
        return query, values