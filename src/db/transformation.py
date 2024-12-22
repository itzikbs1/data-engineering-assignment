from .connectionDB import ConnectionDB


class DataWarehouseTransformer(ConnectionDB):
    def _create_dimension_tables(self):
        """Create dimension tables for the data warehouse"""
        dimension_queries = [
            """
            CREATE TABLE IF NOT EXISTS dim_locations (
                location_key SERIAL PRIMARY KEY,
                original_location_id INTEGER UNIQUE,
                location_name VARCHAR(100),
                city VARCHAR(255),
                country VARCHAR(50),
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                is_mobile BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_parameters (
                parameter_key SERIAL PRIMARY KEY,
                original_parameter_id INTEGER UNIQUE,
                parameter_name VARCHAR(50),
                description TEXT,
                preferred_unit VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_time (
                time_key SERIAL PRIMARY KEY,
                date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                is_weekend BOOLEAN
            );
            """
        ]

        for query in dimension_queries:
            self.execute_query(query)

    def _populate_dimension_tables(self):
        """Populate dimension tables from raw data"""
        dimension_population_queries = [
            """
            INSERT INTO dim_locations (
                original_location_id,
                location_name,
                city,
                country,
                latitude,
                longitude,
                is_mobile
            )
            SELECT DISTINCT
                location_id,
                name,
                city,
                country,
                latitude,
                longitude,
                is_mobile
            FROM locations
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_locations dl
                WHERE dl.original_location_id = locations.location_id
            );
            """,
            """
            INSERT INTO dim_parameters (
                original_parameter_id,
                parameter_name,
                description,
                preferred_unit
            )
            SELECT DISTINCT
                parameter_id,
                name,
                description,
                preferred_unit
            FROM parameters
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_parameters dp
                WHERE dp.original_parameter_id = parameters.parameter_id
            );
            """,
            """
            INSERT INTO dim_time (
                date,
                year,
                month,
                day,
                hour,
                is_weekend
            )
            SELECT DISTINCT
                DATE(timestamp_utc) as date,
                EXTRACT(YEAR FROM timestamp_utc) as year,
                EXTRACT(MONTH FROM timestamp_utc) as month,
                EXTRACT(DAY FROM timestamp_utc) as day,
                EXTRACT(HOUR FROM timestamp_utc) as hour,
                EXTRACT(ISODOW FROM timestamp_utc) IN (6, 7) as is_weekend
            FROM measurements
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_time dt
                WHERE dt.date = DATE(measurements.timestamp_utc)
            );
            """
        ]

        for query in dimension_population_queries:
            self.execute_query(query)

    def _create_fact_table(self):
        """Create fact table for air quality measurements"""
        fact_table_query = """
        CREATE TABLE IF NOT EXISTS fact_air_quality (
            measurement_key BIGSERIAL PRIMARY KEY,
            location_key INTEGER,
            parameter_key INTEGER,
            time_key INTEGER,
            measurement_value DECIMAL(10,2),
            unit VARCHAR(20),
            FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
            FOREIGN KEY (parameter_key) REFERENCES dim_parameters(parameter_key),
            FOREIGN KEY (time_key) REFERENCES dim_time(time_key)
        );
        """

        self.execute_query(fact_table_query)

    def _populate_fact_table(self):
        """Populate fact table with measurements_with_keys"""
        fact_population_query = """
        WITH measurements_with_keys AS (
            SELECT 
                m.value as measurement_value,
                m.unit,
                m.timestamp_utc,
                l.location_key,
                p.parameter_key,
                t.time_key
            FROM measurements m
            JOIN dim_locations l ON m.location_id = l.original_location_id
            JOIN dim_parameters p ON m.parameter = p.parameter_name
            JOIN dim_time t ON DATE(m.timestamp_utc) = t.date
                AND EXTRACT(HOUR FROM m.timestamp_utc) = t.hour
        )
        INSERT INTO fact_air_quality (
            location_key,
            parameter_key,
            time_key,
            measurement_value,
            unit
        )
        SELECT DISTINCT
            location_key,
            parameter_key,
            time_key,
            measurement_value,
            unit
        FROM measurements_with_keys;
        """

        self.execute_query(fact_population_query)

    def run_transformation(self):
        """Execute full data warehouse transformation"""
        try:
            self.connect()
            self._create_dimension_tables()
            self._populate_dimension_tables()
            self._create_fact_table()
            self._populate_fact_table()
        except Exception as e:
            print(f"Transformation error: {str(e)}")
        finally:
            self.close()