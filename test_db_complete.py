import psycopg2
import time


def test_complete_db_setup():
    # Database configuration
    db_config = {
        'dbname': 'openaq_db',
        'user': 'postgres',
        'password': 'abL148#N',
        'host': 'localhost',
        'port': '5433'
    }

    # Create test table query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );
    """

    # Insert test data query
    insert_data_query = """
    INSERT INTO test_table (name) VALUES ('test_record');
    """

    try:
        print("\n=== Starting Database Test ===")

        # Step 1: Test Connection
        print("\nStep 1: Testing connection...")
        conn = psycopg2.connect(**db_config)
        print("✓ Connection successful")

        # Step 2: Create Test Table
        print("\nStep 2: Creating test table...")
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            print("✓ Test table created")

        # Step 3: Insert Test Data
        print("\nStep 3: Inserting test data...")
        with conn.cursor() as cur:
            cur.execute(insert_data_query)
            conn.commit()
            print("✓ Test data inserted")

        # Step 4: Verify Data
        print("\nStep 4: Verifying data...")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM test_table")
            count = cur.fetchone()[0]
            print(f"✓ Found {count} records in test table")

        # Step 5: List All Tables
        print("\nStep 5: Listing all tables...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cur.fetchall()
            print("Tables found:", [table[0] for table in tables])

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed")


if __name__ == "__main__":
    # Wait a few seconds for Docker to fully initialize
    print("Waiting for database to be ready...")
    time.sleep(5)

    test_complete_db_setup()