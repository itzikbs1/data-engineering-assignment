import psycopg2
from psycopg2 import Error


def test_db_connection():
    db_config = {
        'dbname': 'openaq_db',
        'user': 'postgres',
        'password': 'abL148#N',
        'host': 'localhost',
        'port': '5433'
    }

    try:
        # Connect to database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Print connection properties
        print("PostgreSQL connection is working!")
        print("Connection parameters:", connection.get_dsn_parameters())

        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("PostgreSQL version:", version)

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


if __name__ == "__main__":
    test_db_connection()