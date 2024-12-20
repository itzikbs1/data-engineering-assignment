import psycopg2
from typing import Dict, Optional
from psycopg2.extensions import connection


class ConnectionDB:
    def __init__(self, db_params: Dict[str, str]):
        self.db_params = db_params
        self.conn: Optional[connection] = None

    def connect(self) -> None:
        """Establish db connection"""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print("Database connection established successfully")
        except Exception as e:
            raise Exception(f"Failed to connect to db: {str(e)}")

    def close(self) -> None:
        """Close db connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")

    def execute_query(self, query: str, params: tuple = None) -> None:
        """Execute a query with parameters"""
        if not self.conn:
            raise Exception("Database connection not established")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Query execution failed: {str(e)}")