import psycopg2
import psycopg2.extras
import os

# Database configuration
DB_NAME = "abc"
DB_USER = "mark"
DB_PASS = "V3nger!12"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_db_connection():
    """Get a PostgreSQL database connection with RealDictCursor"""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    return conn
