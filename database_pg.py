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
    """Get a PostgreSQL database connection with RealDictCursor using SSL Certs"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cert_dir = os.path.join(base_dir, 'stats_certs')

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="verify-full",
        sslrootcert=os.path.join(cert_dir, 'root.crt'),
        sslcert=os.path.join(cert_dir, 'client.crt'),
        sslkey=os.path.join(cert_dir, 'client.key'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    return conn
