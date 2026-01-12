import sqlite3
import psycopg2
from psycopg2 import Error
import sys
import os

# Add parent directory to path to import config if needed (not needed for this script but good practice)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def migrate_tunebooks():
    sqlite_conn = None
    pg_conn = None
    
    try:
        # SQLite Connection
        print("Connecting to SQLite database...")
        sqlite_conn = sqlite3.connect('crawler.db')
        sqlite_cursor = sqlite_conn.cursor()
        
        # PostgreSQL Connection
        print("Connecting to PostgreSQL database...")
        pg_conn = psycopg2.connect(
            user="mark",
            password="V3nger!12",
            host="localhost",
            port="5432",
            database="abc"
        )
        pg_cursor = pg_conn.cursor()
        
        # Fetch data from SQLite
        print("Fetching data from SQLite tunebooks table...")
        sqlite_cursor.execute("SELECT url, created_at, status, dispatched_at FROM tunebooks")
        rows = sqlite_cursor.fetchall()
        print(f"Found {len(rows)} records in SQLite.")

        # Insert data into PostgreSQL
        print("Inserting data into PostgreSQL...")
        
        insert_query = """
        INSERT INTO tunebooks (url, created_at, status, dispatched_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
        """
        
        for row in rows:
            # SQLite stores timestamps as strings usually, psycopg2 handles strings for timestamp columns well
            pg_cursor.execute(insert_query, row)
            
        pg_conn.commit()
        print("Migration completed successfully.")
        
        # Verification count
        pg_cursor.execute("SELECT count(*) FROM tunebooks;")
        count = pg_cursor.fetchone()[0]
        print(f"Total records in PostgreSQL tunebooks table: {count}")

    except (Exception, Error) as error:
        print("Error while migrating data:", error)
        if pg_conn:
            pg_conn.rollback()
            
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        if pg_conn:
            pg_cursor.close()
            pg_conn.close()
            print("PostgreSQL connection is closed")

if __name__ == "__main__":
    migrate_tunebooks()
