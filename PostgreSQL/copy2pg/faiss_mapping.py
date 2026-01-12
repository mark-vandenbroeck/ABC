import sqlite3
import psycopg2
from psycopg2 import Error
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def migrate():
    sqlite_conn = None
    pg_conn = None
    
    try:
        print("Migrating faiss_mapping...")
        sqlite_conn = sqlite3.connect('crawler.db')
        sqlite_cursor = sqlite_conn.cursor()
        
        pg_conn = psycopg2.connect(user="mark", password="V3nger!12", host="localhost", port="5432", database="abc")
        pg_cursor = pg_conn.cursor()

        # Truncate
        pg_cursor.execute("TRUNCATE TABLE faiss_mapping;")

        # Select
        sqlite_cursor.execute("SELECT faiss_id, tune_id FROM faiss_mapping")
        rows = sqlite_cursor.fetchall()
        print(f"Found {len(rows)} records in SQLite.")

        # Insert
        insert_query = "INSERT INTO faiss_mapping (faiss_id, tune_id) VALUES (%s, %s);"
        
        batch = []
        count = 0
        for row in rows:
            batch.append(row) # IDs are integers, no string sanitization needed
            if len(batch) >= 1000:
                pg_cursor.executemany(insert_query, batch)
                count += len(batch)
                batch = []
        if batch:
            pg_cursor.executemany(insert_query, batch)
            count += len(batch)

        pg_conn.commit()
        print(f"Completed faiss_mapping. Total inserted: {count}")

    except (Exception, Error) as error:
        print("Error:", error)
        if pg_conn: pg_conn.rollback()
    finally:
        if sqlite_conn: sqlite_conn.close()
        if pg_conn: pg_conn.close()

if __name__ == "__main__":
    migrate()
