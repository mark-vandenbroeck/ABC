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
        print("Migrating mime_types...")
        sqlite_conn = sqlite3.connect('crawler.db')
        sqlite_cursor = sqlite_conn.cursor()
        
        pg_conn = psycopg2.connect(user="mark", password="V3nger!12", host="localhost", port="5432", database="abc")
        pg_cursor = pg_conn.cursor()

        # Truncate
        pg_cursor.execute("TRUNCATE TABLE mime_types RESTART IDENTITY;")

        # Select
        columns = ["id", "pattern", "enabled"]
        sqlite_cursor.execute(f"SELECT {', '.join(columns)} FROM mime_types")
        rows = sqlite_cursor.fetchall()
        print(f"Found {len(rows)} records in SQLite.")

        # Insert
        placeholders = ["%s"] * len(columns)
        insert_query = f"INSERT INTO mime_types ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
        
        batch = []
        count = 0
        for row in rows:
            data = list(row)
            # Sanitize strings and cast booleans
            for i, val in enumerate(data):
                if isinstance(val, str):
                    data[i] = val.replace('\x00', '')
            
            # enabled is at index 2
            data[2] = bool(data[2])

            batch.append(tuple(data))
            
            if len(batch) >= 1000:
                pg_cursor.executemany(insert_query, batch)
                count += len(batch)
                batch = []
        if batch:
            pg_cursor.executemany(insert_query, batch)
            count += len(batch)

        # Sequence
        pg_cursor.execute("SELECT setval('mime_types_id_seq', (SELECT MAX(id) FROM mime_types));")

        pg_conn.commit()
        print(f"Completed mime_types. Total inserted: {count}")

    except (Exception, Error) as error:
        print("Error:", error)
        if pg_conn: pg_conn.rollback()
    finally:
        if sqlite_conn: sqlite_conn.close()
        if pg_conn: pg_conn.close()

if __name__ == "__main__":
    migrate()
