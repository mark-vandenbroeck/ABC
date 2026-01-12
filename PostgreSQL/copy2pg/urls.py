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
        print("Migrating urls...")
        sqlite_conn = sqlite3.connect('crawler.db')
        sqlite_cursor = sqlite_conn.cursor()
        
        pg_conn = psycopg2.connect(user="mark", password="V3nger!12", host="localhost", port="5432", database="abc")
        pg_cursor = pg_conn.cursor()

        # Truncate
        pg_cursor.execute("TRUNCATE TABLE urls RESTART IDENTITY;")

        # Select
        # Note: SQLite has 'dispatched_at', Postgres has 'dispatched_at'.
        # Assuming types match closely enough (TIMESTAMP string -> TIMESTAMP WITH TIME ZONE)
        columns = [
            "id", "url", "created_at", "downloaded_at", "size_bytes", "status", 
            "mime_type", "document", "http_status", "retries", "dispatched_at", 
            "host", "has_abc", "link_distance", "url_extension"
        ]
        
        # SQLite SELECT query
        sqlite_cursor.execute(f"SELECT {', '.join(columns)} FROM urls")
        
        # Insert Query
        placeholders = ["%s"] * len(columns)
        insert_query = f"INSERT INTO urls ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
        
        batch = []
        count = 0
        batch_size = 500 # Smaller batch size for potential large BLOBs
        
        while True:
            rows = sqlite_cursor.fetchmany(batch_size)
            if not rows:
                break
                
            for row in rows:
                data = list(row)
                # Sanitize strings (but NOT the blob 'document' which is at index 7)
                for i, val in enumerate(data):
                    if i == 7: # document column
                        continue
                    if isinstance(val, str):
                        data[i] = val.replace('\x00', '')

                # has_abc is at index 12 (based on column list in script)
                data[12] = bool(data[12])

                batch.append(tuple(data))
            
            pg_cursor.executemany(insert_query, batch)
            count += len(batch)
            pg_conn.commit() # Commit frequently for large data
            print(f"Inserted {count} records...")
            batch = []

        # Sequence
        pg_cursor.execute("SELECT setval('urls_id_seq', (SELECT MAX(id) FROM urls));")

        pg_conn.commit()
        print(f"Completed urls. Total inserted: {count}")

    except (Exception, Error) as error:
        print("Error:", error)
        if pg_conn: pg_conn.rollback()
    finally:
        if sqlite_conn: sqlite_conn.close()
        if pg_conn: pg_conn.close()

if __name__ == "__main__":
    migrate()
