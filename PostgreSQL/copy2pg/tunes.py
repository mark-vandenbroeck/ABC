import sqlite3
import psycopg2
from psycopg2 import Error
import sys
import os

# Add parent directory to path to import config if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def convert_to_array(text_value):
    """Converts a comma-separated string to a list of integers."""
    if not text_value or text_value.strip() == "":
        return None
    try:
        # Split by comma, strip whitespace, and convert to float (now targeted column type)
        return [float(x.strip()) for x in text_value.split(',') if x.strip()]
    except ValueError:

        # Handle cases where conversion fails, though data should be clean ideally
        print(f"Warning: Could not convert '{text_value}' to integer array.")
        return None

def migrate_tunes():
    sqlite_conn = None
    pg_conn = None
    
    try:
        # SQLite Connection
        print("Connecting to SQLite database...")
        sqlite_conn = sqlite3.connect('crawler.db')
        sqlite_conn.row_factory = sqlite3.Row  # Access columns by name
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
        
        # Fetch columns to construct query dynamically or explicitly
        # Explicit mapping ensures control over order and types
        columns = [
            "id", "tunebook_id", "reference_number", "title", "composer", "origin", 
            "area", "meter", "unit_note_length", "tempo", "parts", "transcription", 
            "notes", "group", "history", "key", "rhythm", "book", "discography", 
            "source", "instruction", "tune_body", "intervals", "pitches", 
            "status", "skip_reason"
        ]
        
        # Quote "group" for SQLite query
        sqlite_columns = list(columns)
        sqlite_columns[13] = '"group"'
        
        sqlite_query = f"SELECT {', '.join(sqlite_columns)} FROM tunes"
        
        print("Fetching data from SQLite tunes table...")
        sqlite_cursor.execute(sqlite_query)
        rows = sqlite_cursor.fetchall()
        print(f"Found {len(rows)} records in SQLite.")

        # Prepare PostgreSQL Insert
        # Quote "group" for PG query too
        pg_columns = list(columns)
        pg_columns[13] = '"group"'
        
        placeholders = ["%s"] * len(columns)
        insert_query = f"""
        INSERT INTO tunes ({', '.join(pg_columns)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (id) DO NOTHING;
        """
        
        print("Inserting data into PostgreSQL...")
        
        batch_size = 1000
        batch = []
        count = 0
        
        for row in rows:
            data = dict(row)
            
            # Sanitization: Remove NUL characters from all string fields
            for key, value in data.items():
                if isinstance(value, str):
                    data[key] = value.replace('\x00', '')
            
            # Transform intervals and pitches
            data['intervals'] = convert_to_array(data['intervals'])

            data['pitches'] = convert_to_array(data['pitches'])
            
            # Prepare tuple for insertion in correct order
            values = tuple(data[col] if col != '"group"' else data['group'] for col in columns)
            batch.append(values)
            
            if len(batch) >= batch_size:
                pg_cursor.executemany(insert_query, batch)
                pg_conn.commit()
                count += len(batch)
                print(f"Inserted {count} records...")
                batch = []
        
        if batch:
            pg_cursor.executemany(insert_query, batch)
            pg_conn.commit()
            count += len(batch)
            
        print(f"Migration completed successfully. Total inserted: {count}")
        
        # Verification
        pg_cursor.execute("SELECT count(*) FROM tunes;")
        pg_count = pg_cursor.fetchone()[0]
        print(f"Total records in PostgreSQL tunes table: {pg_count}")

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
    migrate_tunes()
