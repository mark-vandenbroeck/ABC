import sqlite3
import os
from database import get_db_connection

def  debug_tune(tune_id):
    print(f"--- Debugging Tune {tune_id} ---")
    try:
        conn = get_db_connection()
        cwd = os.getcwd()
        print(f"CWD: {cwd}")
        
        # Check database file path resolution
        db_path = 'crawler.db' # This is what database.py probably uses if relative
        print(f"Looking for DB at: {os.path.abspath(db_path)}")
        
        cursor = conn.cursor()
        cursor.execute('SELECT intervals FROM tunes WHERE id = ?', (tune_id,))
        row = cursor.fetchone()
        
        if row is None:
            print("Row is None (Tune not found)")
        else:
            intervals = row[0]
            print(f"Row found. Type of intervals: {type(intervals)}")
            print(f"Value of intervals: '{intervals}'")
            if not intervals:
                print("Intervals evaluates to False")
            else:
                print("Intervals evaluates to True")
                
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_tune(51864)
