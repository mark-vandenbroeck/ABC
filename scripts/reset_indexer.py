import sqlite3
import os
from database import DB_PATH, get_db_connection

def reset_indexing():
    print(f"Resetting indexing state in {DB_PATH}...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Clear mapping table
    print("Clearing faiss_mapping table...")
    cursor.execute("DELETE FROM faiss_mapping")
    
    # 2. Reset tunebook status
    print("Resetting tunebook status to allow re-indexing...")
    cursor.execute("UPDATE tunebooks SET status = ''")
    
    # 3. Clear existing intervals in tunes (optional, but clean)
    print("Clearing intervals in tunes table...")
    cursor.execute("UPDATE tunes SET intervals = NULL")
    
    conn.commit()
    conn.close()
    
    # 4. Delete FAISS index file
    index_path = "data/tunes.index"
    if os.path.exists(index_path):
        print(f"Deleting {index_path}...")
        os.remove(index_path)
    
    print("Reset complete. Please restart the indexer.")

if __name__ == "__main__":
    reset_indexing()
