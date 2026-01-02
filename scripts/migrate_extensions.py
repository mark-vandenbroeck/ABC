import sqlite3
import os
import time
from urllib.parse import urlparse
from pathlib import Path

DB_PATH = 'crawler.db'

def get_extension(url):
    try:
        path = urlparse(url).path
        ext = Path(path).suffix
        if ext:
            return ext[1:].lower() # Remove the dot
        return ''
    except:
        return ''

def migrate():
    print("Starting migration of URL extensions...")
    conn = sqlite3.connect(DB_PATH, timeout=120.0)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    
    cursor = conn.cursor()
    
    batch_size = 1000
    last_id = 0
    total_updated = 0
    
    while True:
        cursor.execute('SELECT id, url FROM urls WHERE id > ? AND (url_extension IS NULL) LIMIT ?', (last_id, batch_size))
        rows = cursor.fetchall()
        if not rows:
            break
            
        updates = []
        for row_id, url in rows:
            ext = get_extension(url)
            updates.append((ext, row_id))
            last_id = row_id
            
        if updates:
            cursor.executemany('UPDATE urls SET url_extension = ? WHERE id = ?', updates)
            conn.commit()
            total_updated += len(updates)
            print(f"Updated {total_updated} URLs...")
            time.sleep(0.1) # Brief pause
            
    conn.close()
    print(f"Migration complete. Total updated: {total_updated}")

if __name__ == '__main__':
    migrate()
