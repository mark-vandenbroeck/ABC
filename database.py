import sqlite3
import os
from datetime import datetime
from urllib.parse import urlparse

DB_PATH = 'crawler.db'

def init_database():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # URLs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            downloaded_at TIMESTAMP,
            size_bytes INTEGER,
            status TEXT DEFAULT '',
            mime_type TEXT,
            document BLOB
        )
    ''')
    
    # MIME types configuration table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mime_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT UNIQUE NOT NULL,
            enabled INTEGER DEFAULT 1
        )
    ''')
    
    # Default MIME types (HTML and text)
    default_mimes = [
        ('text/html', 1),
        ('text/plain', 1),
        ('text/*', 1),
    ]
    
    cursor.executemany('''
        INSERT OR IGNORE INTO mime_types (pattern, enabled) VALUES (?, ?)
    ''', default_mimes)
    
    # Processes table to track running processes
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pid INTEGER,
            type TEXT NOT NULL,
            status TEXT DEFAULT 'running',
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Ensure http_status and retries columns exist (for upgrades)
    cursor.execute("PRAGMA table_info(urls)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'http_status' not in cols:
        try:
            cursor.execute('ALTER TABLE urls ADD COLUMN http_status INTEGER')
        except Exception:
            pass
    if 'retries' not in cols:
        try:
            cursor.execute('ALTER TABLE urls ADD COLUMN retries INTEGER DEFAULT 0')
        except Exception:
            pass
    # Add dispatched_at column to track when a URL was sent to a fetcher (for timeout/retry)
    if 'dispatched_at' not in cols:
        try:
            cursor.execute('ALTER TABLE urls ADD COLUMN dispatched_at TIMESTAMP')
        except Exception:
            pass

    # Add host column to urls for efficient per-host scheduling and backfill existing rows
    if 'host' not in cols:
        try:
            cursor.execute('ALTER TABLE urls ADD COLUMN host TEXT')
            # Backfill host values for existing rows
            cursor.execute('SELECT id, url FROM urls WHERE host IS NULL OR host = ""')
            rows_to_update = cursor.fetchall()
            for rid, rurl in rows_to_update:
                try:
                    h = urlparse(rurl).hostname
                    if h:
                        cursor.execute('UPDATE urls SET host = ? WHERE id = ?', (h, rid))
                except Exception:
                    continue
        except Exception:
            pass

    # Ensure hosts table exists to track per-host access times
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hosts (
            host TEXT PRIMARY KEY,
            last_access TIMESTAMP,
            last_http_status INTEGER,
            downloads INTEGER DEFAULT 0
        )
    ''')

    # Add downloads column if missing (migration)
    cursor.execute("PRAGMA table_info(hosts)")
    host_cols = [row[1] for row in cursor.fetchall()]
    if 'downloads' not in host_cols:
        try:
            cursor.execute('ALTER TABLE hosts ADD COLUMN downloads INTEGER DEFAULT 0')
        except Exception:
            pass

    # Add disabled column to hosts to allow disabling problematic hosts
    if 'disabled' not in host_cols:
        try:
            cursor.execute('ALTER TABLE hosts ADD COLUMN disabled INTEGER DEFAULT 0')
        except Exception:
            pass

    # Add columns to record why/when a host was disabled (reason and timestamp)
    cursor.execute("PRAGMA table_info(hosts)")
    host_cols = [row[1] for row in cursor.fetchall()]
    if 'disabled_reason' not in host_cols:
        try:
            cursor.execute("ALTER TABLE hosts ADD COLUMN disabled_reason TEXT")
        except Exception:
            pass
    if 'disabled_at' not in host_cols:
        try:
            cursor.execute("ALTER TABLE hosts ADD COLUMN disabled_at TIMESTAMP")
        except Exception:
            pass

    # Backfill host values for any existing URLs that still have host NULL/empty
    try:
        cursor.execute("SELECT COUNT(*) FROM urls WHERE host IS NULL OR host = ''")
        missing_hosts = cursor.fetchone()[0]
        if missing_hosts > 0:
            print(f"Backfilling host column for {missing_hosts} urls...")
            cursor.execute("SELECT id, url FROM urls WHERE host IS NULL OR host = ''")
            rows_to_update = cursor.fetchall()
            for rid, rurl in rows_to_update:
                try:
                    h = urlparse(rurl).hostname
                    if h:
                        cursor.execute('UPDATE urls SET host = ? WHERE id = ?', (h, rid))
                except Exception:
                    continue
    except Exception:
        pass

    # Add an index on urls.host for faster joins and filtering
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_urls_host ON urls(host)')
    except Exception:
        pass

    conn.commit()
    conn.close()

def get_db_connection():
    """Get a database connection"""
    return sqlite3.connect(DB_PATH)

if __name__ == '__main__':
    init_database()
    print(f"Database initialized at {DB_PATH}")

