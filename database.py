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
            document BLOB,
            link_distance INTEGER DEFAULT 0
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

    # Add link_distance column for crawler depth control
    if 'link_distance' not in cols:
        try:
            cursor.execute('ALTER TABLE urls ADD COLUMN link_distance INTEGER DEFAULT 0')
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

    # Tunebooks table for storing ABC music tunebooks
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tunebooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add status column to tunebooks table
    cursor.execute("PRAGMA table_info(tunebooks)")
    tunebook_cols = [row[1] for row in cursor.fetchall()]
    if 'status' not in tunebook_cols:
        try:
            cursor.execute('ALTER TABLE tunebooks ADD COLUMN status TEXT DEFAULT ""')
        except Exception:
            pass

    # Tunes table for storing individual ABC tunes
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tunes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tunebook_id INTEGER NOT NULL,
            reference_number TEXT,
            title TEXT,
            composer TEXT,
            origin TEXT,
            area TEXT,
            meter TEXT,
            unit_note_length TEXT,
            tempo TEXT,
            parts TEXT,
            transcription TEXT,
            notes TEXT,
            "group" TEXT,
            history TEXT,
            key TEXT,
            rhythm TEXT,
            book TEXT,
            discography TEXT,
            source TEXT,
            instruction TEXT,
            tune_body TEXT NOT NULL,
            pitches TEXT,
            intervals TEXT,
            status TEXT DEFAULT 'parsed',
            skip_reason TEXT,
            FOREIGN KEY (tunebook_id) REFERENCES tunebooks(id)
        )
    ''')

    # Add intervals column if missing (migration)
    cursor.execute("PRAGMA table_info(tunes)")
    tune_cols = [row[1] for row in cursor.fetchall()]
    if 'intervals' not in tune_cols:
        try:
            cursor.execute('ALTER TABLE tunes ADD COLUMN intervals TEXT')
        except Exception:
            pass

    if 'status' not in tune_cols:
        try:
            cursor.execute("ALTER TABLE tunes ADD COLUMN status TEXT DEFAULT 'parsed'")
        except Exception:
            pass
            
    if 'skip_reason' not in tune_cols:
        try:
            cursor.execute("ALTER TABLE tunes ADD COLUMN skip_reason TEXT")
        except Exception:
            pass

    # FAISS mapping table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faiss_mapping (
            faiss_id INTEGER PRIMARY KEY,
            tune_id INTEGER NOT NULL,
            FOREIGN KEY (tune_id) REFERENCES tunes(id)
        )
    ''')


    conn.commit()
    conn.close()

def get_db_connection():
    """Get a database connection with extended timeout and WAL mode enabled"""
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    # Enable Write-Ahead Logging for better concurrency
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

if __name__ == '__main__':
    init_database()
    print(f"Database initialized at {DB_PATH}")

