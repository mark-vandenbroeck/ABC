import os
import sys
import signal
import time
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from database_pg import get_db_connection

# Log file in the logs directory
# Resolve relative to script location
LOG_FILE = Path(__file__).resolve().parent / 'logs' / 'purger.log'
logger = logging.getLogger('url_purger_pg')
logger.setLevel(logging.INFO)

if not logger.handlers:
    os.makedirs(LOG_FILE.parent, exist_ok=True)
    # 3 MB = 3145728 bytes
    fh = RotatingFileHandler(LOG_FILE, maxBytes=3145728, backupCount=4)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)

class URLPurger:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self._write_pid()

    def _write_pid(self):
        try:
            os.makedirs('run', exist_ok=True)
            pid_file = os.path.join('run', 'purger.pid')
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
        except Exception as e:
            logger.warning(f"Could not write PID file: {e}")

    def signal_handler(self, sig, frame):
        logger.info("Shutting down purger...")
        self.running = False
        sys.exit(0)

    def purge(self):
        """Perform the purging logic"""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            
            # 1. Purge URLs with refused extensions
            cursor.execute('SELECT extension FROM refused_extensions')
            refused_exts = [row['extension'] if isinstance(row, dict) else row[0] for row in cursor.fetchall()]
            
            if refused_exts:
                # Use standard PostgreSQL logic
                # Delete in batches
                while True:
                    # 'url_extension' column usage
                    # Use ANY(array) for cleaner syntax
                    cursor.execute("""
                        WITH deleted AS (
                            DELETE FROM urls 
                            WHERE id IN (
                                SELECT id FROM urls 
                                WHERE url_extension = ANY(%s)
                                LIMIT 500
                            )
                            RETURNING id
                        )
                        SELECT count(*) FROM deleted
                    """, (refused_exts,))
                    
                    row = cursor.fetchone()
                    deleted_count = row['count'] if isinstance(row, dict) else row[0]
                    
                    if deleted_count == 0:
                        break
                        
                    logger.info(f"Purger: Deleted batch of {deleted_count} URLs with refused extensions")
                    conn.commit()
                    time.sleep(0.1)

            # 2. Purge URLs from disabled hosts ('dns')
            while True:
                cursor.execute("""
                    WITH deleted AS (
                        DELETE FROM urls 
                        WHERE id IN (
                            SELECT id FROM urls 
                            WHERE host IN (SELECT host FROM hosts WHERE disabled = TRUE AND disabled_reason = 'dns')
                            LIMIT 500
                        )
                        RETURNING id
                    )
                    SELECT count(*) FROM deleted
                """)
                row = cursor.fetchone()
                deleted_count = row['count'] if isinstance(row, dict) else row[0]
                
                if deleted_count == 0:
                    break
                logger.info(f"Purger: Deleted batch of {deleted_count} URLs from 'dns' disabled hosts")
                conn.commit()
                time.sleep(0.1)

            # 3. Purge users hosts disabled for 'dns'
            cursor.execute("""
                DELETE FROM hosts 
                WHERE disabled = TRUE AND disabled_reason = 'dns'
            """)
            if cursor.rowcount > 0:
                logger.info(f"Purger: Deleted {cursor.rowcount} 'dns' disabled hosts")
                conn.commit()
                time.sleep(0.1)

            # 3b. Re-enable hosts that were disabled due to 'timeout' after 24 hours
            # Postgres timestamp math
            cursor.execute("""
                UPDATE hosts 
                SET disabled = FALSE, disabled_reason = NULL 
                WHERE disabled = TRUE AND disabled_reason = 'timeout' 
                AND disabled_at <= NOW() - INTERVAL '24 hours'
            """)
            if cursor.rowcount > 0:
                logger.info(f"Purger: Re-enabled {cursor.rowcount} timed-out hosts for retry")
                conn.commit()
                time.sleep(0.1)

            # 4. Erase document content for parsed URLs without tunes in small batches
            while True:
                # Use subquery with limit
                cursor.execute("""
                    UPDATE urls 
                    SET document = 'erased', size_bytes = 0 
                    WHERE id IN (
                        SELECT id FROM urls 
                        WHERE status = 'parsed' AND has_abc = FALSE AND (document IS NULL OR document != 'erased')
                        LIMIT 200
                    )
                """)
                if cursor.rowcount == 0:
                    break
                logger.info(f"Purger: Erased document content for batch of {cursor.rowcount} non-ABC parsed URLs")
                conn.commit()
                time.sleep(0.2)

        except Exception as e:
            logger.error(f"Purger error: {e}")
            conn.rollback()
        finally:
            conn.close()

    def run(self):
        logger.info("URL Purger started (PostgreSQL), running every 60 seconds...")
        while self.running:
            self.purge()
            for _ in range(60):
                if not self.running:
                    break
                time.sleep(1)

if __name__ == '__main__':
    purger = URLPurger()
    purger.run()
