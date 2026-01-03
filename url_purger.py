import os
import sys
import signal
import time
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from database import get_db_connection, DB_PATH

# Log file in the logs directory
LOG_FILE = Path(DB_PATH).resolve().parent / 'logs' / 'purger.log'
logger = logging.getLogger('url_purger')
logger.setLevel(logging.INFO)
if not logger.handlers:
    os.makedirs(LOG_FILE.parent, exist_ok=True)
    # 3 MB = 3145728 bytes
    fh = RotatingFileHandler(LOG_FILE, maxBytes=3145728, backupCount=4)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)
logger.setLevel(logging.INFO)

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
        cursor = conn.cursor()
        
        try:
            # 1. Verwijder URLs met een filename extension in refused_extensions
            cursor.execute('SELECT extension FROM refused_extensions')
            refused_exts = [row[0] for row in cursor.fetchall()]
            
            if refused_exts:
                # Use the new url_extension column for O(1) matching instead of LIKE scans
                placeholders = ",".join(["?" for _ in refused_exts])
                
                # Delete in batches to avoid locking the entire table
                while True:
                    cursor.execute(f'''
                        DELETE FROM urls 
                        WHERE id IN (
                            SELECT id FROM urls 
                            WHERE url_extension IN ({placeholders})
                            LIMIT 500
                        )
                    ''', refused_exts)
                    
                    if cursor.rowcount == 0:
                        break
                        
                    logger.info(f"Purger: Deleted batch of {cursor.rowcount} URLs with refused extensions")
                    conn.commit()
                    time.sleep(0.1)

            # 2. Verwijder alle URLs waarvan de host gedisabled is met reden 'dns'
            while True:
                cursor.execute('''
                    DELETE FROM urls 
                    WHERE id IN (
                        SELECT id FROM urls 
                        WHERE host IN (SELECT host FROM hosts WHERE disabled = 1 AND disabled_reason = 'dns')
                        LIMIT 500
                    )
                ''')
                if cursor.rowcount == 0:
                    break
                logger.info(f"Purger: Deleted batch of {cursor.rowcount} URLs from 'dns' disabled hosts")
                conn.commit()
                time.sleep(0.1)

            # 3. Verwijder alle hosts die gedisabled zijn met reden 'dns'
            cursor.execute('''
                DELETE FROM hosts 
                WHERE disabled = 1 AND disabled_reason = 'dns'
            ''')
            if cursor.rowcount > 0:
                logger.info(f"Purger: Deleted {cursor.rowcount} 'dns' disabled hosts")
                conn.commit()
                time.sleep(0.1)

            # 3b. Re-enable hosts that were disabled due to 'timeout' after 24 hours
            cursor.execute('''
                UPDATE hosts 
                SET disabled = 0, disabled_reason = NULL 
                WHERE disabled = 1 AND disabled_reason = 'timeout' 
                AND disabled_at <= datetime('now', '-24 hours')
            ''')
            if cursor.rowcount > 0:
                logger.info(f"Purger: Re-enabled {cursor.rowcount} timed-out hosts for retry")
                conn.commit()
                time.sleep(0.1)

            # 4. Erase document content for parsed URLs without tunes in small batches
            while True:
                # Use the optimized idx_urls_purger_cleanup index
                cursor.execute('''
                    UPDATE urls 
                    SET document = 'erased', size_bytes = 0 
                    WHERE id IN (
                        SELECT id FROM urls 
                        WHERE status = 'parsed' AND has_abc = 0 AND document != 'erased'
                        LIMIT 200
                    )
                ''')
                if cursor.rowcount == 0:
                    break
                logger.info(f"Purger: Erased document content for batch of {cursor.rowcount} non-ABC parsed URLs")
                conn.commit()
                time.sleep(0.2) # Allow others access to DB

        except Exception as e:
            logger.error(f"Purger error: {e}")
            try:
                conn.rollback()
            except:
                pass
        finally:
            conn.close()

    def run(self):
        logger.info("URL Purger started, running every 60 seconds...")
        while self.running:
            self.purge()
            # Wait for 1 minute, but check running flag more frequently for faster shutdown
            for _ in range(60):
                if not self.running:
                    break
                time.sleep(1)

if __name__ == '__main__':
    purger = URLPurger()
    purger.run()
