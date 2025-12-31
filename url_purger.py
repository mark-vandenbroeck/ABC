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
    # 10 MB = 10485760 bytes
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10485760, backupCount=4)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)
logger.setLevel(logging.INFO)

class URLPurger:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

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
            # We join urls with refused_extensions based on the extension part of the URL
            # This is a bit complex in SQL because we need to extract the extension.
            # For simplicity and correctness, we'll fetch refused extensions and do it in batches or filtered queries.
            
            cursor.execute('SELECT extension FROM refused_extensions')
            refused_exts = [row[0] for row in cursor.fetchall()]
            
            for ext in refused_exts:
                # Using a broad match to ensure we catch the extension anywhere it appears as a suffix
                # This covers .ext, .ext/, .ext?query, .ext#fragment etc.
                pattern = f'%.{ext}%'
                cursor.execute('DELETE FROM urls WHERE url LIKE ?', (pattern,))
                if cursor.rowcount > 0:
                    logger.info(f"Purger: Deleted {cursor.rowcount} URLs containing .{ext}")

            # 2. Verwijder alle URLs waarvan de host gedisabled is met reden 'dns'
            cursor.execute('''
                DELETE FROM urls 
                WHERE host IN (SELECT host FROM hosts WHERE disabled = 1 AND disabled_reason = 'dns')
            ''')
            if cursor.rowcount > 0:
                logger.info(f"Purger: Deleted {cursor.rowcount} URLs from 'dns' disabled hosts")

            # 3. Verwijder alle hosts die gedisabled zijn met reden 'dns'
            cursor.execute('''
                DELETE FROM hosts 
                WHERE disabled = 1 AND disabled_reason = 'dns'
            ''')
            if cursor.rowcount > 0:
                logger.info(f"Purger: Deleted {cursor.rowcount} 'dns' disabled hosts")

            # 4. Erase document content for parsed URLs without tunes
            cursor.execute('''
                UPDATE urls 
                SET document = 'erased', size_bytes = 0 
                WHERE status = 'parsed' AND has_abc = 0 AND document != 'erased'
            ''')
            if cursor.rowcount > 0:
                logger.info(f"Purger: Erased document content for {cursor.rowcount} non-ABC parsed URLs")

            conn.commit()
        except Exception as e:
            logger.error(f"Purger error: {e}")
            conn.rollback()
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
