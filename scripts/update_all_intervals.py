import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from abc_indexer import calculate_intervals
from database import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_all_intervals():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    logger.info("Fetching tunes with pitches...")
    # Fetch only ID and pitches to minimize memory usage
    cursor.execute('SELECT id, pitches FROM tunes WHERE pitches IS NOT NULL AND pitches != ""')
    all_rows = cursor.fetchall()
    
    logger.info(f"Found {len(all_rows)} tunes. processing...")
    
    batch_size = 1000
    updates = []
    total_processed = 0
    
    for tune_id, pitches_str in all_rows:
        try:
            # Recalculate intervals (now without truncation)
            intervals_str = calculate_intervals(pitches_str)
            updates.append((intervals_str, tune_id))
        except Exception as e:
            logger.error(f"Error for tune {tune_id}: {e}")
        
        if len(updates) >= batch_size:
            cursor.executemany('UPDATE tunes SET intervals = ? WHERE id = ?', updates)
            conn.commit()
            total_processed += len(updates)
            updates = []
            logger.info(f"Updated {total_processed} tunes...")
    
    if updates:
        cursor.executemany('UPDATE tunes SET intervals = ? WHERE id = ?', updates)
        conn.commit()
        total_processed += len(updates)
            
    conn.close()
    logger.info(f"Finished. Total updated: {total_processed}")

if __name__ == "__main__":
    update_all_intervals()
