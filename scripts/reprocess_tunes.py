import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3
import logging
from abc_parser import Tune
from abc_indexer import calculate_intervals
from database import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def reprocess_missing_pitches():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Select tunes that have no pitches
    # These were likely crawled before the robust fallback parser was added
    logger.info("Fetching tunes with missing pitches...")
    cursor.execute('''
        SELECT id, tune_body 
        FROM tunes 
        WHERE (pitches IS NULL OR pitches = '') AND (tune_body IS NOT NULL AND tune_body != '')
    ''')
    
    tunes_to_fix = cursor.fetchall()
    total = len(tunes_to_fix)
    logger.info(f"Found {total} tunes to reprocess.")
    
    count = 0
    success = 0
    
    for tune_id, raw_data in tunes_to_fix:
        try:
            # Parse the tune again using the updated Tune class
            # This class now includes the _extract_pitches_from_elements fallback
            tune = Tune(raw_data)
            
            # pitches is a list of ints. Convert to string "60,62,..."
            if tune.pitches:
                pitches_str = ",".join(map(str, tune.pitches))
                
                # Calculate intervals immediately
                intervals_str = calculate_intervals(pitches_str)
                
                # Update DB
                cursor.execute('''
                    UPDATE tunes 
                    SET pitches = ?, intervals = ?
                    WHERE id = ?
                ''', (pitches_str, intervals_str, tune_id))
                
                success += 1
            else:
                 # Even with fallback, some might fail (e.g. empty body)
                 pass
                 
        except Exception as e:
            logger.error(f"Error reprocessing tune {tune_id}: {e}")
            
        count += 1
        if count % 1000 == 0:
            conn.commit()
            logger.info(f"Processed {count}/{total} tunes. Success: {success}")
            
    conn.commit()
    conn.close()
    logger.info(f"Finished. Successfully updated {success}/{total} tunes.")

if __name__ == "__main__":
    reprocess_missing_pitches()
