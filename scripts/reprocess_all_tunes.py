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

def reprocess_all():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Select ALL tunes
    logger.info("Fetching all tunes for reprocessing...")
    cursor.execute('SELECT id, tune_body FROM tunes WHERE tune_body IS NOT NULL AND tune_body != ""')
    
    tunes = cursor.fetchall()
    total = len(tunes)
    logger.info(f"Found {total} tunes to reprocess.")
    
    count = 0
    success = 0
    
    for tune_id, raw_body in tunes:
        try:
            # Re-parse to get clean pitches (no chords/headers)
            # raw_body in DB is just the body. Tune class expects full ABC or just body works if K: assumed
            # But pitches calculation is better if it's a full ABC string. 
            # However, Tune class handles partial strings too.
            tune = Tune(raw_body)
            
            if tune.pitches:
                pitches_str = ",".join(map(str, tune.pitches))
                
                # Calculate NEW intervals (no repeats)
                # allow_repeats=False is now the default in my updated abc_indexer.py
                intervals_str = calculate_intervals(pitches_str)
                
                cursor.execute('''
                    UPDATE tunes 
                    SET pitches = ?, intervals = ?
                    WHERE id = ?
                ''', (pitches_str, intervals_str, tune_id))
                success += 1
            
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
    reprocess_all()
