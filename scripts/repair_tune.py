import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3
import logging
from abc_parser import Tune
from database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def repair_tune(tune_id):
    logger.info(f"Repairing tune {tune_id}...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Get raw data
    # Note: 'raw_data' column doesn't exist?
    # tunes table has components.
    # But Tune() expects raw ABC.
    # We can reconstruct it or use 'tune_body' if headers are standard.
    # However, for parsing pitches, Tune needs headers (K field) for key?
    # Actually my fallback logic mainly uses elements.
    # The elements are parsed from BODY.
    # Tune takes raw_data.
    
    # Let's verify what 'entries' we have.
    # We need to construct a valid ABC string.
    cursor.execute('''
        SELECT reference_number, title, composer, rhythm, key, meter, unit_note_length, tempo, tune_body
        FROM tunes WHERE id = ?
    ''', (tune_id,))
    row = cursor.fetchone()
    
    if not row:
        logger.error("Tune not found")
        return

    # Construct ABC
    abc_parts = []
    if row[0]: abc_parts.append(f"X:{row[0]}")
    if row[1]: abc_parts.append(f"T:{row[1]}")
    if row[2]: abc_parts.append(f"C:{row[2]}")
    if row[4]: abc_parts.append(f"K:{row[4]}") # Key
    if row[5]: abc_parts.append(f"M:{row[5]}")
    if row[6]: abc_parts.append(f"L:{row[6]}")
    if row[7]: abc_parts.append(f"Q:{row[7]}")
    
    # Add body
    if row[8]:
        abc_parts.append(row[8])
        
    full_abc = "\n".join(abc_parts)
    
    logger.info("Parsing ABC...")
    tune = Tune(full_abc)
    
    pitches = ",".join(map(str, tune.pitches))
    logger.info(f"Found {len(tune.pitches)} pitches: {pitches[:50]}...")
    
    if tune.pitches:
        logger.info("Updating database...")
        cursor.execute('UPDATE tunes SET pitches = ? WHERE id = ?', (pitches, tune_id))
        conn.commit()
        logger.info("Update complete.")
    else:
        logger.warning("No pitches found, skipping update.")
        
    conn.close()

if __name__ == "__main__":
    repair_tune(51864)
