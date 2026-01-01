import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3
import logging
from database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def remove_duplicates():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    logger.info("Finding duplicates...")
    # Find duplicates based on the combination of tunebook_id, reference_number, and title.
    # Group by these fields, having count > 1.
    cursor.execute('''
        SELECT tunebook_id, reference_number, title, COUNT(*) as c, MAX(id) as max_id
        FROM tunes 
        GROUP BY tunebook_id, reference_number, title 
        HAVING c > 1
    ''')
    
    rows = cursor.fetchall()
    logger.info(f"Found {len(rows)} groups of duplicates.")
    
    deleted_count = 0
    for row in rows:
        tb_id = row[0]
        ref_num = row[1]
        title = row[2]
        keep_id = row[4]
        
        # Delete all records for this group EXCEPT the one we want to keep
        if title is None:
             cursor.execute('''
                DELETE FROM tunes 
                WHERE tunebook_id = ? AND reference_number = ? AND title IS NULL AND id != ?
            ''', (tb_id, ref_num, keep_id))           
        else:
            cursor.execute('''
                DELETE FROM tunes 
                WHERE tunebook_id = ? AND reference_number = ? AND title = ? AND id != ?
            ''', (tb_id, ref_num, title, keep_id))
            
        deleted_count += cursor.rowcount
        
    conn.commit()
    logger.info(f"Deleted {deleted_count} duplicate tunes.")
    conn.close()

if __name__ == "__main__":
    remove_duplicates()
