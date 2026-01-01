import logging
import numpy as np
from database import get_db_connection
from vector_index import VectorIndex

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rebuild_index')

def rebuild_index():
    logger.info("Starting FAISS index rebuild...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Fetch all tunes with valid intervals
    logger.info("Fetching intervals from database...")
    cursor.execute('SELECT id, intervals FROM tunes WHERE intervals != "" AND intervals IS NOT NULL')
    rows = cursor.fetchall()
    
    if not rows:
        logger.warning("No tunes with intervals found in database.")
        return

    tune_ids = []
    vectors = []
    
    for rid, intervals_str in rows:
        try:
            # Parse intervals string "1.0, 2.0, ..."
            vals = [float(x.strip()) for x in intervals_str.split(',') if x.strip()]
            
            # Ensure fixed length (32) padding or trimming
            # But the stored intervals should already be normalized by abc_indexer?
            # actually abc_indexer.calculate_intervals calls normalize_intervals which returns a fixed length vector string.
            # So we just need to load it.
            
            if len(vals) != 32:
                # Fallback normalization just in case
                v = np.zeros(32, dtype=np.float32)
                n = min(len(vals), 32)
                v[:n] = vals[:n]
                vectors.append(v)
            else:
                vectors.append(np.array(vals, dtype=np.float32))
                
            tune_ids.append(rid)
            
        except ValueError as e:
            logger.warning(f"Error parsing intervals for tune {rid}: {e}")
            
    conn.close()
    
    logger.info(f"Prepared {len(vectors)} vectors.")
    
    if not vectors:
        return

    # 2. Recreate index
    # We force a new index by deleting the old one? 
    # Or VectorIndex(..., mode='w')? default loads existing.
    # checking VectorIndex code: if path exists, it loads.
    # We want to overwrite or add? Since index size is 0 (or stale), simplest is to delete file first.
    
    import os
    if os.path.exists("data/tunes.index"):
        logger.info("Removing stale index file.")
        os.remove("data/tunes.index")
        
    # Also need to clear faiss_mapping table!
    conn = get_db_connection()
    conn.execute('DELETE FROM faiss_mapping')
    conn.commit()
    conn.close()
    logger.info("Cleared faiss_mapping table.")

    # 3. Add vectors
    idx = VectorIndex() # Will create new because file deleted
    
    # Convert list of arrays to 2D array
    vectors_array = np.array(vectors)
    
    idx.add_vectors(tune_ids, vectors_array)
    
    logger.info("Index rebuild complete.")

if __name__ == "__main__":
    rebuild_index()
