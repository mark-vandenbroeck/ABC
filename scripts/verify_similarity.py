import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vector_index import VectorIndex
from database import get_db_connection

def verify():
    # Load index
    idx = VectorIndex()
    
    tune_id_1 = 67557
    tune_id_2 = 67580
    
    print(f"Index size: {idx.index.ntotal}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get intervals for tune 1
    cursor.execute('SELECT intervals FROM tunes WHERE id = ?', (tune_id_1,))
    row = cursor.fetchone()
    if not row:
        print("Tune 1 not found")
        return
        
    intervals_str = row[0]
    vals = [float(x.strip()) for x in intervals_str.split(',') if x.strip()]
    print(f"Tune 1 intervals length: {len(vals)}")
    print(f"Tune 1 intervals (start): {vals[:10]}")
    
    # Get candidates
    print(f"Searching for candidates for tune {tune_id_1}...")
    candidates = idx.get_candidates(vals, k=20, exclude_id=None)
    
    found = False
    for c in candidates:
        print(f"Candidate: {c['tune_id']}, Dist: {c['distance']}")
        if c['tune_id'] == tune_id_2:
            found = True
            print(f"-> FOUND MATCHING TUNE {tune_id_2}! Distance: {c['distance']}")
            
    if not found:
        print(f"-> FAILURE: Tune {tune_id_2} NOT found in top 20.")
    else:
        print("SUCCESS")

if __name__ == "__main__":
    verify()
