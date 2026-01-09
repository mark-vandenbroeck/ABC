import sys
import os
import numpy as np
import faiss
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vector_index import VectorIndex
from database import get_db_connection

def debug_harvest_home():
    idx = VectorIndex()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    t1 = 53708
    t2 = 66308
    
    cursor.execute('SELECT id, intervals FROM tunes WHERE id IN (?, ?)', (t1, t2))
    rows = cursor.fetchall()
    conn.close()
    
    data = {r[0]: [float(x.strip()) for x in r[1].split(',') if x.strip()] for r in rows}
    
    if t1 not in data or t2 not in data:
        print("Required tunes not found in DB.")
        return

    print(f"Tune {t1} length: {len(data[t1])}")
    print(f"Tune {t2} length: {len(data[t2])}")

    w1 = VectorIndex.generate_windows(data[t1], 32, 8)
    w2 = VectorIndex.generate_windows(data[t2], 32, 8)

    print(f"Tune {t1} windows: {len(w1)}")
    print(f"Tune {t2} windows: {len(w2)}")

    # Check cross-distance between ALL windows
    min_dist = float('inf')
    best_pair = (0, 0)
    
    for i, vec1 in enumerate(w1):
        for j, vec2 in enumerate(w2):
            dist = np.sum((vec1 - vec2)**2)
            if dist < min_dist:
                min_dist = dist
                best_pair = (i, j)
    
    print(f"Minimum Euclidean distance between any window pair: {min_dist}")
    print(f"Best matching windows: {best_pair}")
    
    # Check if they are candidates
    print(f"\nSearching candidates for {t1} (showing top 20):")
    candidates = idx.get_candidates(data[t1], k=20)
    for c in candidates:
        print(f"  Tune ID: {c['tune_id']}, Distance: {c['distance']}")
        if c['tune_id'] == t2:
            print(f"  *** MATCH FOUND AT DISTANCE {c['distance']} ***")

if __name__ == "__main__":
    debug_harvest_home()
