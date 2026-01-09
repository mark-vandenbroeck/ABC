import sys
import os
import numpy as np
import faiss
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vector_index import VectorIndex
from database import get_db_connection

def debug_faiss():
    idx = VectorIndex()
    print(f"Index size: {idx.index.ntotal}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get intervals for tune 67557 and 67580
    cursor.execute('SELECT id, intervals FROM tunes WHERE id IN (67557, 67580)')
    rows = cursor.fetchall()
    conn.close()
    
    data = {}
    for tid, intervals_str in rows:
        data[tid] = [float(x.strip()) for x in intervals_str.split(',') if x.strip()]
        print(f"Tune {tid} has {len(data[tid])} intervals.")

    # Check for literal equality
    if data[67557] == data[67580]:
        print("Intervals are literally equal.")
    else:
        print("Intervalls are NOT equal.")
        # Find first difference
        for i in range(min(len(data[67557]), len(data[67580]))):
            if data[67557][i] != data[67580][i]:
                print(f"First diff at index {i}: {data[67557][i]} vs {data[67580][i]}")
                break

    # Generate windows
    w1 = VectorIndex.generate_windows(data[67557], 32, 8)
    w2 = VectorIndex.generate_windows(data[67580], 32, 8)
    
    print(f"Tune 67557 has {len(w1)} windows.")
    print(f"Tune 67580 has {len(w2)} windows.")
    
    # Check if first window of 67557 is in FAISS
    q_vec = w1[0].reshape(1, -1).astype('float32')
    distances, indices = idx.index.search(q_vec, 100)
    
    print("\nSearch results for 67557 first window:")
    conn = get_db_connection()
    cursor = conn.cursor()
    for d, i in zip(distances[0], indices[0]):
        if i == -1: continue
        cursor.execute('SELECT tune_id FROM faiss_mapping WHERE faiss_id = ?', (int(i),))
        row = cursor.fetchone()
        tid = row[0] if row else "UNKNOWN"
        if tid in [67557, 67580] or d < 1.0:
            print(f"  Tune: {tid}, FAISS ID: {i}, Dist: {d}")
    conn.close()

if __name__ == "__main__":
    debug_faiss()
