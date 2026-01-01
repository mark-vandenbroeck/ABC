import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
from dtaidistance import dtw
from vector_index import VectorIndex
from database import get_db_connection

# Replicate rerank_with_dtw from abc_app.py
def rerank_with_dtw(query_intervals, candidates, database_intervals):
    print("Starting DTW reranking...")
    scored = []
    for tune_id in candidates:
        if tune_id not in database_intervals:
            print(f"Tune {tune_id} not in DB intervals")
            continue
        candidate_intervals = database_intervals[tune_id]
        
        try:
            # dtaidistance requires numpy arrays
            # window=5 used in app
            d = dtw.distance(
                np.array(query_intervals, dtype=np.float64),
                np.array(candidate_intervals, dtype=np.float64),
                window=10  # Increased window just to be safe, checking if this is the issue
            )
            print(f"DTW Tune {tune_id}: {d}")
            scored.append((tune_id, d))
        except Exception as e:
            print(f"DTW error for tune {tune_id}: {e}")
            continue
            
    return sorted(scored, key=lambda x: x[1])

def debug_full_stack():
    tune_id_1 = 67557
    tune_id_2 = 67580
    
    print(f"Debugging full stack similarity for {tune_id_1} -> expect {tune_id_2}")
    
    # 1. Init Index
    v_index = VectorIndex()
    print(f"Index loaded. Total vectors: {v_index.index.ntotal}")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 2. Get Query Intervals
    cursor.execute('SELECT intervals FROM tunes WHERE id = ?', (tune_id_1,))
    row = cursor.fetchone()
    if not row:
        print("Query tune not found")
        return
    query_intervals = [float(x) for x in row[0].split(',') if x.strip()]
    print(f"Query intervals length: {len(query_intervals)}")

    # 3. Get Candidates (FAISS)
    print("Getting FAISS candidates...")
    faiss_candidates = v_index.get_candidates(query_intervals, k=100, exclude_id=tune_id_1)
    
    candidate_ids = [r['tune_id'] for r in faiss_candidates]
    print(f"Found {len(candidate_ids)} candidates.")
    if tune_id_2 in candidate_ids:
        print(f"Target tune {tune_id_2} IS in FAISS candidates.")
    else:
        print(f"Target tune {tune_id_2} IS NOT in FAISS candidates!")
        return

    # 4. Fetch Candidate Intervals
    placeholders = ', '.join(['?'] * len(candidate_ids))
    cursor.execute(f'''
        SELECT id, intervals 
        FROM tunes 
        WHERE id IN ({placeholders})
    ''', candidate_ids)
    
    candidate_rows = cursor.fetchall()
    db_intervals = {}
    for r in candidate_rows:
        if r[1]:
            db_intervals[r[0]] = [float(x) for x in r[1].split(',') if x.strip()]
            
    conn.close()
    
    # 5. Rerank
    reranked = rerank_with_dtw(query_intervals, candidate_ids, db_intervals)
    
    print("\nTop 10 Reranked:")
    found = False
    for i, (tid, dist) in enumerate(reranked[:10]):
        print(f"{i+1}. Tune {tid} - Dist: {dist}")
        if tid == tune_id_2:
            found = True
    
    if found:
        print("\nSUCCESS: Target tune found in top 10 after DTW.")
    else:
        print("\nFAILURE: Target tune NOT in top 10 after DTW.")

if __name__ == "__main__":
    debug_full_stack()
