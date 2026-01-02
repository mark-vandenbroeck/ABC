import faiss
import numpy as np
import os
import logging
from database import get_db_connection

logger = logging.getLogger('abc_indexer')

class VectorIndex:
    def __init__(self, index_path="data/tunes.index", dimension=16):
        self.index_path = index_path
        self.dimension = dimension
        self.index = None
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        
        self._load_or_create()

    def _load_or_create(self):
        try:
            if os.path.exists(self.index_path):
                self.index = faiss.read_index(self.index_path)
                logger.info(f"Loaded FAISS index from {self.index_path} ({self.index.ntotal} vectors)")
            else:
                self.index = faiss.IndexFlatL2(self.dimension)
                logger.info("Created new FAISS FlatL2 index")
        except Exception as e:
            logger.error(f"Error loading/creating FAISS index: {e}")
            # Fallback to new index
            self.index = faiss.IndexFlatL2(self.dimension)

    def save(self):
        try:
            faiss.write_index(self.index, self.index_path)
            # logger.info(f"Saved FAISS index to {self.index_path}")
        except Exception as e:
            logger.error(f"Error saving FAISS index: {e}")

    def add_vectors(self, tune_ids, vectors, external_conn=None):
        """
        Add multiple vectors and their corresponding tune_ids
        vectors: numpy array of shape (N, dimension), float32
        tune_ids: list of N tune IDs
        external_conn: optional active sqlite3 connection for atomic updates
        """
        if len(tune_ids) == 0:
            return

        conn = external_conn
        close_conn = False
        try:
            start_count = self.index.ntotal
            
            # 1. Update mapping in SQLite FIRST (within transaction)
            if conn is None:
                conn = get_db_connection()
                close_conn = True
            
            cursor = conn.cursor()
            
            mapping_data = []
            for i, tune_id in enumerate(tune_ids):
                # FAISS adds vectors sequentially, so internal ID is start_count + i
                faiss_id = start_count + i
                mapping_data.append((faiss_id, tune_id))
            
            cursor.executemany('INSERT OR REPLACE INTO faiss_mapping (faiss_id, tune_id) VALUES (?, ?)', 
                             mapping_data)
            
            # If we opened the connection here, commit it. 
            # If external_conn was provided, the caller handles commit.
            if close_conn:
                conn.commit()
            
            # 2. Add to FAISS index ONLY if DB update succeeded
            self.index.add(vectors.astype('float32'))
            end_count = self.index.ntotal
            
            # 3. Persistence
            self.save()
            logger.info(f"Atomic update: added {len(tune_ids)} vectors to FAISS index (Total: {end_count})")
            
        except Exception as e:
            logger.error(f"Error in atomic add_vectors: {e}")
            if close_conn and conn:
                conn.rollback()
            raise # Re-raise to let caller know the update failed
        finally:
            if close_conn and conn:
                conn.close()

    def search(self, query_vector, k=10):
        """
        Search for nearest neighbors
        query_vector: numpy array of shape (dimension,)
        k: number of results
        Returns list of (tune_id, distance)
        """
        if self.index.ntotal == 0:
            return []

        try:
            # Reshape for search (1, dimension)
            q = query_vector.reshape(1, -1).astype('float32')
            
            distances, indices = self.index.search(q, k)
            
            # Get tune_ids from mapping
            conn = get_db_connection()
            cursor = conn.cursor()
            
            results = []
            for dist, idx in zip(distances[0], indices[0]):
                if idx == -1: continue # No more results
                
                cursor.execute('SELECT tune_id FROM faiss_mapping WHERE faiss_id = ?', (int(idx),))
                row = cursor.fetchone()
                if row:
                    results.append({'tune_id': row[0], 'distance': float(dist)})
            
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Error searching FAISS index: {e}")
            return []

    @staticmethod
    def generate_windows(intervals, window_size=16, stride=4):
        """
        Generate overlapping windows from interval list.
        Returns list of numpy arrays, each of shape (window_size,)
        """
        if not intervals:
            return []
            
        # If shorter than window, pad once and return
        if len(intervals) <= window_size:
            vec = np.zeros(window_size, dtype=np.float32)
            for i, val in enumerate(intervals):
                vec[i] = val
            return [vec]
            
        windows = []
        # Slide window
        for i in range(0, len(intervals) - window_size + 1, stride):
            window = intervals[i : i + window_size]
            vec = np.zeros(window_size, dtype=np.float32)
            for j, val in enumerate(window):
                vec[j] = val
            windows.append(vec)
            
        # Handle the tail if we missed a significant chunk?
        # With stride logic, we might miss the very last few notes if they don't fit a full stride step
        # but the specific overlap usually covers it.
        
        return windows

    def get_candidates(self, query_intervals, k=100, exclude_id=None):
        """
        High-level search that handles window generation for the query
        and deduplication of results.
        """
        # 1. Generate windows for query
        query_vectors = self.generate_windows(query_intervals, self.dimension, stride=4)
        
        # 2. Search for each window
        all_results = []
        for q_vec in query_vectors:
            results = self.search(q_vec, k=k)
            all_results.extend(results)
            
        # 3. Deduplicate and aggregate
        # Strategy: Keep the MINIMUM distance for each tune_id
        best_scores = {}
        for res in all_results:
            tid = res['tune_id']
            dist = res['distance']
            
            if exclude_id and tid == exclude_id:
                continue
                
            if tid not in best_scores or dist < best_scores[tid]:
                best_scores[tid] = dist
                
        # 4. Sort by distance
        sorted_candidates = sorted(best_scores.items(), key=lambda x: x[1])
        
        # Return top K unique tunes
        return [{'tune_id': tid, 'distance': dist} for tid, dist in sorted_candidates[:k]]
