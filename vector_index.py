import faiss
import numpy as np
import os
import logging
from database import get_db_connection

logger = logging.getLogger('abc_indexer')

class VectorIndex:
    def __init__(self, index_path="data/tunes.index", dimension=32):
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
                # HNSWFlat: Hierarchical Navigable Small World
                # M=32 is the number of neighbors per node
                self.index = faiss.IndexHNSWFlat(self.dimension, 32)
                self.index.hnsw.efConstruction = 40
                logger.info("Created new FAISS HNSW index")
        except Exception as e:
            logger.error(f"Error loading/creating FAISS index: {e}")
            # Fallback to new index
            self.index = faiss.IndexHNSWFlat(self.dimension, 32)

    def save(self):
        try:
            faiss.write_index(self.index, self.index_path)
            # logger.info(f"Saved FAISS index to {self.index_path}")
        except Exception as e:
            logger.error(f"Error saving FAISS index: {e}")

    def add_vectors(self, tune_ids, vectors):
        """
        Add multiple vectors and their corresponding tune_ids
        vectors: numpy array of shape (N, dimension), float32
        tune_ids: list of N tune IDs
        """
        if len(tune_ids) == 0:
            return

        try:
            start_count = self.index.ntotal
            # FAISS requires float32
            self.index.add(vectors.astype('float32'))
            end_count = self.index.ntotal
            
            # Update mapping in SQLite
            conn = get_db_connection()
            cursor = conn.cursor()
            
            mapping_data = []
            for i, tune_id in enumerate(tune_ids):
                # FAISS adds vectors sequentially, so internal ID is start_count + i
                faiss_id = start_count + i
                mapping_data.append((faiss_id, tune_id))
            
            cursor.executemany('INSERT OR REPLACE INTO faiss_mapping (faiss_id, tune_id) VALUES (?, ?)', 
                             mapping_data)
            
            conn.commit()
            conn.close()
            
            self.save()
            logger.info(f"Added {len(tune_ids)} vectors to FAISS index (Total: {end_count})")
        except Exception as e:
            logger.error(f"Error adding vectors to FAISS: {e}")

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
            
            # efSearch controls speed/accuracy at search time
            self.index.hnsw.efSearch = 64
            
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
