import socket
import json
import time
import signal
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import numpy as np
from database_pg import get_db_connection
from vector_index import VectorIndex

# Dispatcher configuration
DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

# Pitch normalization parameters
MAX_INTERVAL = 12
VECTOR_LEN = 32

# Logging configuration
logger = logging.getLogger('abc_indexer_pg')
logger.setLevel(logging.INFO)

def normalize_intervals(intervals, length=None):
    if length is not None:
        v = np.zeros(length, dtype=np.float32)
        n = min(len(intervals), length)
        for i in range(n):
            v[i] = np.clip(intervals[i], -MAX_INTERVAL, MAX_INTERVAL)
        return v
    return [np.clip(val, -MAX_INTERVAL, MAX_INTERVAL) for val in intervals]

def calculate_intervals(pitches_str, allow_repeats=False):
    """
    Convert separate string of pitches to normalized intervals.
    NOTE: pitches_str coming from database might be a list representation if not handled carefully,
    but in legacy it was a string. Checks needed.
    """
    if not pitches_str:
        return []
    
    # Check if pitches is already a list (if adapter handled it? No, pitches is usually TEXT in tunes)
    # In SQLite it was TEXT. In PostgreSQL migration, we likely kept it as TEXT or convert to array?
    # Schema check: tunes.pitches is probably TEXT or INTEGER[]?
    # Let's assume it's TEXT to match legacy behavior, or check schema.
    # Looking at schema from previous context: tunes.pitches might be integer array?
    # Actually, in SQLite it was CSV string. In PG migration we might have made it int[]?
    # In 'tunes.sql' for PG:
    # "pitches" TEXT
    # So it is still text.
    
    try:
        if isinstance(pitches_str, list):
             pitches = pitches_str
        else:
             pitches = [int(p.strip()) for p in pitches_str.split(',') if p.strip()]
        
        if not pitches:
            return []
            
        if not allow_repeats:
            filtered = []
            if pitches:
                filtered.append(pitches[0])
                for i in range(1, len(pitches)):
                    if pitches[i] != pitches[i-1]:
                        filtered.append(pitches[i])
            pitches = filtered
            
        if len(pitches) < 2:
            return []
        
        intervals = [pitches[i+1] - pitches[i] for i in range(len(pitches) - 1)]
        normalized = normalize_intervals(intervals)
        
        # Return as list of floats
        return [float(x) for x in normalized]
    except (ValueError, AttributeError) as e:
        logger.error(f"Error calculating intervals: {e}")
        return []

class ABCIndexer:
    def __init__(self, indexer_id):
        self.indexer_id = indexer_id
        self.setup_logging()
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.vector_index = VectorIndex()
        logger.info(f"Indexer {self.indexer_id} started (PostgreSQL)")

    def setup_logging(self):
        log_dir = Path(__file__).resolve().parent / 'logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = log_dir / f'indexer.{self.indexer_id}.log'
        
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        logger.handlers = [] # clear self handlers
        
        # 3 MB
        fh = RotatingFileHandler(log_file, maxBytes=3145728, backupCount=4)
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s'))
        root_logger.addHandler(fh)
        
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s'))
        root_logger.addHandler(sh)
        
        logger.info(f"Logging initialized for indexer {self.indexer_id}")

    def signal_handler(self, sig, frame):
        logger.info(f"Indexer {self.indexer_id} shutting down...")
        self.running = False
        sys.exit(0)

    def process_tunebook(self, tunebook_id):
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            
            # Get all tunes
            cursor.execute('''
                SELECT id, pitches 
                FROM tunes 
                WHERE tunebook_id = %s AND status = 'parsed'
            ''', (tunebook_id,))
            
            tunes = cursor.fetchall()
            processed_count = 0
            
            all_vectors = []
            all_tune_ids = []

            for row in tunes:
                # RealDictCursor
                tune_id = row['id']
                pitches = row['pitches']
                
                intervals_list = calculate_intervals(pitches)
                
                # Update intervals column (even if empty) to mark as processed
                final_intervals = intervals_list if intervals_list else []
                cursor.execute('''
                    UPDATE tunes 
                    SET intervals = %s 
                    WHERE id = %s
                ''', (final_intervals, tune_id))
                
                if intervals_list:
                    # Generate windows for FAISS
                    windows = self.vector_index.generate_windows(intervals_list)
                    
                    for window in windows:
                        all_vectors.append(window)
                        all_tune_ids.append(tune_id)

                processed_count += 1
            
            # Batch add to FAISS index
            if all_vectors:
                try:
                    vectors_array = np.array(all_vectors, dtype=np.float32)
                    self.vector_index.add_vectors(all_tune_ids, vectors_array, external_conn=conn)
                    logger.info(f"Added {len(all_vectors)} vectors for {processed_count} tunes to FAISS")
                except Exception as e:
                    logger.error(f"Error adding vectors to FAISS: {e}")
                    raise

            conn.commit()
            logger.info(f"Indexer {self.indexer_id} processed tunebook {tunebook_id}: {processed_count} tunes")
            return True
            
        except Exception as e:
            logger.error(f"Indexer {self.indexer_id} error processing tunebook {tunebook_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def _send_to_dispatcher(self, request):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            chunks = []
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                chunks.append(chunk)
                try:
                    data = b"".join(chunks).decode('utf-8')
                    return json.loads(data)
                except json.JSONDecodeError:
                    continue
            return None
        finally:
            if sock:
                sock.close()

    def communicate_with_dispatcher(self):
        try:
            # 1. Request
            response = self._send_to_dispatcher({'action': 'get_tunebook'})
            
            if not response:
                logger.error(f"Indexer {self.indexer_id} received empty response")
                time.sleep(2)
                return

            if response['status'] == 'ok':
                tunebook_id = response['tunebook_id']
                logger.info(f"Indexer {self.indexer_id} processing tunebook: {tunebook_id}")
                
                success = self.process_tunebook(tunebook_id)
                
                result = {
                    'action': 'submit_indexed_result',
                    'tunebook_id': tunebook_id,
                    'success': success
                }
                ack = self._send_to_dispatcher(result)
                
                if ack and ack.get('status') == 'ok':
                    logger.info(f"Indexer {self.indexer_id} completed tunebook {tunebook_id}")
                else:
                    logger.error(f"Indexer {self.indexer_id} error submitting result: {ack}")
            
            elif response['status'] == 'empty':
                if getattr(self, '_last_empty_log', 0) < time.time() - 300:
                    logger.info(f"Indexer {self.indexer_id} idle")
                    self._last_empty_log = time.time()
                time.sleep(10)
            else:
                logger.error(f"Indexer {self.indexer_id} error: {response.get('message')}")
                time.sleep(5)
            
        except ConnectionRefusedError:
            logger.error(f"Indexer {self.indexer_id}: Cannot connect to dispatcher")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Indexer {self.indexer_id} error: {e}")
            time.sleep(2)

    def run(self):
        logger.info(f"Indexer {self.indexer_id} starting main loop")
        self._last_empty_log = 0
        while self.running:
            try:
                self.communicate_with_dispatcher()
            except Exception as e:
                logger.error(f"Indexer {self.indexer_id} critical loop error: {e}")
                time.sleep(10)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python abc_indexer_pg.py <indexer_id>")
        sys.exit(1)
    
    indexer_id = sys.argv[1]
    indexer = ABCIndexer(indexer_id)
    indexer.run()
