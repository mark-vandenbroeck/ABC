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
from database import get_db_connection, DB_PATH
from vector_index import VectorIndex

# Dispatcher configuration
DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

# Pitch normalization parameters
MAX_INTERVAL = 12
VECTOR_LEN = 32

# Logging configuration
LOG_FILE = Path(DB_PATH).resolve().parent / 'logs' / 'indexer.log'
logger = logging.getLogger('abc_indexer')
logger.setLevel(logging.INFO)
if not logger.handlers:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    # 3 MB = 3145728 bytes
    fh = RotatingFileHandler(LOG_FILE, maxBytes=3145728, backupCount=4)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)

def normalize_intervals(intervals, length=None):
    """
    Clip intervals to +/- MAX_INTERVAL. 
    If length is None, returns the full sequence.
    If length is specified, truncates/pads to that length (legacy behavior).
    """
    if length is not None:
        v = np.zeros(length, dtype=np.float32)
        n = min(len(intervals), length)
        for i in range(n):
            v[i] = np.clip(intervals[i], -MAX_INTERVAL, MAX_INTERVAL)
        return v
    
    # Return full sequence as list
    return [np.clip(val, -MAX_INTERVAL, MAX_INTERVAL) for val in intervals]

def calculate_intervals(pitches_str, allow_repeats=False):
    """
        Convert pitches to normalized intervals.
        Pitches are separated by comma, optionally followed by a space.
        Example: "60, 62, 64, 62" → "2.0, 2.0, -2.0, 0.0, ..."
        
        If allow_repeats is False, consecutive identical pitches are collapsed,
        effectively removing all 0.0 intervals.
    """
    if not pitches_str or not pitches_str.strip():
        return ""
    
    try:
        # Split by comma (with optional space after)
        pitches = [int(p.strip()) for p in pitches_str.split(',') if p.strip()]
        
        if not pitches:
            return ""
            
        if not allow_repeats:
            # Collapse consecutive identical pitches
            filtered = []
            if pitches:
                filtered.append(pitches[0])
                for i in range(1, len(pitches)):
                    if pitches[i] != pitches[i-1]:
                        filtered.append(pitches[i])
            pitches = filtered
            
        if len(pitches) < 2:
            return ""
        
        # Calculate differences between consecutive pitches
        intervals = [pitches[i+1] - pitches[i] for i in range(len(pitches) - 1)]
        
        # Normalize intervals
        normalized = normalize_intervals(intervals)
        
        # Join with comma-space
        return ", ".join(f"{val:.1f}" if val != 0 else "0" for val in normalized)
    except (ValueError, AttributeError) as e:
        logger.error(f"Error calculating intervals from '{pitches_str}': {e}")
        return ""

class ABCIndexer:
    def __init__(self, indexer_id):
        self.indexer_id = indexer_id
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.vector_index = VectorIndex()
        logger.info(f"Indexer {self.indexer_id} started (PID: {os.getpid()})")

    def signal_handler(self, sig, frame):
        logger.info(f"Indexer {self.indexer_id} shutting down...")
        self.running = False
        sys.exit(0)

    def process_tunebook(self, tunebook_id):
        """Process a tunebook by calculating intervals for all its tunes"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tunes for this tunebook
            cursor.execute('''
                SELECT id, pitches 
                FROM tunes 
                WHERE tunebook_id = ? AND status = 'parsed'
            ''', (tunebook_id,))
            
            tunes = cursor.fetchall()
            processed_count = 0
            
            all_vectors = []
            all_tune_ids = []

            for tune_id, pitches in tunes:
                intervals_str = calculate_intervals(pitches)
                
                # Update the intervals column
                cursor.execute('''
                    UPDATE tunes 
                    SET intervals = ? 
                    WHERE id = ?
                ''', (intervals_str, tune_id))
                
                if intervals_str:
                    # Convert back to list of floats for vector generation
                    intervals = [float(x) for x in intervals_str.split(', ')]
                    
                    # Generate windows
                    windows = self.vector_index.generate_windows(intervals)
                    
                    # Accumulate vectors and tune_ids
                    for window in windows:
                        all_vectors.append(window)
                        all_tune_ids.append(tune_id)

                processed_count += 1
            
            # Batch add to FAISS index
            if all_vectors:
                try:
                    vectors_array = np.array(all_vectors, dtype=np.float32)
                    self.vector_index.add_vectors(all_tune_ids, vectors_array)
                    logger.info(f"Added {len(all_vectors)} vectors for {processed_count} tunes to FAISS")
                except Exception as e:
                    logger.error(f"Error adding vectors to FAISS: {e}")

            conn.commit()
            self.vector_index.save()
            logger.info(f"Indexer {self.indexer_id} processed tunebook {tunebook_id}: {processed_count} tunes")
            return True
            
        except Exception as e:
            logger.error(f"Indexer {self.indexer_id} error processing tunebook {tunebook_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def _send_to_dispatcher(self, request):
        """Helper to send a request to the dispatcher and receive a response"""
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive response (handle possible multiple chunks)
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
        """Communicate with dispatcher to get tunebooks and submit results"""
        try:
            # 1. Request a tunebook
            response = self._send_to_dispatcher({'action': 'get_tunebook'})
            
            if not response:
                logger.error(f"Indexer {self.indexer_id} received empty response from dispatcher")
                time.sleep(2)
                return

            if response['status'] == 'ok':
                tunebook_id = response['tunebook_id']
                logger.info(f"Indexer {self.indexer_id} processing tunebook: {tunebook_id}")
                
                # 2. Process the tunebook (this is local DB work)
                success = self.process_tunebook(tunebook_id)
                
                # 3. Report result back to dispatcher in a new connection
                result = {
                    'action': 'submit_indexed_result',
                    'tunebook_id': tunebook_id,
                    'success': success
                }
                ack = self._send_to_dispatcher(result)
                
                if ack and ack.get('status') == 'ok':
                    logger.info(f"Indexer {self.indexer_id} completed tunebook {tunebook_id}")
                else:
                    logger.error(f"Indexer {self.indexer_id} error submitting result or no ack: {ack}")
            
            elif response['status'] == 'empty':
                # No tunebooks available, wait before retrying.
                # Use a slightly longer wait and log to show we're alive.
                if getattr(self, '_last_empty_log', 0) < time.time() - 300: # Log every 5 mins
                    logger.info(f"Indexer {self.indexer_id} idle (waiting for new tunebooks...)")
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
        """Main loop with persistence logic"""
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
        print("Usage: python abc_indexer.py <indexer_id>")
        sys.exit(1)
    
    indexer_id = sys.argv[1]
    indexer = ABCIndexer(indexer_id)
    indexer.run()
