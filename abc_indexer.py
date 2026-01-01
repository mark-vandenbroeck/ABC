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

def normalize_intervals(intervals, length=VECTOR_LEN):
    """
    Clip intervals and convert to fixed-length vector
    """
    v = np.zeros(length, dtype=np.float32)
    n = min(len(intervals), length)

    for i in range(n):
        v[i] = np.clip(intervals[i], -MAX_INTERVAL, MAX_INTERVAL)

    return v

class ABCIndexer:
    def __init__(self, indexer_id):
        self.indexer_id = indexer_id
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        logger.info(f"Indexer {self.indexer_id} started (PID: {os.getpid()})")

    def signal_handler(self, sig, frame):
        logger.info(f"Indexer {self.indexer_id} shutting down...")
        self.running = False
        sys.exit(0)

    def calculate_intervals(self, pitches_str):
        """
        Convert pitches to normalized intervals.
        Pitches are separated by comma, optionally followed by a space.
        Example: "60, 62, 64, 62" → "2.0, 2.0, -2.0, 0.0, ..."
        """
        if not pitches_str or not pitches_str.strip():
            return ""
        
        try:
            # Split by comma (with optional space after)
            pitches = [int(p.strip()) for p in pitches_str.split(',') if p.strip()]
            
            if not pitches:
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

    def process_tunebook(self, tunebook_id):
        """Process a tunebook by calculating intervals for all its tunes"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tunes for this tunebook
            cursor.execute('''
                SELECT id, pitches 
                FROM tunes 
                WHERE tunebook_id = ?
            ''', (tunebook_id,))
            
            tunes = cursor.fetchall()
            processed_count = 0
            
            for tune_id, pitches in tunes:
                intervals = self.calculate_intervals(pitches)
                
                # Update the intervals column
                cursor.execute('''
                    UPDATE tunes 
                    SET intervals = ? 
                    WHERE id = ?
                ''', (intervals, tune_id))
                
                processed_count += 1
            
            conn.commit()
            logger.info(f"Indexer {self.indexer_id} processed tunebook {tunebook_id}: {processed_count} tunes")
            return True
            
        except Exception as e:
            logger.error(f"Indexer {self.indexer_id} error processing tunebook {tunebook_id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def communicate_with_dispatcher(self):
        """Communicate with dispatcher to get tunebooks and submit results"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            
            # Request a tunebook
            request = {'action': 'get_tunebook'}
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive response
            response_data = sock.recv(4096).decode('utf-8')
            response = json.loads(response_data)
            
            if response['status'] == 'ok':
                tunebook_id = response['tunebook_id']
                
                print(f"Indexer {self.indexer_id} processing tunebook: {tunebook_id}")
                
                # Process the tunebook
                success = self.process_tunebook(tunebook_id)
                
                # Report result back to dispatcher
                result = {
                    'action': 'submit_indexed_result',
                    'tunebook_id': tunebook_id,
                    'success': success
                }
                sock.send(json.dumps(result).encode('utf-8'))
                
                # Wait for acknowledgment
                ack_data = sock.recv(1024).decode('utf-8')
                ack = json.loads(ack_data)
                
                if ack['status'] == 'ok':
                    print(f"Indexer {self.indexer_id} completed tunebook {tunebook_id}")
                else:
                    logger.error(f"Indexer {self.indexer_id} error submitting result: {ack.get('message')}")
            
            elif response['status'] == 'empty':
                # No tunebooks available, wait before retrying
                time.sleep(2)
            else:
                logger.error(f"Indexer {self.indexer_id} error: {response.get('message')}")
                time.sleep(2)
            
            sock.close()
            
        except ConnectionRefusedError:
            logger.error(f"Indexer {self.indexer_id}: Cannot connect to dispatcher")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Indexer {self.indexer_id} error: {e}")
            time.sleep(2)

    def run(self):
        """Main loop"""
        logger.info(f"Indexer {self.indexer_id} starting main loop")
        
        while self.running:
            self.communicate_with_dispatcher()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python abc_indexer.py <indexer_id>")
        sys.exit(1)
    
    indexer_id = sys.argv[1]
    indexer = ABCIndexer(indexer_id)
    indexer.run()
