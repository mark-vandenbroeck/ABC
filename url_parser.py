import socket
import json
import time
import signal
import sys
import logging
from abc_parser import Tunebook
from database import get_db_connection

# Dispatcher configuration
DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/parser.log')
    ]
)
logger = logging.getLogger('url_parser')

class URLParser:
    def __init__(self, parser_id):
        self.parser_id = parser_id
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        logger.info(f"Parser {self.parser_id} shutting down...")
        self.running = False
        sys.exit(0)

    def save_tunebook(self, tunebook_data):
        """Save tunebook and its tunes to the database"""
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # 1. Insert into tunebooks
            cursor.execute('''
                INSERT OR IGNORE INTO tunebooks (url, created_at)
                VALUES (?, CURRENT_TIMESTAMP)
            ''', (tunebook_data['url'],))
            
            # Get the tunebook ID
            cursor.execute('SELECT id FROM tunebooks WHERE url = ?', (tunebook_data['url'],))
            tunebook_id = cursor.fetchone()[0]

            # 2. Insert into tunes
            for tune in tunebook_data['tunes']:
                meta = tune['metadata']
                cursor.execute('''
                    INSERT INTO tunes (
                        tunebook_id, reference_number, title, composer, origin, area, 
                        meter, unit_note_length, tempo, parts, transcription, notes, 
                        "group", history, key, rhythm, book, discography, source, 
                        instruction, tune_body, pitches
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tunebook_id,
                    meta.get('reference_number'),
                    meta.get('title', tune['title']),
                    meta.get('composer'),
                    meta.get('origin'),
                    meta.get('area'),
                    meta.get('meter'),
                    meta.get('unit_note_length'),
                    meta.get('tempo'),
                    meta.get('parts'),
                    meta.get('transcription'),
                    meta.get('notes'),
                    meta.get('group'),
                    meta.get('history'),
                    meta.get('key'),
                    meta.get('rhythm'),
                    meta.get('book'),
                    meta.get('discography'),
                    meta.get('source'),
                    meta.get('instruction'),
                    tune['tune_body'],
                    tune['pitches']
                ))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error saving tunebook {tunebook_data['url']}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def process_url(self, url_id, url):
        """Fetch content from DB, parse and save"""
        logger.info(f"Processing URL: {url}")
        try:
            # Tunebook class handles fetching from DB and parsing
            tunebook = Tunebook(url)
            
            if tunebook.success and tunebook.tunes:
                logger.info(f"Found {len(tunebook.tunes)} tunes in {url}")
                save_success = self.save_tunebook(tunebook.to_dict())
                return True, save_success
            else:
                logger.info(f"No valid tunes found in {url}")
                return True, False # Success in processing, but no ABC found
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return False, False

    def communicate_with_dispatcher(self):
        """Communicate with dispatcher to get URLs and submit results"""
        try:
            with socket.create_connection((DISPATCHER_HOST, DISPATCHER_PORT), timeout=5) as sock:
                # 1. Request a URL
                request = {'action': 'get_fetched_url'}
                sock.sendall(json.dumps(request).encode('utf-8'))
                
                response_data = sock.recv(4096).decode('utf-8')
                if not response_data:
                    return
                
                response = json.loads(response_data)
                if response['status'] == 'no_urls':
                    return
                
                if response['status'] == 'ok':
                    url_id = response['url_id']
                    url = response['url']
                    
                    # 2. Process the URL
                    proc_success, has_abc = self.process_url(url_id, url)
                    
                    # 3. Report back to dispatcher
                    # We need a new connection for reporting result because dispatcher closes it after get_fetched_url
                    with socket.create_connection((DISPATCHER_HOST, DISPATCHER_PORT), timeout=5) as sock2:
                        report = {
                            'action': 'submit_parsed_result',
                            'url_id': url_id,
                            'has_abc': has_abc
                        }
                        sock2.sendall(json.dumps(report).encode('utf-8'))
                        sock2.recv(1024) # Ack
        except Exception as e:
            logger.error(f"Communication error: {e}")

    def run(self):
        logger.info(f"URL Parser {self.parser_id} started...")
        while self.running:
            self.communicate_with_dispatcher()
            # Wait a bit if no work was found or after processing
            time.sleep(2)

if __name__ == '__main__':
    parser_id = sys.argv[1] if len(sys.argv) > 1 else '1'
    parser = URLParser(parser_id)
    parser.run()
