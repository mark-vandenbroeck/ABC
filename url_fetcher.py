import socket
import sqlite3
import json
import requests
import time
import signal
import sys
import os
import base64
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from database import get_db_connection, DB_PATH
import re
import logging
import traceback
from pathlib import Path

from logging.handlers import RotatingFileHandler

# Log file configuration (will be specialized in __init__)
logger = logging.getLogger('url_fetcher')
logger.setLevel(logging.INFO)

DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888
MAX_LINK_DISTANCE = 0

# Set a global socket timeout as a last-resort safety net
socket.setdefaulttimeout(30)

class URLFetcher:
    def __init__(self, fetcher_id):
        self.fetcher_id = fetcher_id
        self.setup_logging()
        self.running = True
        self.robots_cache = {}
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGUSR1, self.dump_stack_trace)
    
    def setup_logging(self):
        """Configure logging for this specific fetcher instance"""
        log_dir = Path(DB_PATH).resolve().parent / 'logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = log_dir / f'fetcher.{self.fetcher_id}.log'
        
        # Remove existing handlers if any
        logger.handlers = []
        
        # 3 MB = 3145728 bytes
        fh = RotatingFileHandler(log_file, maxBytes=3145728, backupCount=4)
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        logger.addHandler(fh)
        logger.info(f"Logging initialized for fetcher {self.fetcher_id}")
    
    def dump_stack_trace(self, sig, frame):
        """Dump stack trace of all threads to the log"""
        logger.info("SIGUSR1 received: Dumping stack trace")
        for thread_id, stack in sys._current_frames().items():
            logger.info(f"\n# ThreadID: {thread_id}")
            for filename, lineno, name, line in traceback.extract_stack(stack):
                logger.info(f'File: "{filename}", line {lineno}, in {name}')
                if line:
                    logger.info(f"  {line.strip()}")
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        print(f"\nFetcher {self.fetcher_id} shutting down...")
        self.running = False
        sys.exit(0)
    
    def get_robots_parser(self, url):
        """Get or create a robots.txt parser for a domain"""
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        if base_url not in self.robots_cache:
            robots_url = urljoin(base_url, '/robots.txt')
            rp = RobotFileParser()
            try:
                headers = {'User-Agent': 'WebCrawler/1.0'}
                # Use requests to get content with a timeout
                response = requests.get(robots_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    rp.parse(response.text.splitlines())
                else:
                    # If 404 or other error, assume allow_all = True
                    rp.allow_all = True
            except Exception as e:
                logger.warning(f"Fetcher {self.fetcher_id} - Could not read robots.txt from {robots_url}: {e}")
                # Allow all if robots.txt can't be read
                rp.allow_all = True
            self.robots_cache[base_url] = rp
        
        return self.robots_cache[base_url]
    
    def can_fetch(self, url):
        """Check if URL can be fetched according to robots.txt"""
        try:
            rp = self.get_robots_parser(url)
            return rp.can_fetch('*', url)
        except:
            return True  # Default to allowing if check fails
    
    def is_mime_type_allowed(self, mime_type):
        """Check if MIME type is allowed based on configuration"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT pattern, enabled FROM mime_types WHERE enabled = 1')
        patterns = cursor.fetchall()
        conn.close()
        
        if not patterns:
            return False
        
        for pattern, enabled in patterns:
            if enabled:
                # Simple wildcard matching
                if '*' in pattern:
                    # Convert pattern to regex
                    regex_pattern = pattern.replace('*', '.*')
                    if re.match(regex_pattern, mime_type):
                        return True
                elif pattern == mime_type:
                    return True
        
        return False
    
    def extract_links(self, html_content, base_url):
        """Extract all links from HTML content"""
        links = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for tag in soup.find_all(['a', 'link']):
                href = tag.get('href')
                if href:
                    absolute_url = urljoin(base_url, href)
                    # Only add http/https URLs
                    parsed = urlparse(absolute_url)
                    if parsed.scheme in ['http', 'https']:
                        links.append(absolute_url)
        except Exception as e:
            print(f"Error extracting links: {e}")
        
        return links
    
    def add_urls_to_database(self, urls, current_distance=0):
        """Add new URLs to the database, storing host when possible to ensure per-host
        cooldowns are applied by the dispatcher immediately after insertion."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        new_distance = current_distance + 1
        added = 0
        for url in urls:
            try:
                parsed = urlparse(url)
                # Only add http/https URLs
                if parsed.scheme not in ('http', 'https'):
                    continue

                host = None
                try:
                    host = parsed.hostname
                except Exception:
                    host = None

                if host:
                    # Extract extension for optimized purging
                    ext = ''
                    try:
                        p = Path(urlparse(url).path)
                        ext = p.suffix[1:].lower() if p.suffix else ''
                    except: pass
                    
                    cursor.execute('INSERT OR IGNORE INTO urls (url, host, link_distance, url_extension) VALUES (?, ?, ?, ?)', (url, host, new_distance, ext))
                else:
                    cursor.execute('INSERT OR IGNORE INTO urls (url, link_distance) VALUES (?, ?)', (url, new_distance))

                if cursor.rowcount > 0:
                    added += 1
            except Exception as e:
                print(f"Error adding URL {url}: {e}")
        
        conn.commit()
        conn.close()
        return added
    
    def fetch_url(self, url_id, url, link_distance=0):
        """Fetch a URL and return the result"""
        try:
            # Check robots.txt
            if not self.can_fetch(url):
                print(f"URL {url} blocked by robots.txt")
                return None
            
            # Fetch the URL
            headers = {
                'User-Agent': 'WebCrawler/1.0'
            }
            try:
                # logger.debug(f"Fetcher {self.fetcher_id} starting requests.get for {url}")
                response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
            except Exception as e:
                # Network error / no response
                logger.info(f"Fetcher {self.fetcher_id} - {url} - ERROR - {e}")
                print(f"Error fetching {url}: {e}")
                return None

            # Log the status code for this download
            status_code = getattr(response, 'status_code', 'NO_RESPONSE')
            logger.info(f"Fetcher {self.fetcher_id} - {url} - {status_code}")

            try:
                response.raise_for_status()
            except Exception as e:
                # HTTP error (e.g., 404) - we already logged the status code above
                print(f"Error fetching {url}: {e}")
                return None

            mime_type = response.headers.get('Content-Type', '').split(';')[0].strip()
            
            # Check if MIME type is allowed
            if not self.is_mime_type_allowed(mime_type):
                print(f"URL {url} has disallowed MIME type: {mime_type}")
                return {
                    'url_id': url_id,
                    'size_bytes': len(response.content),
                    'mime_type': mime_type,
                    'document': b''  # Don't store disallowed types
                }
            
            content = response.content
            size_bytes = len(content)
            
            # Extract links if HTML
            if mime_type.startswith('text/html'):
                try:
                    # Check link distance before harvesting
                    if link_distance < MAX_LINK_DISTANCE:
                        html_text = content.decode('utf-8', errors='ignore')
                        links = self.extract_links(html_text, url)
                        if links:
                            added = self.add_urls_to_database(links, current_distance=link_distance)
                            print(f"Added {added} new URLs from {url} (dist: {link_distance} -> {link_distance+1})")
                    else:
                        print(f"Skipping link harvesting for {url} (distance {link_distance} >= {MAX_LINK_DISTANCE})")
                except Exception as e:
                    print(f"Error processing links from {url}: {e}")
            
            return {
                'url_id': url_id,
                'size_bytes': size_bytes,
                'mime_type': mime_type,
                'document': content,
                'http_status': status_code
            }
            
        except requests.exceptions.Timeout as e:
            print(f"Timeout fetching {url}: {e}")
            return {'error_type': 'timeout', 'error_message': str(e)}
        except requests.exceptions.ConnectionError as e:
            # Could be DNS resolution error or other connection issue
            msg = str(e)
            error_type = 'dns' if 'Name or service not known' in msg or 'nodename nor servname' in msg else 'connection'
            print(f"Connection error fetching {url}: {e}")
            return {'error_type': error_type, 'error_message': msg}
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return {'error_type': 'other', 'error_message': str(e)}
    
    def _submit_result(self, result_data):
        """Submit result to dispatcher using a new connection"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(15.0)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            
            # Prepare payload
            if 'action' not in result_data:
                # It's a raw result from fetch_url
                document_b64 = base64.b64encode(result_data['document']).decode('utf-8') if result_data.get('document') else ''
                payload = {
                    'action': 'submit_result',
                    'url_id': result_data['url_id'],
                    'size_bytes': result_data.get('size_bytes', 0),
                    'mime_type': result_data.get('mime_type', ''),
                    'document': document_b64,
                    'http_status': result_data.get('http_status'),
                    'error_type': result_data.get('error_type')
                }
            else:
                payload = result_data

            sock.send(json.dumps(payload).encode('utf-8'))
            sock.close()
        except Exception as e:
            print(f"Error submitting result: {e}")

    def communicate_with_dispatcher(self):
        """Communicate with dispatcher to get URLs and submit results"""
        try:
            # 1. Get URL
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(60.0) # Increased timeout for dispatcher bottlenecks
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            
            request = {'action': 'get_url'}
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            response_data = sock.recv(4096).decode('utf-8')
            sock.close() # Close immediately
            
            response = json.loads(response_data)
            
            if response['status'] == 'ok':
                url_id = response['url_id']
                url = response['url']
                link_distance = response.get('link_distance', 0)
                
                print(f"Fetcher {self.fetcher_id} fetching: {url} (dist: {link_distance})")
                
                # 2. Fetch
                result = self.fetch_url(url_id, url, link_distance)
                
                # 3. Submit Result via NEW connection
                if result:
                    self._submit_result(result)
                else:
                    # Submit failure/empty to ensure it's marked processed
                    self._submit_result({
                        'action': 'submit_result',
                        'url_id': url_id,
                        'size_bytes': 0,
                        'mime_type': '',
                        'document': '',
                        'http_status': None
                    })
                return True

            elif response['status'] == 'no_urls':
                time.sleep(2)
                return True
            else:
                time.sleep(2)
                return False
                
        except Exception as e:
            logger.error(f"Fetcher {self.fetcher_id} communication error: {e}")
            print(f"Fetcher {self.fetcher_id} communication error: {e}")
            time.sleep(2) # Reduced sleep for faster recovery
            return False
    
    def run(self):
        """Main loop"""
        logger.info(f"Fetcher {self.fetcher_id} started (PID: {os.getpid()})")
        
        while self.running:
            try:
                has_work = self.communicate_with_dispatcher()
                
                if not has_work:
                    # No URLs available, wait a bit
                    time.sleep(2)
                else:
                    # Small delay between fetches
                    time.sleep(0.5)
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Fetcher {self.fetcher_id} error: {e}")
                time.sleep(2)

if __name__ == '__main__':
    import sys
    fetcher_id = sys.argv[1] if len(sys.argv) > 1 else '1'
    fetcher = URLFetcher(fetcher_id)
    fetcher.run()

