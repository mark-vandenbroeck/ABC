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
from pathlib import Path

from logging.handlers import RotatingFileHandler

# Log file in the logs directory
LOG_FILE = Path(DB_PATH).resolve().parent / 'logs' / 'fetcher.log'
logger = logging.getLogger('url_fetcher')
logger.setLevel(logging.INFO)
if not logger.handlers:
    os.makedirs(LOG_FILE.parent, exist_ok=True)
    # 3 MB = 3145728 bytes
    fh = RotatingFileHandler(LOG_FILE, maxBytes=3145728, backupCount=4)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(fh)

DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

class URLFetcher:
    def __init__(self, fetcher_id):
        self.fetcher_id = fetcher_id
        self.running = True
        self.robots_cache = {}
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
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
                rp.set_url(robots_url)
                rp.read()
            except Exception as e:
                print(f"Warning: Could not read robots.txt from {robots_url}: {e}")
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
    
    def add_urls_to_database(self, urls):
        """Add new URLs to the database, storing host when possible to ensure per-host
        cooldowns are applied by the dispatcher immediately after insertion."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
                    cursor.execute('INSERT OR IGNORE INTO urls (url, host) VALUES (?, ?)', (url, host))
                else:
                    cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))

                if cursor.rowcount > 0:
                    added += 1
            except Exception as e:
                print(f"Error adding URL {url}: {e}")
        
        conn.commit()
        conn.close()
        return added
    
    def fetch_url(self, url_id, url):
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
                response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
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
                    # Harvesting disabled temporarily
                    pass
                    # html_text = content.decode('utf-8', errors='ignore')
                    # links = self.extract_links(html_text, url)
                    # if links:
                    #     added = self.add_urls_to_database(links)
                    #     print(f"Added {added} new URLs from {url}")
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
    
    def communicate_with_dispatcher(self):
        """Communicate with dispatcher to get URLs and submit results"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
            
            # Request a URL
            request = {'action': 'get_url'}
            sock.send(json.dumps(request).encode('utf-8'))
            
            # Receive response
            response_data = sock.recv(4096).decode('utf-8')
            response = json.loads(response_data)
            
            if response['status'] == 'ok':
                url_id = response['url_id']
                url = response['url']
                
                print(f"Fetcher {self.fetcher_id} fetching: {url}")
                
                # Fetch the URL
                result = self.fetch_url(url_id, url)
                
                if result:
                    # If fetch_url returned an error_type, send it so dispatcher can act (e.g., disable host)
                    if 'error_type' in result:
                        submit_request = {
                            'action': 'submit_result',
                            'url_id': url_id,
                            'size_bytes': 0,
                            'mime_type': '',
                            'document': '',
                            'http_status': None,
                            'error_type': result.get('error_type')
                        }
                        sock.send(json.dumps(submit_request).encode('utf-8'))
                        try:
                            sock.recv(1024)
                        except:
                            pass
                        print(f"Fetcher {self.fetcher_id} error for {url}: {result.get('error_type')}")
                    else:
                        # Submit result (encode binary as base64 for JSON)
                        document_b64 = base64.b64encode(result['document']).decode('utf-8') if result['document'] else ''
                        submit_request = {
                            'action': 'submit_result',
                            'url_id': result['url_id'],
                            'size_bytes': result['size_bytes'],
                            'mime_type': result['mime_type'],
                            'document': document_b64,
                            'http_status': result.get('http_status')
                        }
                        
                        sock.send(json.dumps(submit_request).encode('utf-8'))
                        submit_response = sock.recv(1024).decode('utf-8')
                        print(f"Fetcher {self.fetcher_id} completed: {url}")
                else:
                    # Mark as fetched even if failed (to avoid infinite retries)
                    submit_request = {
                        'action': 'submit_result',
                        'url_id': url_id,
                        'size_bytes': 0,
                        'mime_type': '',
                        'document': ''  # Empty base64 string
                    }
                    sock.send(json.dumps(submit_request).encode('utf-8'))
                    sock.recv(1024)
            
            sock.close()
            return response['status'] == 'ok'
            
        except Exception as e:
            print(f"Fetcher {self.fetcher_id} communication error: {e}")
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

