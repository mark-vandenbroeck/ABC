import socket
import sqlite3
import json
import time
import signal
import sys
import base64
import re
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timedelta
from database import get_db_connection, DB_PATH, init_database
import threading
import logging
from logging.handlers import RotatingFileHandler

# Configure logging with rotation
LOG_FILE = Path(DB_PATH).resolve().parent / 'logs' / 'dispatcher.log'
logger = logging.getLogger('url_dispatcher')
logger.setLevel(logging.INFO)
if not logger.handlers:
    # 10 MB = 10485760 bytes
    handler = RotatingFileHandler(LOG_FILE, maxBytes=10485760, backupCount=4)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)

DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

class URLDispatcher:
    # How long (seconds) to wait for a fetcher to return a submit_result after sending a URL.
    # Must be longer than the fetcher's HTTP timeout (URL_FETCH_TIMEOUT). Set to 40s to allow slow hosts.
    SUBMIT_RESULT_TIMEOUT = 40
    # How many retries before we disable a host for repeated timeouts.
    # Increase this if you have lots of slow-but-healthy hosts.
    DISABLE_RETRIES_THRESHOLD = 4

    def __init__(self):
        self.running = True
        self.server_socket = None
        self.connected_fetchers = []
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Ensure DB is initialized (migrations)
        try:
            init_database()
        except Exception:
            pass

        # Log scanner state
        self._log_pos = 0
        self._log_thread = threading.Thread(target=self._log_scanner_loop, daemon=True)
        self._log_thread.start()
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        logger.info("Shutting down dispatcher...")
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        sys.exit(0)
    
    def _get_host(self, url):
        try:
            return urlparse(url).hostname
        except Exception:
            return None

    def _host_allowed(self, host, cooldown_seconds=10):
        if not host:
            return True
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT last_access FROM hosts WHERE host = ?', (host,))
        row = cur.fetchone()
        conn.close()
        if not row or not row[0]:
            return True
        try:
            last_access = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        except Exception:
            # If parsing fails, allow
            return True
        return (datetime.utcnow() - last_access) >= timedelta(seconds=cooldown_seconds)

    def _log_scanner_loop(self, interval_seconds=60):
        """Periodically scan the fetcher log for DNS/NameResolution errors and mark hosts disabled with a reason."""
        log_path = Path(DB_PATH).resolve().parent / 'logs' / 'fetcher.log'
        pattern = re.compile(r"Failed to resolve '([^']+)'", re.IGNORECASE)
        while self.running:
            try:
                if not log_path.exists():
                    time.sleep(interval_seconds)
                    continue
                with log_path.open('r', encoding='utf-8', errors='replace') as fh:
                    fh.seek(self._log_pos)
                    data = fh.read()
                    self._log_pos = fh.tell()
                if not data:
                    time.sleep(interval_seconds)
                    continue
                for m in pattern.finditer(data):
                    host = m.group(1)
                    try:
                        conn = get_db_connection(); cur = conn.cursor()
                        cur.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads, disabled, disabled_reason, disabled_at) VALUES (?, NULL, NULL, 0, 1, ?, CURRENT_TIMESTAMP)', (host, 'dns'))
                        cur.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP WHERE host = ?', ('dns', host))
                        conn.commit(); conn.close()
                        logger.warning(f"Log scanner: marked host {host} disabled (dns)")
                    except Exception as e:
                        logger.warning(f"Log scanner warning: could not update host {host}: {e}")
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Log scanner error: {e}")
                time.sleep(interval_seconds)

    def get_next_url(self, batch_size=50, dispatch_timeout_seconds=60, host_cooldown_seconds=10):
        """Get a batch of URLs for a single host to process.
        Returns a list of {'id': id, 'url': url} or None.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        timeout_param = f'-{dispatch_timeout_seconds} seconds'
        cooldown_param = f'-{host_cooldown_seconds} seconds'

        try:
            conn.execute('BEGIN IMMEDIATE')

            # 1. Find an eligible candidate host first
            cursor.execute('''
                SELECT u.host
                FROM urls u
                LEFT JOIN hosts h ON u.host = h.host
                WHERE ((u.status = '' OR u.status IS NULL) OR (u.status = 'dispatched' AND (u.dispatched_at IS NULL OR u.dispatched_at <= datetime('now', ?))))
                  AND (u.retries IS NULL OR u.retries < 3)
                  AND (h.last_access IS NULL OR h.last_access <= datetime('now', ?))
                  AND (h.disabled IS NULL OR h.disabled = 0)
                ORDER BY u.created_at ASC
                LIMIT 1
            ''', (timeout_param, cooldown_param))

            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None
            
            target_host = row[0]

            # 2. Get a batch of URLs for this specific host
            cursor.execute('''
                SELECT id, url
                FROM urls
                WHERE host = ?
                  AND ((status = '' OR status IS NULL) OR (status = 'dispatched' AND (dispatched_at IS NULL OR dispatched_at <= datetime('now', ?))))
                  AND (retries IS NULL OR retries < 3)
                ORDER BY created_at ASC
                LIMIT ?
            ''', (target_host, timeout_param, batch_size))

            candidates = cursor.fetchall()
            if not candidates:
                conn.commit()
                return None

            urls_to_dispatch = []
            for url_id, url in candidates:
                # Update status
                cursor.execute('''
                    UPDATE urls
                    SET status = 'dispatched', dispatched_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (url_id,))
                urls_to_dispatch.append({'id': url_id, 'url': url})

            # 3. Reserve the host's last_access
            if target_host:
                try:
                    cursor.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads) VALUES (?, CURRENT_TIMESTAMP, NULL, 0)', (target_host,))
                    cursor.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP WHERE host = ?', (target_host,))
                except Exception as e2:
                    logger.warning(f"Warning: could not reserve host {target_host} on dispatch: {e2}")

            conn.commit()
            return urls_to_dispatch
        except Exception as e:
            logger.error(f"Error in get_next_url: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            conn.close()

    def get_next_fetched_url(self, batch_size=50):
        """Get a batch of URLs with status 'fetched' for parsing"""
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            conn.execute('BEGIN IMMEDIATE')
            cursor.execute('''
                SELECT id, url
                FROM urls
                WHERE status = 'fetched'
                ORDER BY downloaded_at ASC
                LIMIT ?
            ''', (batch_size,))
            
            rows = cursor.fetchall()
            if not rows:
                conn.commit()
                return None
            
            urls = []
            for url_id, url in rows:
                # Mark as 'parsing' to avoid other parsers picking it up
                cursor.execute('UPDATE urls SET status = \'parsing\' WHERE id = ?', (url_id,))
                urls.append({'id': url_id, 'url': url})
            
            conn.commit()
            return urls
        except Exception as e:
            logger.error(f"Error in get_next_fetched_url: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            conn.close()
    
    def mark_url_parsed(self, url_id, has_abc):
        """Mark a URL as parsed and set the has_abc flag"""
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE urls 
                SET status = 'parsed',
                    has_abc = ?
                WHERE id = ?
            ''', (has_abc, url_id))
            conn.commit()
        finally:
            conn.close()
    
    def mark_url_fetched(self, url_id, size_bytes, mime_type, document, http_status=None):
        """Mark a URL as fetched in the database and reset retries; update host downloads and status"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE urls 
            SET downloaded_at = CURRENT_TIMESTAMP,
                size_bytes = ?,
                mime_type = ?,
                document = ?,
                http_status = ?,
                retries = 0,
                status = 'fetched',
                dispatched_at = NULL
            WHERE id = ?
        ''', (size_bytes, mime_type, document, http_status, url_id))
        
        conn.commit()

        # Update hosts table: increment downloads and record last access/status
        try:
            cursor.execute('SELECT url FROM urls WHERE id = ?', (url_id,))
            r = cursor.fetchone()
            if r:
                host = self._get_host(r[0])

                cursor.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads) VALUES (?, NULL, NULL, 0)', (host,))
                cursor.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = ?, downloads = COALESCE(downloads, 0) + 1 WHERE host = ?', (http_status, host))
                conn.commit()
        except Exception as e:
            logger.warning(f"Warning: could not update host record in mark_url_fetched: {e}")
        finally:
            conn.close()
    
    def handle_fetcher_request(self, client_socket, address):
        """Handle a request from a fetcher or parser"""
        try:
            client_socket.settimeout(5.0) 
            f = client_socket.makefile('r', encoding='utf-8')
            line = f.readline()
            
            if not line:
                return
            
            try:
                request = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding initial request: {e}")
                return
            
            action = request.get('action')

            if action == 'get_url':
                # Get batch of URLs for fetching
                urls = self.get_next_url()
                if urls:
                    response = {'status': 'ok', 'urls': urls}
                    client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                    
                    # Expect multiple submit_result calls
                    client_socket.settimeout(self.SUBMIT_RESULT_TIMEOUT)
                    for _ in range(len(urls)):
                        res_line = f.readline()
                        if not res_line:
                            break
                        
                        try:
                            res_req = json.loads(res_line)
                            if res_req.get('action') == 'submit_result':
                                url_id = res_req['url_id']
                                size_bytes = res_req.get('size_bytes', 0)
                                mime_type = res_req.get('mime_type', '')
                                document_b64 = res_req.get('document', '')
                                http_status = res_req.get('http_status')
                                error_type = res_req.get('error_type')

                                document = b''
                                if document_b64:
                                    try:
                                        document = base64.b64decode(document_b64)
                                    except:
                                        pass

                                # Handle result
                                if error_type or (http_status is not None and http_status >= 400):
                                    # Failure handling
                                    conn = get_db_connection()
                                    cur = conn.cursor()
                                    cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = ?', (url_id,))
                                    conn.commit()
                                    cur.execute('SELECT retries, url FROM urls WHERE id = ?', (url_id,))
                                    retries, url = cur.fetchone()

                                    if retries >= self.DISABLE_RETRIES_THRESHOLD:
                                        cur.execute("UPDATE urls SET status = 'error', downloaded_at = CURRENT_TIMESTAMP, http_status = ?, dispatched_at = NULL WHERE id = ?", (http_status, url_id))
                                    else:
                                        cur.execute("UPDATE urls SET status = '', dispatched_at = NULL WHERE id = ?", (url_id,))
                                    
                                    # Disabled host check
                                    host = self._get_host(url)
                                    if host:
                                        should_disable = False
                                        reason = None
                                        if error_type == 'dns':
                                            should_disable = True
                                            reason = 'dns'
                                        elif (error_type == 'timeout' or http_status is None) and retries >= self.DISABLE_RETRIES_THRESHOLD:
                                            should_disable = True
                                            reason = 'timeout'
                                        
                                        if should_disable:
                                            logger.warning(f"Marking host {host} disabled due to {reason}")
                                            cur.execute('INSERT OR IGNORE INTO hosts (host) VALUES (?)', (host,))
                                            cur.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (reason, http_status, host))
                                        else:
                                            cur.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (http_status, host))
                                    
                                    conn.commit()
                                    conn.close()
                                else:
                                    # Success
                                    self.mark_url_fetched(url_id, size_bytes, mime_type, document, http_status)
                                
                                # Send ACK for this URL
                                client_socket.sendall((json.dumps({'status': 'ok'}) + '\n').encode('utf-8'))
                        except Exception as e:
                            logger.error(f"Error processing batched fetch result: {e}")
                            break
                else:
                    client_socket.sendall((json.dumps({'status': 'no_urls'}) + '\n').encode('utf-8'))
            
            elif action == 'get_fetched_url':
                # Get batch of URLs for parsing
                urls = self.get_next_fetched_url()
                if urls:
                    response = {'status': 'ok', 'urls': urls}
                    client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                    
                    # Expect multiple submit_parsed_result calls
                    client_socket.settimeout(5.0)
                    for _ in range(len(urls)):
                        res_line = f.readline()
                        if not res_line:
                            break
                        try:
                            res_req = json.loads(res_line)
                            if res_req.get('action') == 'submit_parsed_result':
                                self.mark_url_parsed(res_req['url_id'], res_req.get('has_abc', False))
                                client_socket.sendall((json.dumps({'status': 'ok'}) + '\n').encode('utf-8'))
                        except:
                            break
                else:
                    client_socket.sendall((json.dumps({'status': 'no_urls'}) + '\n').encode('utf-8'))

            elif action == 'submit_parsed_result':
                # Legacy support for single submit
                self.mark_url_parsed(request['url_id'], request.get('has_abc', False))
                client_socket.sendall((json.dumps({'status': 'ok'}) + '\n').encode('utf-8'))

        except Exception as e:
            logger.error(f"Error handling request from {address}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def run(self):
        """Run the dispatcher server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((DISPATCHER_HOST, DISPATCHER_PORT))
            self.server_socket.listen(5)
            logger.info(f"URL Dispatcher listening on {DISPATCHER_HOST}:{DISPATCHER_PORT}")
            
            import threading
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"Fetcher connected from {address}")
                    # Handle each fetcher connection in a separate thread so multiple fetchers can
                    # request URLs concurrently and we don't block on long-running fetches.
                    t = threading.Thread(target=self.handle_fetcher_request, args=(client_socket, address), daemon=True)
                    t.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {e}")
                    continue
                    
        except Exception as e:
            logger.critical(f"Dispatcher error: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()

if __name__ == '__main__':
    dispatcher = URLDispatcher()
    dispatcher.run()

