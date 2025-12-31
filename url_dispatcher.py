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
        print("\nShutting down dispatcher...")
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
                        print(f"Log scanner: marked host {host} disabled (dns)")
                    except Exception as e:
                        print(f"Log scanner warning: could not update host {host}: {e}")
                time.sleep(interval_seconds)
            except Exception as e:
                print(f"Log scanner error: {e}")
                time.sleep(interval_seconds)

    def get_next_url(self, batch_size=1000, dispatch_timeout_seconds=60, host_cooldown_seconds=10):
        """Get the next URL to process (oldest created, not yet fetched, or dispatched but timed out),
        using an SQL filter that joins `hosts` so we exclude URLs whose host has been accessed within
        the cooldown window.

        - dispatch_timeout_seconds: if a URL was marked 'dispatched' but no result arrived within this
          many seconds, it becomes eligible again.
        - host_cooldown_seconds: minimum seconds since hosts.last_access to allow dispatching another URL
          for that host.
        """
        conn = get_db_connection()
        cursor = conn.cursor()

        timeout_param = f'-{dispatch_timeout_seconds} seconds'
        cooldown_param = f'-{host_cooldown_seconds} seconds'

        # Use a transaction and conditional updates to atomically claim a URL only if
        # its host hasn't been accessed within the cooldown window. We select a batch
        # of candidates then try to claim each with a conditional UPDATE that checks
        # the host's last_access in the WHERE clause — this prevents races where two
        # threads both see the host as eligible and both try to claim different URLs.
        try:
            conn.execute('BEGIN IMMEDIATE')

            # Fetch a batch of candidate URLs ordered by created_at
            cursor.execute('''
                SELECT u.id, u.url, u.host
                FROM urls u
                WHERE ((u.status = '' OR u.status IS NULL) OR (u.status = 'dispatched' AND (u.dispatched_at IS NULL OR u.dispatched_at <= datetime('now', ?))))
                  AND (u.retries IS NULL OR u.retries < 3)
                ORDER BY u.created_at ASC
                LIMIT ?
            ''', (timeout_param, batch_size))

            candidates = cursor.fetchall()
            if not candidates:
                conn.commit()
                return None

            for url_id, url, host in candidates:
                # Try to atomically claim this URL only if the host is allowed
                try:
                    cursor.execute('''
                        UPDATE urls
                        SET status = 'dispatched', dispatched_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                          AND ((status = '' OR status IS NULL) OR (status = 'dispatched' AND (dispatched_at IS NULL OR dispatched_at <= datetime('now', ?))))
                          AND (
                            (SELECT last_access FROM hosts WHERE host = urls.host) IS NULL
                            OR (SELECT last_access FROM hosts WHERE host = urls.host) <= datetime('now', ?)
                          )
                          AND (
                            (SELECT disabled FROM hosts WHERE host = urls.host) IS NULL
                            OR (SELECT disabled FROM hosts WHERE host = urls.host) = 0
                          )
                    ''', (url_id, timeout_param, cooldown_param))

                    if cursor.rowcount == 0:
                        # Could be raced or host not allowed; try next candidate
                        continue

                    # Successfully claimed the URL; now reserve the host's last_access
                    if host:
                        try:
                            cursor.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads) VALUES (?, CURRENT_TIMESTAMP, NULL, 0)', (host,))
                            cursor.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP WHERE host = ?', (host,))
                        except Exception as e2:
                            print(f"Warning: could not reserve host {host} on dispatch: {e2}")

                    conn.commit()
                    return {'id': url_id, 'url': url}
                except sqlite3.OperationalError:
                    # Lock error while trying to claim; roll back and give up (caller may retry)
                    try:
                        conn.rollback()
                    except:
                        pass
                    return None

            # No candidate could be claimed
            conn.commit()
            return None
        finally:
            conn.close()

    def get_next_fetched_url(self, batch_size=100):
        """Get the next URL with status 'fetched' for parsing"""
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
            
            candidates = cursor.fetchall()
            if not candidates:
                conn.commit()
                return None
                
            for url_id, url in candidates:
                # Atomically mark as parsing to avoid double work
                cursor.execute("UPDATE urls SET status = 'parsing' WHERE id = ?", (url_id,))
                if cursor.rowcount > 0:
                    conn.commit()
                    return {'id': url_id, 'url': url}
            
            conn.commit()
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
            print(f"Warning: could not update host record in mark_url_fetched: {e}")
        finally:
            conn.close()
    
    def handle_fetcher_request(self, client_socket, address):
        """Handle a request from a fetcher"""
        try:
            # The original recv loop is replaced by makefile and readline for efficiency
            client_socket.settimeout(2.0) # Initial timeout for the first line
            f = client_socket.makefile('r', encoding='utf-8')
            line = f.readline()
            client_socket.settimeout(None) # Reset timeout after reading the first line

            if not line:
                # Client closed connection or sent nothing
                return
            
            try:
                request = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Error decoding initial request: {e}")
                return
            
            url_data = None # Initialize url_data for scope outside if/elif blocks

            if request['action'] == 'get_url':
                # Get next URL for fetching
                url_data = self.get_next_url()
                
                if url_data:
                    response = {
                        'status': 'ok',
                        'url_id': url_data['id'],
                        'url': url_data['url']
                    }
                else:
                    response = {'status': 'no_urls'}
                
                client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
            
            elif request['action'] == 'get_fetched_url':
                # Get next URL for parsing
                url_data = self.get_next_fetched_url()
                if url_data:
                    response = {
                        'status': 'ok',
                        'url_id': url_data['id'],
                        'url': url_data['url']
                    }
                else:
                    response = {'status': 'no_urls'}
                client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                return

            elif request['action'] == 'submit_parsed_result':
                # Parser submits result
                url_id = request['url_id']
                has_abc = request.get('has_abc', False)
                self.mark_url_parsed(url_id, has_abc)
                response = {'status': 'ok'}
                client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                return

            # If we sent a URL, ensure the host exists in hosts table, but do NOT update last_access yet.
            # last_access should reflect actual fetch attempts/results (updated in submit_result/mark_url_fetched).
            # This block is executed only if the action was 'get_url' and url_data was returned.
            if url_data:
                try:
                    host = self._get_host(url_data['url'])
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads) VALUES (?, NULL, NULL, 0)', (host,))
                    conn.commit()
                    conn.close()
                except Exception as e:
                    print(f"Warning: could not ensure host record for {url_data.get('url')}: {e}")

                # Expect the fetcher to send a submit_result on the same socket (as a second line)
                try:
                    client_socket.settimeout(self.SUBMIT_RESULT_TIMEOUT)
                    line2 = f.readline()
                    client_socket.settimeout(None)
                    
                    if not line2:
                        print('No result received from fetcher (socket closed or timed out)')
                        request2 = None
                    else:
                        request2 = json.loads(line2)
                        
                    if request2 and request2.get('action') == 'submit_result':
                        # log error_type if the fetcher provided it
                        print(f"in-socket submit_result error_type={request2.get('error_type') if isinstance(request2, dict) else None}")
                        url_id = request2['url_id']
                        size_bytes = request2.get('size_bytes', 0)
                        mime_type = request2.get('mime_type', '')
                        document_b64 = request2.get('document', '')
                        http_status = request2.get('http_status')

                        print(f"Received result for URL id={url_id}, size={size_bytes}, mime={mime_type}, http_status={http_status}")

                        if document_b64:
                            try:
                                document = base64.b64decode(document_b64)
                            except Exception as e:
                                print(f"Error decoding document: {e}")
                                document = b''
                        else:
                            document = b''

                        # Determine success vs failure: failure if http_status is None (network) or >=400
                        retries = 0
                        if http_status is None or (isinstance(http_status, int) and http_status >= 400):
                            try:
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = ?', (url_id,))
                                conn.commit()
                                cur.execute('SELECT retries FROM urls WHERE id = ?', (url_id,))
                                retries = cur.fetchone()[0]
                                if retries >= self.DISABLE_RETRIES_THRESHOLD:
                                    cur.execute("UPDATE urls SET status = 'error', downloaded_at = CURRENT_TIMESTAMP, http_status = ?, dispatched_at = NULL WHERE id = ?", (http_status, url_id))
                                    conn.commit()
                                    print(f"URL id={url_id} marked error after {retries} retries")
                                else:
                                    try:
                                        cur.execute("UPDATE urls SET status = '', dispatched_at = NULL WHERE id = ?", (url_id,))
                                        conn.commit()
                                    except Exception as e:
                                        print(f"Warning: couldn't reset status for url {url_id}: {e}")
                                    print(f"URL id={url_id} failed (http_status={http_status}), retries now {retries}")
                            except Exception as e:
                                print(f"Error updating retries for url {url_id}: {e}")
                            finally:
                                conn.close()

                            err_type = request2.get('error_type') if isinstance(request2, dict) else None
                            response2 = {'status': 'ok'}
                            client_socket.sendall((json.dumps(response2) + '\n').encode('utf-8'))

                            try:
                                conn2 = get_db_connection()
                                cur2 = conn2.cursor()
                                cur2.execute('SELECT url FROM urls WHERE id = ?', (url_id,))
                                r = cur2.fetchone()
                                if r:
                                    host = self._get_host(r[0])
                                cur2.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, disabled) VALUES (?, NULL, NULL, 0)', (host,))

                                should_disable = False
                                reason = None
                                if err_type == 'dns':
                                    should_disable = True
                                    reason = 'dns'
                                elif err_type == 'timeout' and retries >= self.DISABLE_RETRIES_THRESHOLD:
                                    should_disable = True
                                    reason = 'timeout'
                                elif err_type is None and http_status is None and retries >= self.DISABLE_RETRIES_THRESHOLD:
                                    should_disable = True
                                    reason = 'timeout'

                                if should_disable:
                                    print(f"Marking host {host} disabled due to reason={reason} http_status={http_status} (retries={retries})")
                                    cur2.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (reason, http_status, host))
                                else:
                                    cur2.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (http_status, host))
                                conn2.commit()
                            except Exception as e:
                                print(f"Warning: could not update host record: {e}")
                            finally:
                                try: conn2.close()
                                except: pass
                        else:
                            self.mark_url_fetched(url_id, size_bytes, mime_type, document, http_status)
                            response2 = {'status': 'ok'}
                            client_socket.sendall((json.dumps(response2) + '\n').encode('utf-8'))
                except socket.timeout:
                    print('Timed out waiting for submit_result from fetcher')
                    if url_data:
                        url_id = url_data['id']
                        try:
                            conn = get_db_connection()
                            cur = conn.cursor()
                            cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = ?', (url_id,))
                            conn.commit()
                            cur.execute('SELECT retries FROM urls WHERE id = ?', (url_id,))
                            retries = cur.fetchone()[0]
                            if retries >= self.DISABLE_RETRIES_THRESHOLD:
                                cur.execute("UPDATE urls SET status = 'error', downloaded_at = CURRENT_TIMESTAMP, http_status = NULL, dispatched_at = NULL WHERE id = ?", (url_id,))
                            else:
                                cur.execute("UPDATE urls SET status = '', dispatched_at = NULL WHERE id = ?", (url_id,))
                            conn.commit()
                            conn.close()
                        except Exception as e:
                            print(f"Error handling missing submit_result: {e}")
                except Exception as e:
                    print(f"Error receiving submit_result: {e}")
                    try:
                        client_socket.sendall((json.dumps({'status': 'error', 'message': str(e)}) + '\n').encode('utf-8'))
                    except:
                        pass
            elif request['action'] == 'submit_result':
                # Standalone submit_result handling
                pass
                url_id = request['url_id']
                size_bytes = request.get('size_bytes', 0)
                mime_type = request.get('mime_type', '')
                document_b64 = request.get('document', '')
                http_status = request.get('http_status')

                print(f"Received result for URL id={url_id}, size={size_bytes}, mime={mime_type}, http_status={http_status}")
                print(f"submit_result payload error_type={request.get('error_type') if isinstance(request, dict) else None}")
                
                # Decode base64 back to binary
                if document_b64:
                    try:
                        document = base64.b64decode(document_b64)
                    except Exception as e:
                        print(f"Error decoding document: {e}")
                        document = b''
                else:
                    document = b''

                # Determine success vs failure: failure if http_status is None (network) or >=400
                if http_status is None or (isinstance(http_status, int) and http_status >= 400):
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = ?', (url_id,))
                        conn.commit()
                        cur.execute('SELECT retries FROM urls WHERE id = ?', (url_id,))
                        retries = cur.fetchone()[0]
                        if retries >= 3:
                            cur.execute("UPDATE urls SET status = 'error', downloaded_at = CURRENT_TIMESTAMP, http_status = ?, dispatched_at = NULL WHERE id = ?", (http_status, url_id))
                            conn.commit()
                            print(f"URL id={url_id} marked error after {retries} retries")
                        else:
                            # Allow retrying: reset status and clear dispatched_at so get_next_url can pick it again
                            try:
                                cur.execute("UPDATE urls SET status = '', http_status = ?, dispatched_at = NULL WHERE id = ?", (http_status, url_id))
                                conn.commit()
                            except Exception as e:
                                print(f"Warning: couldn't reset status for url {url_id}: {e}")
                            print(f"URL id={url_id} failed (http_status={http_status}), retries now {retries}")
                    except Exception as e:
                        print(f"Error updating retries for url {url_id}: {e}")
                    finally:
                        conn.close()
                    response = {'status': 'ok'}
                    client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))

                    # Update host info (last_access, last_http_status) and mark host disabled conservatively
                    try:
                        conn2 = get_db_connection()
                        cur2 = conn2.cursor()
                        cur2.execute('SELECT url FROM urls WHERE id = ?', (url_id,))
                        r = cur2.fetchone()
                        if r:
                            host = self._get_host(r[0])
                            cur2.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, disabled) VALUES (?, NULL, NULL, 0)', (host,))

                            # Decide whether to disable host: immediate for DNS, but for timeouts only after multiple retries
                            err_type = request.get('error_type') if isinstance(request, dict) else None
                            # Fetch the current retries count
                            cur2.execute('SELECT retries FROM urls WHERE id = ?', (url_id,))
                            row_retries = cur2.fetchone()
                            retries = row_retries[0] if row_retries else 0

                            should_disable = False
                            reason = None
                            if err_type == 'dns':
                                should_disable = True
                                reason = 'dns'
                            elif err_type == 'timeout' and retries >= 3:
                                should_disable = True
                                reason = 'timeout'
                            elif err_type is None and http_status is None and retries >= 3:
                                should_disable = True
                                reason = 'timeout'

                            if should_disable:
                                print(f"Marking host {host} disabled due to reason={reason} http_status={http_status} (retries={retries})")
                                cur2.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (reason, http_status, host))
                            else:
                                cur2.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (http_status, host))

                        conn2.commit()
                    except Exception as e:
                        print(f"Warning: could not update host record: {e}")
                    finally:
                        try:
                            conn2.close()
                        except:
                            pass
                else:
                    self.mark_url_fetched(url_id, size_bytes, mime_type, document, http_status)
                    response = {'status': 'ok'}
                    client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                
        except Exception as e:
            print(f"Error handling fetcher request: {e}")
            try:
                error_response = {'status': 'error', 'message': str(e)}
                client_socket.sendall((json.dumps(error_response) + '\n').encode('utf-8'))
            except:
                pass
        finally:
            client_socket.close()
    
    def run(self):
        """Run the dispatcher server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((DISPATCHER_HOST, DISPATCHER_PORT))
            self.server_socket.listen(5)
            print(f"URL Dispatcher listening on {DISPATCHER_HOST}:{DISPATCHER_PORT}")
            
            import threading
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    client_socket, address = self.server_socket.accept()
                    print(f"Fetcher connected from {address}")
                    # Handle each fetcher connection in a separate thread so multiple fetchers can
                    # request URLs concurrently and we don't block on long-running fetches.
                    t = threading.Thread(target=self.handle_fetcher_request, args=(client_socket, address), daemon=True)
                    t.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"Error accepting connection: {e}")
                    continue
                    
        except Exception as e:
            print(f"Dispatcher error: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()

if __name__ == '__main__':
    dispatcher = URLDispatcher()
    dispatcher.run()

