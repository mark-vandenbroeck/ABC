import socket
import sqlite3
import json
import time
import signal
import sys
import os
import base64
import re
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timedelta
from database import get_db_connection, DB_PATH, init_database
import threading

DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

import threading
import sys
import traceback

def dump_stack_trace(sig, frame):
    print("\n--- STACK TRACE ---")
    message = "\n".join(traceback.format_stack(frame))
    print(message)
    print("-------------------\n")

import signal
signal.signal(signal.SIGUSR1, dump_stack_trace)

class URLDispatcher:
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

        # Release stale URLs on startup
        self._reset_stale_urls()

        # Write PID file for management dashboard
        self._write_pid()

    def _write_pid(self):
        try:
            os.makedirs('run', exist_ok=True)
            pid_file = os.path.join('run', 'dispatcher.pid')
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
        except Exception as e:
            print(f"Warning: Could not write PID file: {e}")
    
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

                # Check if file rotated (size < current pos)
                try:
                    current_size = log_path.stat().st_size
                    if current_size < self._log_pos:
                        self._log_pos = 0
                except:
                    pass

                with log_path.open('r', encoding='utf-8', errors='replace') as fh:
                    fh.seek(self._log_pos)
                    data = fh.read()
                    self._log_pos = fh.tell()
                
                if not data:
                    time.sleep(interval_seconds)
                    continue
                
                matches = list(pattern.finditer(data))
                if matches:
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        count = 0
                        batch_size = 50
                        
                        for i, m in enumerate(matches):
                            host = m.group(1)
                            try:
                                cur.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, downloads, disabled, disabled_reason, disabled_at) VALUES (?, NULL, NULL, 0, 1, ?, CURRENT_TIMESTAMP)', (host, 'dns'))
                                cur.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP WHERE host = ?', ('dns', host))
                                count += 1
                                
                                # Commit in batches to release lock frequently
                                if count % batch_size == 0:
                                    conn.commit()
                            except Exception:
                                pass
                                
                        conn.commit()
                        conn.close()
                        if count > 0:
                            print(f"Log scanner: marked {count} hosts disabled (dns)")
                    except Exception as e:
                        print(f"Log scanner error during batch update: {e}")
                
                time.sleep(300) # Increased interval to 5 minutes to reduce contention
            except Exception as e:
                print(f"Log scanner error: {e}")
                time.sleep(interval_seconds)

    def _reset_stale_urls(self, timeout_seconds=300):
        """Release URLs that were stuck in dispatched/parsing/indexing state from a previous session."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # Reset anything that was dispatched more than 'timeout_seconds' ago
            # or anything that doesn't have a dispatched_at at all but has a transient status
            cur.execute('''
                UPDATE urls 
                SET status = '', dispatched_at = NULL 
                WHERE (status = 'dispatched' OR status = 'parsing' OR status = 'indexing')
                AND (dispatched_at IS NULL OR dispatched_at <= datetime('now', ?))
            ''', (f'-{timeout_seconds} seconds',))
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                print(f"Recovered {count} stale URLs on startup")
        except Exception as e:
            print(f"Error resetting stale URLs: {e}")

    def get_next_url(self, batch_size=100, dispatch_timeout_seconds=120, host_cooldown_seconds=30):
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
            # Optimization: Focus on status = '' to avoid MULTI-INDEX OR and temp B-tree sort.
            # We already verified status is never NULL.
            cursor.execute('''
                SELECT u.id, u.url, u.host, COALESCE(u.link_distance, 0) as link_distance
                FROM urls u
                LEFT JOIN hosts h ON u.host = h.host
                WHERE (u.status = '' OR (u.status = 'dispatched' AND u.dispatched_at <= datetime('now', ?)))
                  AND (u.retries IS NULL OR u.retries < 3)
                  AND (h.disabled IS NULL OR h.disabled = 0)
                  AND (h.last_access IS NULL OR h.last_access <= datetime('now', ?))
                ORDER BY (u.url LIKE '%.abc') DESC, u.created_at ASC
                LIMIT ?
            ''', (timeout_param, cooldown_param, batch_size))

            candidates = cursor.fetchall()
            if not candidates:
                conn.commit()
                return None

            for url_id, url, host, dist in candidates:
                # Try to atomically claim this URL only if the host is allowed
                try:
                    # Try to atomically claim this URL. Since we are in BEGIN IMMEDIATE, 
                    # we only need to verify status hasn't changed.
                    cursor.execute('''
                        UPDATE urls
                        SET status = 'dispatched', dispatched_at = CURRENT_TIMESTAMP
                        WHERE id = ? AND (status = '' OR status = 'dispatched')
                    ''', (url_id,))

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
                    return {'id': url_id, 'url': url, 'link_distance': dist}
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
    
    
    def get_next_fetched_batch(self, batch_size=50, dispatch_timeout_seconds=300):
        """Get a batch of URLs that have been fetched but not yet parsed."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        timeout_param = f'-{dispatch_timeout_seconds} seconds'
        
        try:
            conn.execute('BEGIN IMMEDIATE')
            
            # Select URLs that are 'fetched' OR 'parsing' but timed out
            cursor.execute('''
                SELECT id, url 
                FROM urls 
                WHERE (status = 'fetched') 
                   OR (status = 'parsing' AND (dispatched_at IS NULL OR dispatched_at <= datetime('now', ?)))
                LIMIT ?
            ''', (timeout_param, batch_size))
            
            rows = cursor.fetchall()
            if not rows:
                conn.commit()
                return []
                
            ids = [row[0] for row in rows]
            id_list = ','.join('?' * len(ids))
            
            # Update status to 'parsing'
            cursor.execute(f'''
                UPDATE urls 
                SET status = 'parsing', dispatched_at = CURRENT_TIMESTAMP 
                WHERE id IN ({id_list})
            ''', ids)
            
            conn.commit()
            
            return [{'id': row[0], 'url': row[1]} for row in rows]
            
        except Exception as e:
            print(f"Error getting fetched batch: {e}")
            try:
                conn.rollback()
            except:
                pass
            return []
        finally:
            conn.close()
    
    def get_next_tunebook(self, dispatch_timeout_seconds=300):
        """Get the next tunebook that needs indexing (status = '')."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        timeout_param = f'-{dispatch_timeout_seconds} seconds'
        
        try:
            conn.execute('BEGIN IMMEDIATE')
            
            # Select tunebooks that are new ('') OR 'indexing' but timed out
            cursor.execute('''
                SELECT id
                FROM tunebooks
                WHERE status = ''
                   OR (status = 'indexing' AND (dispatched_at IS NULL OR dispatched_at <= datetime('now', ?)))
                ORDER BY created_at ASC
                LIMIT 1
            ''', (timeout_param,))
            
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None
            
            tunebook_id = row[0]
            
            # Mark as indexing
            cursor.execute('''
                UPDATE tunebooks
                SET status = 'indexing', dispatched_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (tunebook_id,))
            
            conn.commit()
            return tunebook_id
            
        except Exception as e:
            print(f"Error getting next tunebook: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            conn.close()
    
    def mark_tunebook_indexed(self, tunebook_id, success=True):
        """Mark a tunebook as indexed"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            status = 'indexed' if success else 'error'
            cursor.execute('''
                UPDATE tunebooks
                SET status = ?
                WHERE id = ?
            ''', (status, tunebook_id))
            
            if success:
                # Synchronize status to the main urls table
                cursor.execute('''
                    UPDATE urls
                    SET status = 'indexed'
                    WHERE url = (SELECT url FROM tunebooks WHERE id = ?)
                ''', (tunebook_id,))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error marking tunebook {tunebook_id} as indexed: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


    def handle_client_request(self, client_socket, address):
        """Handle a request from a fetcher or parser"""
        request = None
        try:
            # Receive request (read until we can parse complete JSON or timeout)
            client_socket.settimeout(5.0)
            chunks = []
            while True:
                try:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    try:
                        data = b''.join(chunks).decode('utf-8')
                        request = json.loads(data)
                        break
                    except json.JSONDecodeError:
                        # incomplete, continue reading
                        continue
                except socket.timeout:
                    print(f"Timeout receiving from {address}. Data so far: {b''.join(chunks)}")
                    # Try parsing what we have
                    try:
                        data = b''.join(chunks).decode('utf-8')
                        request = json.loads(data)
                        break
                    except json.JSONDecodeError:
                        raise Exception('Incomplete request data from client')
            client_socket.settimeout(None)
            
            if request is None:
                print(f"Warning: No valid JSON request received from {address}")
                return

            action = request.get('action')
            
            if action == 'get_url':
                print("DEBUG: Client requested get_url")
                # --- FETCHER: Request URL ---
                url_data = self.get_next_url()
                
                if url_data:
                    response = {
                        'status': 'ok',
                        'url_id': url_data['id'],
                        'url': url_data['url']
                    }
                else:
                    response = {'status': 'no_urls'}
                
                client_socket.sendall(json.dumps(response).encode('utf-8'))

                # We do NOT wait for the result here anymore. 
                # The fetcher will reconnect to submit the result.
                return

            elif action == 'submit_result':
                # --- FETCHER: Submit Result (Standalone) ---
                self._handle_submit_result(request, None, client_socket)

            elif action == 'get_fetched_url':
                # --- PARSER: Request Batch ---
                urls = self.get_next_fetched_batch()
                if urls:
                    response = {'status': 'ok', 'urls': urls}
                else:
                    response = {'status': 'no_urls'}
                
                # Send response with newline as parser expects readline()
                client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                
                if urls:
                    # Expect submit_parsed_result from parser
                    # The parser sends a result for EACH url in the batch sequentially over the same socket
                    # We loop until we receive results for all or socket closes
                    client_socket.settimeout(60.0) 
                    processed_count = 0
                    while processed_count < len(urls):
                        try:
                            # Read line-based JSON from parser
                            line = b''
                            while True:
                                part = client_socket.recv(1)
                                if part == b'\n' or part == b'':
                                    break
                                line += part
                            
                            if not line:
                                break
                                
                            result_req = json.loads(line.decode('utf-8'))
                            if result_req.get('action') == 'submit_parsed_result':
                                self._handle_parsed_result(result_req)
                                # Send ACK
                                client_socket.sendall(b'ack\n')
                                processed_count += 1
                        except socket.timeout:
                            break
                        except Exception as e:
                            print(f"Error receiving parser results: {e}")
                            break

            elif action == 'submit_parsed_result':
                 # --- PARSER: Submit Result (Standalone) ---
                 self._handle_parsed_result(request)
                 client_socket.sendall(b'ack\n')
            
            elif action == 'get_tunebook':
                # --- INDEXER: Request tunebook ---
                tunebook_id = self.get_next_tunebook()
                if tunebook_id:
                    response = {'status': 'ok', 'tunebook_id': tunebook_id}
                else:
                    response = {'status': 'empty'}
                client_socket.sendall(json.dumps(response).encode('utf-8'))
            
            elif action == 'submit_indexed_result':
                # --- INDEXER: Submit indexing result ---
                tunebook_id = request.get('tunebook_id')
                success = request.get('success', True)
                
                if tunebook_id is None:
                    response = {'status': 'error', 'message': 'Missing tunebook_id'}
                else:
                    self.mark_tunebook_indexed(tunebook_id, success)
                    response = {'status': 'ok'}
                client_socket.sendall(json.dumps(response).encode('utf-8'))


        except Exception as e:
            print(f"Error handling client request: {e}")
            try:
                error_response = {'status': 'error', 'message': str(e)}
                client_socket.sendall(json.dumps(error_response).encode('utf-8'))
            except:
                pass
        finally:
            client_socket.close()
    
    def _handle_parsed_result(self, request):
        url_id = request.get('url_id')
        has_abc = request.get('has_abc', False)
        
        if not url_id:
            return

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('''
                UPDATE urls 
                SET status = 'parsed', has_abc = ?, dispatched_at = NULL 
                WHERE id = ?
            ''', (1 if has_abc else 0, url_id))
            conn.commit()
            conn.close()
            print(f"Marked URL {url_id} as parsed (has_abc={has_abc})")
        except Exception as e:
            print(f"Error marking URL {url_id} parsed: {e}")

    def _handle_fetcher_timeout(self, url_data):
        if not url_data: 
            return
        url_id = url_data['id']
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # Increment retries
            cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = ?', (url_id,))
            conn.commit()
            
            # Check retries
            cur.execute('SELECT retries FROM urls WHERE id = ?', (url_id,))
            retries = cur.fetchone()[0]
            
            if retries >= 3:
                cur.execute("UPDATE urls SET status = 'error', dispatched_at = NULL WHERE id = ?", (url_id,))
            else:
                cur.execute("UPDATE urls SET status = '', dispatched_at = NULL WHERE id = ?", (url_id,))
            
            conn.commit()
            conn.close()
            
            # Disable host
            self._disable_host_timeout(url_data['url'])
            
        except Exception as e:
            print(f"Error handling fetcher timeout cleanup: {e}")

    def _disable_host_timeout(self, url):
        try:
            host = self._get_host(url)
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('INSERT OR IGNORE INTO hosts (host, last_access, last_http_status, disabled) VALUES (?, NULL, NULL, 0)', (host,))
            cur.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP WHERE host = ?', ('timeout', host))
            conn.commit()
            conn.close()
            print(f"Host {host} disabled due to timeout")
        except Exception as e:
            print(f"Error disabling host {host}: {e}")

    def _handle_submit_result(self, request, url_data, client_socket):
        # Fetcher submits result 
        # (This logic extracted from original giant method for clarity/reuse)
        url_id = request.get('url_id') or (url_data['id'] if url_data else None)
        if not url_id: return

        size_bytes = request.get('size_bytes', 0)
        mime_type = request.get('mime_type', '')
        document_b64 = request.get('document', '')
        http_status = request.get('http_status')
        error_type = request.get('error_type')

        print(f"Received result for URL id={url_id}, status={http_status}, err={error_type}")
        
        if document_b64:
            try:
                document = base64.b64decode(document_b64)
            except Exception:
                document = b''
        else:
            document = b''

        # Determine success vs failure
        if http_status is None or (isinstance(http_status, int) and http_status >= 400) or error_type:
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
                    try:
                        cur.execute("UPDATE urls SET status = '', http_status = ?, dispatched_at = NULL WHERE id = ?", (http_status, url_id))
                        conn.commit()
                    except Exception:
                        pass
                    print(f"URL id={url_id} failed, retrying (count {retries})")
            except Exception as e:
                print(f"Error updating retries: {e}")
            finally:
                conn.close()

            # Reply OK to fetcher so it continues
            try:
                client_socket.sendall(json.dumps({'status': 'ok'}).encode('utf-8'))
            except: pass

            # Host disabling logic for network/dns/timeout
            try:
                conn2 = get_db_connection()
                cur2 = conn2.cursor()
                cur2.execute('SELECT url FROM urls WHERE id = ?', (url_id,))
                r = cur2.fetchone()
                if r:
                    host = self._get_host(r[0])
                    cur2.execute('INSERT OR IGNORE INTO hosts (host) VALUES (?)', (host,))
                    
                    if error_type in ('timeout', 'dns') or http_status is None:
                         reason = error_type if error_type else 'timeout'
                         print(f"Marking host {host} disabled ({reason})")
                         cur2.execute('UPDATE hosts SET disabled = 1, disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP WHERE host = ?', (reason, host))
                    else:
                         cur2.execute('UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = ? WHERE host = ?', (http_status, host))
                    conn2.commit()
            except Exception as e:
                print(f"Warning updating host: {e}")
            finally:
                conn2.close()

        else:
            # Success
            self.mark_url_fetched(url_id, size_bytes, mime_type, document, http_status)
            try:
                 client_socket.sendall(json.dumps({'status': 'ok'}).encode('utf-8'))
            except: pass
    
    def run(self):
        print(f"Dispatcher started (PID: {os.getpid()})")
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
                    t = threading.Thread(target=self.handle_client_request, args=(client_socket, address), daemon=True)
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

