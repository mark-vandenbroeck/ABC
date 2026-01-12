import socket
import psycopg2
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
import threading
import traceback
import logging

# Import PostgreSQL connection logic
from database_pg import get_db_connection

DISPATCHER_HOST = 'localhost'
DISPATCHER_PORT = 8888

def dump_stack_trace(sig, frame):
    print("\n--- STACK TRACE ---")
    message = "\n".join(traceback.format_stack(frame))
    print(message)
    print("-------------------\n")

signal.signal(signal.SIGUSR1, dump_stack_trace)

class URLDispatcher:
    def __init__(self):
        self.running = True
        self.server_socket = None
        self.connected_fetchers = []
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Ensure DB is initialized (migrations)
        # In PG version, we assume schema is managed via SQL scripts, not init_database()

        # Log scanner state
        self._log_pos = 0
        self._log_thread = threading.Thread(target=self._log_scanner_loop, daemon=True)
        self._log_thread.start()

        # Host re-enable thread
        self._host_thread = threading.Thread(target=self._host_reenable_loop, daemon=True)
        self._host_thread.start()

        # Release stale URLs on startup
        self._reset_stale_urls()
        self._reenable_timeout_hosts()

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
        try:
            cur = conn.cursor()
            cur.execute('SELECT last_access FROM hosts WHERE host = %s', (host,))
            row = cur.fetchone()
            if not row or not row['last_access']:
                return True
            
            # PostgreSQL returns datetime objects for timestamps
            last_access = row['last_access']
            # Ensure utc comparison
            if last_access.tzinfo:
                # If offset-aware, compare with aware 'now'. If 'last_access' is offset naive, assume local/utc
                # Python's datetime.utcnow() is naive. Our DB is TIMESTAMP WITH TIME ZONE.
                # Simplest: compare both as naive or aware.
                # Psycopg2 returns aware datetime if setup correctly.
                now = datetime.now(last_access.tzinfo)
            else:
                now = datetime.now()

            return (now - last_access) >= timedelta(seconds=cooldown_seconds)
        finally:
            conn.close()

    def _log_scanner_loop(self, interval_seconds=60):
        """Periodically scan the fetcher log for DNS/NameResolution errors and mark hosts disabled with a reason."""
        # Logs are now relative to the script location
        log_path = Path(__file__).resolve().parent / 'logs' / 'fetcher.log'
        pattern = re.compile(r"Failed to resolve '([^']+)'", re.IGNORECASE)
        
        while self.running:
            try:
                # In PG version, we might have multiple fetcher logs (fetcher.1.log, etc)
                # But typically they might all log to consistent files or we scan all of them.
                # For simplicity, we assume fetcher_out.log or we might need to scan all fetcher.*.log?
                # The original code scanned 'fetcher.log'.
                # Let's try to scan 'logs/fetcher_out.log' if app_pg redirects stdout there, 
                # OR scan specific fetcher logs.
                # Implementation choice: Scan 'fetcher_out.log' (stdout capture) or individually.
                # Assuming app_pg.py redirects stdout to fetcher_out.log, let's look there first.
                target_log = Path(__file__).resolve().parent / 'logs' / 'fetcher_out.log'
                
                if not target_log.exists():
                     # Fallback to scanning individual logs? Too complex for now, try one file.
                     pass 

                if not target_log.exists():
                    time.sleep(interval_seconds)
                    continue

                # Check if file rotated (size < current pos)
                try:
                    current_size = target_log.stat().st_size
                    if current_size < self._log_pos:
                        self._log_pos = 0
                except:
                    pass

                with target_log.open('r', encoding='utf-8', errors='replace') as fh:
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
                                # PostgreSQL syntax
                                cur.execute("""
                                    INSERT INTO hosts (host, last_access, last_http_status, downloads, disabled, disabled_reason, disabled_at) 
                                    VALUES (%s, NULL, NULL, 0, TRUE, %s, CURRENT_TIMESTAMP)
                                    ON CONFLICT (host) DO NOTHING
                                """, (host, 'dns'))
                                cur.execute("""
                                    UPDATE hosts 
                                    SET disabled = TRUE, disabled_reason = %s, disabled_at = CURRENT_TIMESTAMP 
                                    WHERE host = %s
                                """, ('dns', host))
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

    def _host_reenable_loop(self, interval_seconds=600):
        """Periodically check for hosts that can be re-enabled."""
        while self.running:
            self._reenable_timeout_hosts()
            time.sleep(interval_seconds)

    def _reenable_timeout_hosts(self):
        """Re-enable hosts that were disabled due to timeout > 24 hours ago."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # PostgreSQL interval syntax
            cur.execute("""
                UPDATE hosts 
                SET disabled = FALSE, disabled_reason = NULL, disabled_at = NULL
                WHERE disabled = TRUE 
                AND disabled_reason = 'timeout' 
                AND disabled_at <= NOW() - INTERVAL '24 hours'
            """)
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                print(f"Re-enabled {count} hosts (previously disabled due to timeout)")
        except Exception as e:
            print(f"Error re-enabling timeout hosts: {e}")

    def _reset_stale_urls(self, timeout_seconds=300):
        """Release URLs that were stuck in dispatched/parsing/indexing state from a previous session."""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # PostgreSQL interval syntax
            cur.execute(f'''
                UPDATE urls 
                SET status = '', dispatched_at = NULL 
                WHERE (status = 'dispatched' OR status = 'parsing' OR status = 'indexing')
                AND (dispatched_at IS NULL OR dispatched_at <= NOW() - INTERVAL '{timeout_seconds} seconds')
            ''')
            count = cur.rowcount
            conn.commit()
            conn.close()
            if count > 0:
                print(f"Recovered {count} stale URLs on startup")
        except Exception as e:
            print(f"Error resetting stale URLs: {e}")

    def get_next_url(self, batch_size=100, dispatch_timeout_seconds=120, host_cooldown_seconds=30):
        """Get the next URL to process."""
        conn = get_db_connection()
        cursor = conn.cursor() # RealDictCursor

        # Use row locking (FOR UPDATE SKIP LOCKED) to handle concurrency safely in PostgreSQL
        try:
            # PostgreSQL approach:
            # Select candidate URLs that are available and whose hosts are not on cooldown.
            # We can use a CTE or just a complex query with FOR UPDATE SKIP LOCKED.
            
            # Note: Checking host cooldown in the same query is tricky because we need to check 'hosts' table.
            # We can join but locking gets complicated.
            # Simplified approach: fetch candidates, then try to lock and claim one.
            
            # Using SKIP LOCKED is the best way to avoid race conditions between multiple dispatchers
            # (though here we only have one dispatcher, but multiple threads? No, dispatcher is single process/single thread for logic)
            # Actually, dispatcher is single threaded logic (handle_client_request runs in threads).
            # So we DO have concurrency.
            
            query = f'''
                WITH candidates AS (
                    SELECT u.id, u.url, u.host, u.link_distance
                    FROM urls u
                    LEFT JOIN hosts h ON u.host = h.host
                    WHERE (u.status = '' OR (u.status = 'dispatched' AND u.dispatched_at <= NOW() - INTERVAL '{dispatch_timeout_seconds} seconds'))
                      AND (u.retries IS NULL OR u.retries < 3)
                      AND (h.disabled IS NULL OR h.disabled = FALSE)
                      AND (h.last_access IS NULL OR h.last_access <= NOW() - INTERVAL '{host_cooldown_seconds} seconds')
                    ORDER BY (u.url LIKE '%.abc') DESC, u.created_at ASC
                    LIMIT {batch_size}
                )
                SELECT * FROM candidates
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            '''
            # Wait, FOR UPDATE SKIP LOCKED on the CTE result ? Not directly.
            # We need to lock the rows in the main table.
            
            # Better Query for Postgres:
            # UPDATE ... RETURNING ...
            # But we need to check the host cooldown which is a different table.
            
            # Let's stick to the Select, then Update pattern with transaction isolation or explicit locking.
            # Or just optimistic locking.
            
            cursor.execute(f'''
                SELECT u.id, u.url, u.host, COALESCE(u.link_distance, 0) as link_distance
                FROM urls u
                LEFT JOIN hosts h ON u.host = h.host
                WHERE (u.status = '' OR (u.status = 'dispatched' AND u.dispatched_at <= NOW() - INTERVAL '{dispatch_timeout_seconds} seconds'))
                  AND (u.retries IS NULL OR u.retries < 3)
                  AND (h.disabled IS NULL OR h.disabled = FALSE)
                  AND (h.last_access IS NULL OR h.last_access <= NOW() - INTERVAL '{host_cooldown_seconds} seconds')
                ORDER BY (u.url LIKE '%%.abc') DESC, u.created_at ASC
                LIMIT {batch_size}
            ''')
            
            candidates = cursor.fetchall()
            
            for row in candidates:
                url_id = row['id']
                url = row['url']
                host = row['host']
                dist = row['link_distance']
                
                # Try to atomically claim
                cursor.execute('''
                    UPDATE urls
                    SET status = 'dispatched', dispatched_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND (status = '' OR status = 'dispatched')
                ''', (url_id,))
                
                if cursor.rowcount == 1:
                    # Successfully claimed
                    if host:
                        try:
                            # Update host last access
                            cursor.execute("""
                                INSERT INTO hosts (host, last_access, last_http_status, downloads) 
                                VALUES (%s, CURRENT_TIMESTAMP, NULL, 0)
                                ON CONFLICT (host) DO UPDATE SET last_access = CURRENT_TIMESTAMP
                            """, (host,))
                        except Exception as e2:
                            print(f"Warning: could not reserve host {host} on dispatch: {e2}")
                    
                    conn.commit()
                    return {'id': url_id, 'url': url, 'link_distance': dist}
            
            conn.commit()
            return None
            
        except Exception as e:
            print(f"Error getting next URL: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def mark_url_fetched(self, url_id, size_bytes, mime_type, document, http_status=None):
        """Mark a URL as fetched in the database and reset retries; update host downloads and status"""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE urls 
                SET downloaded_at = CURRENT_TIMESTAMP,
                    size_bytes = %s,
                    mime_type = %s,
                    document = %s,
                    http_status = %s,
                    retries = 0,
                    status = 'fetched',
                    dispatched_at = NULL
                WHERE id = %s
            ''', (size_bytes, mime_type, document, http_status, url_id))
            
            # Update hosts table
            cursor.execute('SELECT url FROM urls WHERE id = %s', (url_id,))
            r = cursor.fetchone()
            if r:
                # RealDictCursor return dict
                url_val = r['url']
                host = self._get_host(url_val)

                if host:
                    cursor.execute("""
                        INSERT INTO hosts (host, last_access, last_http_status, downloads) 
                        VALUES (%s, NULL, NULL, 0)
                        ON CONFLICT (host) DO NOTHING
                    """, (host,))
                    
                    cursor.execute("""
                        UPDATE hosts 
                        SET last_access = CURRENT_TIMESTAMP, 
                            last_http_status = %s, 
                            downloads = COALESCE(downloads, 0) + 1 
                        WHERE host = %s
                    """, (http_status, host))
            
            conn.commit()
        except Exception as e:
            print(f"Warning: could not update host record in mark_url_fetched: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    
    def get_next_fetched_batch(self, batch_size=50, dispatch_timeout_seconds=300):
        """Get a batch of URLs that have been fetched but not yet parsed."""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            
            # PostgreSQL syntax
            # Select IDs
            cursor.execute(f'''
                SELECT id, url 
                FROM urls 
                WHERE (status = 'fetched') 
                   OR (status = 'parsing' AND (dispatched_at IS NULL OR dispatched_at <= NOW() - INTERVAL '{dispatch_timeout_seconds} seconds'))
                LIMIT %s
            ''', (batch_size,))
            
            rows = cursor.fetchall()
            if not rows:
                conn.commit()
                return []
                
            ids = [row['id'] for row in rows] # RealDictCursor
            
            if not ids:
                return []
                
            # Update status to 'parsing'
            cursor.execute('''
                UPDATE urls 
                SET status = 'parsing', dispatched_at = CURRENT_TIMESTAMP 
                WHERE id = ANY(%s)
            ''', (ids,))
            
            conn.commit()
            
            return [{'id': row['id'], 'url': row['url']} for row in rows]
            
        except Exception as e:
            print(f"Error getting fetched batch: {e}")
            conn.rollback()
            return []
        finally:
            conn.close()
    
    def get_next_tunebook(self, dispatch_timeout_seconds=300):
        """Get the next tunebook that needs indexing (status = '')."""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT id
                FROM tunebooks
                WHERE status = ''
                   OR (status = 'indexing' AND (dispatched_at IS NULL OR dispatched_at <= NOW() - INTERVAL '{dispatch_timeout_seconds} seconds'))
                ORDER BY created_at ASC
                LIMIT 1
            ''')
            
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None
            
            tunebook_id = row['id']
            
            # Mark as indexing
            cursor.execute('''
                UPDATE tunebooks
                SET status = 'indexing', dispatched_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (tunebook_id,))
            
            conn.commit()
            return tunebook_id
            
        except Exception as e:
            print(f"Error getting next tunebook: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def mark_tunebook_indexed(self, tunebook_id, success=True):
        """Mark a tunebook as indexed"""
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            status = 'indexed' if success else 'error'
            cursor.execute('''
                UPDATE tunebooks
                SET status = %s
                WHERE id = %s
            ''', (status, tunebook_id))
            
            if success:
                # Synchronize status to the main urls table
                # Use subquery for update
                cursor.execute('''
                    UPDATE urls
                    SET status = 'indexed'
                    WHERE url = (SELECT url FROM tunebooks WHERE id = %s)
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
                        continue
                except socket.timeout:
                    # Try parsing what we have
                    try:
                        data = b''.join(chunks).decode('utf-8')
                        request = json.loads(data)
                        break
                    except json.JSONDecodeError:
                        raise Exception('Incomplete request data from client')
            client_socket.settimeout(None)
            
            if request is None:
                # print(f"Warning: No valid JSON request received from {address}")
                return

            action = request.get('action')
            
            if action == 'get_url':
                # --- FETCHER: Request URL ---
                url_data = self.get_next_url()
                
                if url_data:
                    response = {
                        'status': 'ok',
                        'url_id': url_data['id'],
                        'url': url_data['url'],
                        'link_distance': url_data.get('link_distance', 0)
                    }
                else:
                    response = {'status': 'no_urls'}
                
                client_socket.sendall(json.dumps(response).encode('utf-8'))
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
                
                client_socket.sendall((json.dumps(response) + '\n').encode('utf-8'))
                
                if urls:
                    # Expect submit_parsed_result from parser
                    client_socket.settimeout(60.0) 
                    processed_count = 0
                    while processed_count < len(urls):
                        try:
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

        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE urls 
                SET status = 'parsed', has_abc = %s, dispatched_at = NULL 
                WHERE id = %s
            """, (has_abc, url_id))
            conn.commit()
            print(f"Marked URL {url_id} as parsed (has_abc={has_abc})")
        except Exception as e:
            print(f"Error marking URL {url_id} parsed: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _handle_submit_result(self, request, url_data, client_socket):
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
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                cur.execute('UPDATE urls SET retries = COALESCE(retries,0) + 1 WHERE id = %s', (url_id,))
                
                # We need to fetch the count after update
                # Or use UPDATE ... RETURNING retries
                # Since get_db_connection returns a wrapper or native conn, let's assume standard behavior + rowcount
                cur.execute('SELECT retries FROM urls WHERE id = %s', (url_id,))
                row = cur.fetchone()
                retries = row['retries'] if row else 0 # RealDictCursor
                
                if retries >= 3:
                     # Mark error
                    cur.execute("""
                        UPDATE urls SET status = 'error', downloaded_at = CURRENT_TIMESTAMP, http_status = %s, dispatched_at = NULL 
                        WHERE id = %s
                    """, (http_status, url_id))
                    print(f"URL id={url_id} marked error after {retries} retries")
                else:
                    cur.execute("""
                        UPDATE urls SET status = '', http_status = %s, dispatched_at = NULL 
                        WHERE id = %s
                    """, (http_status, url_id))
                    print(f"URL id={url_id} failed, retrying ({retries})")
                
                conn.commit()
            except Exception as e:
                print(f"Error updating retries: {e}")
                conn.rollback()
            finally:
                conn.close()

            # Reply OK
            try:
                client_socket.sendall(json.dumps({'status': 'ok'}).encode('utf-8'))
            except: pass

            # Host disabling logic
            try:
                conn2 = get_db_connection()
                cur2 = conn2.cursor()
                cur2.execute('SELECT url FROM urls WHERE id = %s', (url_id,))
                r = cur2.fetchone()
                if r:
                    host = self._get_host(r['url'])
                    cur2.execute("""
                        INSERT INTO hosts (host) VALUES (%s)
                        ON CONFLICT (host) DO NOTHING
                    """, (host,))
                    
                    if error_type in ('timeout', 'dns') or http_status is None:
                         reason = error_type if error_type else 'timeout'
                         print(f"Marking host {host} disabled ({reason})")
                         cur2.execute("""
                            UPDATE hosts 
                            SET disabled = TRUE, disabled_reason = %s, disabled_at = CURRENT_TIMESTAMP, last_access = CURRENT_TIMESTAMP 
                            WHERE host = %s
                        """, (reason, host))
                    else:
                         cur2.execute("UPDATE hosts SET last_access = CURRENT_TIMESTAMP, last_http_status = %s WHERE host = %s", (http_status, host))
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
            
            while self.running:
                try:
                    self.server_socket.settimeout(1.0)
                    client_socket, address = self.server_socket.accept()
                    # print(f"Fetcher connected from {address}")
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
