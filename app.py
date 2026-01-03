from flask import Flask, render_template, jsonify, request, Response
import sqlite3
import subprocess
import os
import signal
import json
import threading
import time
import logging
import math
from database import get_db_connection, DB_PATH, init_database
from log_rotator import RotatingFileWriter

app = Flask(__name__)

# Store process PIDs
processes = {
    'dispatcher': None,
    'purger': None,
    'fetchers': {},
    'parsers': {},
    'indexers': {}
}

def get_process_info():
    """Get information about running processes"""
    info = {
        'dispatcher': None,
        'purger': None,
        'fetchers': [],
        'parsers': [],
        'indexers': []
    }
    
    # Check dispatcher
    def _read_pidfile(path):
        try:
            with open(path, 'r') as f:
                return int(f.read().strip())
        except Exception:
            return None

    if processes['dispatcher']:
        pid = processes['dispatcher']
        try:
            os.kill(pid, 0)  # Check if process exists
            info['dispatcher'] = {'pid': pid, 'status': 'running'}
        except OSError:
            info['dispatcher'] = {'pid': pid, 'status': 'stopped'}
            processes['dispatcher'] = None
    else:
        # If we don't have an in-memory PID, check for a PID file created by start scripts
        pidfile = os.path.join(os.getcwd(), 'run', 'dispatcher.pid')
        pid_from_file = _read_pidfile(pidfile)
        if pid_from_file:
            try:
                os.kill(pid_from_file, 0)
                processes['dispatcher'] = pid_from_file
                info['dispatcher'] = {'pid': pid_from_file, 'status': 'running'}
            except OSError:
                # Stale PID file; remove it
                try:
                    os.remove(pidfile)
                except Exception:
                    pass
                info['dispatcher'] = {'pid': pid_from_file, 'status': 'stopped'}
    
    # Check purger
    if processes['purger']:
        pid = processes['purger']
        try:
            os.kill(pid, 0)
            info['purger'] = {'pid': pid, 'status': 'running'}
        except OSError:
            info['purger'] = {'pid': pid, 'status': 'stopped'}
            processes['purger'] = None
    else:
        pidfile = os.path.join(os.getcwd(), 'run', 'purger.pid')
        pid_from_file = _read_pidfile(pidfile)
        if pid_from_file:
            try:
                os.kill(pid_from_file, 0)
                processes['purger'] = pid_from_file
                info['purger'] = {'pid': pid_from_file, 'status': 'running'}
            except OSError:
                try:
                    os.remove(pidfile)
                except Exception:
                    pass
                info['purger'] = {'pid': pid_from_file, 'status': 'stopped'}

    # Check fetchers
    for fetcher_id, pid in list(processes['fetchers'].items()):
        try:
            os.kill(pid, 0)
            info['fetchers'].append({'id': fetcher_id, 'pid': pid, 'status': 'running'})
        except OSError:
            info['fetchers'].append({'id': fetcher_id, 'pid': pid, 'status': 'stopped'})
            del processes['fetchers'][fetcher_id]
            # Try to remove pidfile
            try: os.remove(os.path.join('run', f'fetcher.{fetcher_id}.pid'))
            except: pass
    else:
        # Check run/ directory for any fetcher pidfiles we might have missed
        if os.path.exists('run'):
            for f in os.listdir('run'):
                if f.startswith('fetcher.') and f.endswith('.pid'):
                    try:
                        parts = f.split('.')
                        if len(parts) >= 3:
                            fetcher_id = parts[1]
                            if fetcher_id not in processes['fetchers']:
                                pid_from_file = _read_pidfile(os.path.join('run', f))
                                if pid_from_file:
                                    try:
                                        os.kill(pid_from_file, 0)
                                        processes['fetchers'][fetcher_id] = pid_from_file
                                        info['fetchers'].append({'id': fetcher_id, 'pid': pid_from_file, 'status': 'running'})
                                    except OSError:
                                        try: os.remove(os.path.join('run', f))
                                        except: pass
                    except: pass
    
    # Check parsers
    for parser_id, pid in list(processes['parsers'].items()):
        try:
            os.kill(pid, 0)
            info['parsers'].append({'id': parser_id, 'pid': pid, 'status': 'running'})
        except OSError:
            info['parsers'].append({'id': parser_id, 'pid': pid, 'status': 'stopped'})
            del processes['parsers'][parser_id]
            try: os.remove(os.path.join('run', f'parser.{parser_id}.pid'))
            except: pass
    else:
        # Check run/ directory for any parser pidfiles we might have missed
        if os.path.exists('run'):
            for f in os.listdir('run'):
                if f.startswith('parser.') and f.endswith('.pid'):
                    parser_id = f.split('.')[1]
                    if parser_id not in processes['parsers']:
                        pid_from_file = _read_pidfile(os.path.join('run', f))
                        if pid_from_file:
                            try:
                                os.kill(pid_from_file, 0)
                                processes['parsers'][parser_id] = pid_from_file
                                info['parsers'].append({'id': parser_id, 'pid': pid_from_file, 'status': 'running'})
                            except OSError:
                                try: os.remove(os.path.join('run', f))
                                except: pass

    # Check indexers
    for indexer_id, pid in list(processes['indexers'].items()):
        try:
            os.kill(pid, 0)
            info['indexers'].append({'id': indexer_id, 'pid': pid, 'status': 'running'})
        except OSError:
            info['indexers'].append({'id': indexer_id, 'pid': pid, 'status': 'stopped'})
            del processes['indexers'][indexer_id]
            try: os.remove(os.path.join('run', f'indexer.{indexer_id}.pid'))
            except: pass
    else:
        # Check run/ directory for any indexer pidfiles we might have missed
        if os.path.exists('run'):
            for f in os.listdir('run'):
                if f.startswith('indexer.') and f.endswith('.pid'):
                    indexer_id = f.split('.')[1]
                    if indexer_id not in processes['indexers']:
                        pid_from_file = _read_pidfile(os.path.join('run', f))
                        if pid_from_file:
                            try:
                                os.kill(pid_from_file, 0)
                                processes['indexers'][indexer_id] = pid_from_file
                                info['indexers'].append({'id': indexer_id, 'pid': pid_from_file, 'status': 'running'})
                            except OSError:
                                try: os.remove(os.path.join('run', f))
                                except: pass


    # Sync with database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM processes")
        
        # Add dispatcher
        if info['dispatcher'] and info['dispatcher']['status'] == 'running':
            cursor.execute("INSERT INTO processes (pid, type, status) VALUES (?, ?, ?)", 
                         (info['dispatcher']['pid'], 'dispatcher', 'running'))
        
        # Add purger
        if info['purger'] and info['purger']['status'] == 'running':
            cursor.execute("INSERT INTO processes (pid, type, status) VALUES (?, ?, ?)", 
                         (info['purger']['pid'], 'purger', 'running'))
            
        # Add fetchers
        for f in info['fetchers']:
            if f['status'] == 'running':
                cursor.execute("INSERT INTO processes (pid, type, status) VALUES (?, ?, ?)", 
                             (f['pid'], f'fetcher:{f["id"]}', 'running'))
                
        # Add parsers
        for p in info['parsers']:
            if p['status'] == 'running':
                cursor.execute("INSERT INTO processes (pid, type, status) VALUES (?, ?, ?)", 
                             (p['pid'], f'parser:{p["id"]}', 'running'))
                
        # Add indexers
        for i in info['indexers']:
            if i['status'] == 'running':
                cursor.execute("INSERT INTO processes (pid, type, status) VALUES (?, ?, ?)", 
                             (i['pid'], f'indexer:{i["id"]}', 'running'))
                
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error syncing processes table: {e}")

    return info

@app.route('/')
def index():
    """Main dashboard"""
    return render_template('index.html')

@app.route('/api/processes', methods=['GET'])
def get_processes():
    """Get status of all processes"""
    return jsonify(get_process_info())

@app.route('/api/logs/stream/<filename>')
def stream_log(filename):
    """Stream log file contents via SSE"""
    # Security check: only allow known log files and prevent directory traversal
    import re
    is_valid = False
    
    # Static list
    allowed_static = [
        'fetcher_out.log', 'parser_out.log', 'indexer_out.log', 
        'purger.log', 'purger_error.log', 'dispatcher.log'
    ]
    if filename in allowed_static:
        is_valid = True
    # Pattern-based (fetcher.1.log, parser.1.log, etc)
    elif re.match(r'^(fetcher|parser|indexer)\.\d+\.log$', filename):
        is_valid = True
    elif filename in ['fetcher.log', 'parser.log', 'indexer.log']: # support legacy paths if any
        is_valid = True

    if not is_valid:
        return jsonify({'status': 'error', 'message': 'Forbidden log file'}), 403
    
    log_path = os.path.join('logs', filename)
    if not os.path.exists(log_path):
        os.makedirs('logs', exist_ok=True)
        with open(log_path, 'a'): pass

    def generate():
        # Yield an initial comment to open the connection immediately
        yield ": connection started\n\n"
        
        with open(log_path, 'r', errors='replace') as f:
            # Start near the end
            f.seek(0, os.SEEK_END)
            curr_size = f.tell()
            # Seek back 2000 chars roughly
            f.seek(max(0, curr_size - 5000), os.SEEK_SET)
            
            # Skip the first partial line if we seeked into middle
            if f.tell() > 0:
                f.readline()
                
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.5)
                    continue
                yield f"data: {json.dumps(line)}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/processes/dispatcher/start', methods=['POST'])
def start_dispatcher():
    """Start the URL dispatcher"""
    if processes['dispatcher']:
        try:
            os.kill(processes['dispatcher'], 0)
            return jsonify({'status': 'error', 'message': 'Dispatcher already running'}), 400
        except OSError:
            pass  # Process doesn't exist, continue
    
    try:
        os.makedirs('logs', exist_ok=True)
        log_file = open('logs/dispatcher.log', 'a')
        log_file_err = open('logs/dispatcher_error.log', 'a')
        proc = subprocess.Popen(['python', '-u', 'url_dispatcher.py'], 
                              stdout=log_file, 
                              stderr=log_file_err)
        processes['dispatcher'] = proc.pid
        # Write pidfile so external scripts can see/coordinate with this process
        try:
            os.makedirs('run', exist_ok=True)
            with open(os.path.join('run', 'dispatcher.pid'), 'w') as f:
                f.write(str(proc.pid))
        except Exception:
            pass
        return jsonify({'status': 'ok', 'pid': proc.pid})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/dispatcher/stop', methods=['POST'])
def stop_dispatcher():
    """Stop the URL dispatcher"""
    if not processes['dispatcher']:
        return jsonify({'status': 'error', 'message': 'Dispatcher not running'}), 400
    
    try:
        os.kill(processes['dispatcher'], signal.SIGTERM)
        # Clean up pidfile if it was created
        try:
            pidfile = os.path.join(os.getcwd(), 'run', 'dispatcher.pid')
            if os.path.exists(pidfile):
                os.remove(pidfile)
        except Exception:
            pass
        processes['dispatcher'] = None
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/purger/start', methods=['POST'])
def start_purger():
    """Start the URL purger"""
    if processes['purger']:
        try:
            os.kill(processes['purger'], 0)
            return jsonify({'status': 'error', 'message': 'Purger already running'}), 400
        except OSError:
            pass
    
    try:
        os.makedirs('logs', exist_ok=True)
        log_file = RotatingFileWriter('logs/purger_error.log', max_bytes=3145728, backup_count=4)
        proc = subprocess.Popen(['python', '-u', 'url_purger.py'],
                              stdout=log_file,
                              stderr=log_file)
        processes['purger'] = proc.pid
        try:
            os.makedirs('run', exist_ok=True)
            with open(os.path.join('run', 'purger.pid'), 'w') as f:
                f.write(str(proc.pid))
        except Exception:
            pass
        return jsonify({'status': 'ok', 'pid': proc.pid})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/purger/stop', methods=['POST'])
def stop_purger():
    """Stop the URL purger"""
    if not processes['purger']:
        return jsonify({'status': 'error', 'message': 'Purger not running'}), 400
    
    try:
        os.kill(processes['purger'], signal.SIGTERM)
        try:
            pidfile = os.path.join(os.getcwd(), 'run', 'purger.pid')
            if os.path.exists(pidfile):
                os.remove(pidfile)
        except Exception:
            pass
        processes['purger'] = None
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/fetcher/add', methods=['POST'])
def add_fetcher():
    """Add a new fetcher process"""
    data = request.json or {}
    fetcher_id = data.get('id') or str(len(processes['fetchers']) + 1)
    
    if fetcher_id in processes['fetchers']:
        try:
            os.kill(processes['fetchers'][fetcher_id], 0)
            return jsonify({'status': 'error', 'message': f'Fetcher {fetcher_id} already running'}), 400
        except OSError:
            pass
    
    try:
        os.makedirs('logs', exist_ok=True)
        log_file = RotatingFileWriter('logs/fetcher_out.log', max_bytes=3145728, backup_count=4)
        proc = subprocess.Popen(['python', '-u', 'url_fetcher.py', fetcher_id],
                              stdout=log_file,
                              stderr=log_file)
        processes['fetchers'][fetcher_id] = proc.pid
        # Write pidfile for fetcher
        try:
            os.makedirs('run', exist_ok=True)
            with open(os.path.join('run', f'fetcher.{fetcher_id}.pid'), 'w') as f:
                f.write(str(proc.pid))
        except Exception:
            pass
        return jsonify({'status': 'ok', 'id': fetcher_id, 'pid': proc.pid})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/fetcher/<fetcher_id>/remove', methods=['POST'])
def remove_fetcher(fetcher_id):
    """Remove a fetcher process"""
    if fetcher_id not in processes['fetchers']:
        return jsonify({'status': 'error', 'message': f'Fetcher {fetcher_id} not found'}), 404
    
    try:
        os.kill(processes['fetchers'][fetcher_id], signal.SIGTERM)
        # Remove pidfile if present
        try:
            pidfile = os.path.join(os.getcwd(), 'run', f'fetcher.{fetcher_id}.pid')
            if os.path.exists(pidfile):
                os.remove(pidfile)
        except Exception:
            pass
        del processes['fetchers'][fetcher_id]
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/parser/add', methods=['POST'])
def add_parser():
    """Add a new parser process"""
    data = request.json or {}
    parser_id = data.get('id') or str(len(processes['parsers']) + 1)
    
    if parser_id in processes['parsers']:
        try:
            os.kill(processes['parsers'][parser_id], 0)
            return jsonify({'status': 'error', 'message': f'Parser {parser_id} already running'}), 400
        except OSError:
            pass
    
    try:
        os.makedirs('logs', exist_ok=True)
        log_file = RotatingFileWriter('logs/parser_out.log', max_bytes=3145728, backup_count=4)
        proc = subprocess.Popen(['python', '-u', 'url_parser.py', parser_id],
                              stdout=log_file,
                              stderr=log_file)
        processes['parsers'][parser_id] = proc.pid
        # Write pidfile
        try:
            os.makedirs('run', exist_ok=True)
            with open(os.path.join('run', f'parser.{parser_id}.pid'), 'w') as f:
                f.write(str(proc.pid))
        except Exception:
            pass
        return jsonify({'status': 'ok', 'id': parser_id, 'pid': proc.pid})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/parser/<parser_id>/remove', methods=['POST'])
def remove_parser(parser_id):
    """Remove a parser process"""
    if parser_id not in processes['parsers']:
        return jsonify({'status': 'error', 'message': f'Parser {parser_id} not running'}), 400
    
    try:
        os.kill(processes['parsers'][parser_id], signal.SIGTERM)
        # Clean up pidfile
        try:
            pidfile = os.path.join(os.getcwd(), 'run', f'parser.{parser_id}.pid')
            if os.path.exists(pidfile):
                os.remove(pidfile)
        except Exception:
            pass
        del processes['parsers'][parser_id]
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/indexer/add', methods=['POST'])
def add_indexer():
    """Add a new indexer process"""
    data = request.json or {}
    indexer_id = data.get('id') or str(len(processes['indexers']) + 1)
    
    if indexer_id in processes['indexers']:
        try:
            os.kill(processes['indexers'][indexer_id], 0)
            return jsonify({'status': 'error', 'message': f'Indexer {indexer_id} already running'}), 400
        except OSError:
            pass
    
    try:
        os.makedirs('logs', exist_ok=True)
        log_file = RotatingFileWriter('logs/indexer_out.log', max_bytes=3145728, backup_count=4)
        proc = subprocess.Popen(['python', '-u', 'abc_indexer.py', indexer_id],
                              stdout=log_file,
                              stderr=log_file)
        processes['indexers'][indexer_id] = proc.pid
        # Write pidfile
        try:
            os.makedirs('run', exist_ok=True)
            with open(os.path.join('run', f'indexer.{indexer_id}.pid'), 'w') as f:
                f.write(str(proc.pid))
        except Exception:
            pass
        return jsonify({'status': 'ok', 'id': indexer_id, 'pid': proc.pid})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/processes/indexer/<indexer_id>/remove', methods=['POST'])
def remove_indexer(indexer_id):
    """Remove an indexer process"""
    if indexer_id not in processes['indexers']:
        return jsonify({'status': 'error', 'message': f'Indexer {indexer_id} not running'}), 400
    
    try:
        os.kill(processes['indexers'][indexer_id], signal.SIGTERM)
        # Clean up pidfile
        try:
            pidfile = os.path.join(os.getcwd(), 'run', f'indexer.{indexer_id}.pid')
            if os.path.exists(pidfile):
                os.remove(pidfile)
        except Exception:
            pass
        del processes['indexers'][indexer_id]
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/processes/stop-all', methods=['POST'])
def stop_all_processes():
    """Stop all running processes (dispatcher, purger, fetchers, parsers, indexers)"""
    stopped = []
    errors = []
    
    # Stop all fetchers
    for fetcher_id in list(processes['fetchers'].keys()):
        try:
            os.kill(processes['fetchers'][fetcher_id], signal.SIGTERM)
            try:
                pidfile = os.path.join(os.getcwd(), 'run', f'fetcher.{fetcher_id}.pid')
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except Exception:
                pass
            del processes['fetchers'][fetcher_id]
            stopped.append(f'fetcher-{fetcher_id}')
        except Exception as e:
            errors.append(f'fetcher-{fetcher_id}: {str(e)}')
    
    # Stop all parsers
    for parser_id in list(processes['parsers'].keys()):
        try:
            os.kill(processes['parsers'][parser_id], signal.SIGTERM)
            try:
                pidfile = os.path.join(os.getcwd(), 'run', f'parser.{parser_id}.pid')
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except Exception:
                pass
            del processes['parsers'][parser_id]
            stopped.append(f'parser-{parser_id}')
        except Exception as e:
            errors.append(f'parser-{parser_id}: {str(e)}')
    
    # Stop all indexers
    for indexer_id in list(processes['indexers'].keys()):
        try:
            os.kill(processes['indexers'][indexer_id], signal.SIGTERM)
            try:
                pidfile = os.path.join(os.getcwd(), 'run', f'indexer.{indexer_id}.pid')
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except Exception:
                pass
            del processes['indexers'][indexer_id]
            stopped.append(f'indexer-{indexer_id}')
        except Exception as e:
            errors.append(f'indexer-{indexer_id}: {str(e)}')
    
    # Stop purger
    if processes['purger']:
        try:
            os.kill(processes['purger'], signal.SIGTERM)
            try:
                pidfile = os.path.join(os.getcwd(), 'run', 'purger.pid')
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except Exception:
                pass
            processes['purger'] = None
            stopped.append('purger')
        except Exception as e:
            errors.append(f'purger: {str(e)}')
    
    # Stop dispatcher
    if processes['dispatcher']:
        try:
            os.kill(processes['dispatcher'], signal.SIGTERM)
            try:
                pidfile = os.path.join(os.getcwd(), 'run', 'dispatcher.pid')
                if os.path.exists(pidfile):
                    os.remove(pidfile)
            except Exception:
                pass
            processes['dispatcher'] = None
            stopped.append('dispatcher')
        except Exception as e:
            errors.append(f'dispatcher: {str(e)}')
    
    return jsonify({
        'status': 'ok',
        'stopped': stopped,
        'errors': errors if errors else None
    })

@app.route('/api/urls', methods=['GET'])
def get_urls():
    """Get URLs from the queue with optional filters (status, url wildcard, mime wildcard)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get query parameters
    status = request.args.get('status', '')
    url_filter = (request.args.get('url_filter') or '').strip()
    mime_filter = (request.args.get('mime_filter') or '').strip()
    has_abc = request.args.get('has_abc', '')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))

    query = 'SELECT id, url, created_at, downloaded_at, size_bytes, status, mime_type, http_status, retries, link_distance FROM urls WHERE 1=1'
    params = []

    if status:
        if status == 'new':
            query += ' AND (status = ? OR status IS NULL)'
            params.append('')
        else:
            query += ' AND status = ?'
            params.append(status)

    if has_abc == 'true':
        query += ' AND has_abc = 1'

    # URL filter: support simple wildcards using '*'
    if url_filter:
        pattern = url_filter.replace('*', '%')
        if '%' not in pattern and '_' not in pattern:
            pattern = f'%{pattern}%'
        query += ' AND url LIKE ?'
        params.append(pattern)

    # MIME filter: support wildcards too
    if mime_filter:
        mpat = mime_filter.replace('*', '%')
        if '%' not in mpat and '_' not in mpat:
            mpat = f'%{mpat}%'
        query += ' AND mime_type LIKE ?'
        params.append(mpat)

    query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()

    # Get total count with same filters
    count_query = 'SELECT COUNT(*) FROM urls WHERE 1=1'
    count_params = []

    if status:
        if status == 'new':
            count_query += ' AND (status = ? OR status IS NULL)'
            count_params.append('')
        else:
            count_query += ' AND status = ?'
            count_params.append(status)

    if has_abc == 'true':
        count_query += ' AND has_abc = 1'

    if url_filter:
        pattern = url_filter.replace('*', '%')
        if '%' not in pattern and '_' not in pattern:
            pattern = f'%{pattern}%'
        count_query += ' AND url LIKE ?'
        count_params.append(pattern)

    if mime_filter:
        mpat = mime_filter.replace('*', '%')
        if '%' not in mpat and '_' not in mpat:
            mpat = f'%{mpat}%'
        count_query += ' AND mime_type LIKE ?'
        count_params.append(mpat)

    cursor.execute(count_query, count_params)
    total = cursor.fetchone()[0]

    conn.close()

    urls = []
    for row in rows:
        urls.append({
            'id': row[0],
            'url': row[1],
            'created_at': row[2],
            'downloaded_at': row[3],
            'size_bytes': row[4],
            'status': row[5] or '',
            'mime_type': row[6] or '',
            'http_status': row[7],
            'retries': row[8] or 0,
            'link_distance': row[9] or 0
        })

    return jsonify({
        'urls': urls,
        'total': total,
        'limit': limit,
        'offset': offset
    })

@app.route('/api/urls', methods=['POST'])
def add_url():
    """Add a new URL to the queue"""
    data = request.json
    if not data or 'url' not in data:
        return jsonify({'status': 'error', 'message': 'URL required'}), 400
    
    url = data['url']
    
    # Reject URLs with refused filename extensions
    try:
        from urllib.parse import urlparse
        import os
        parsed = urlparse(url)
        path = parsed.path or ''
        _, ext = os.path.splitext(path)
        if ext:
            ext = ext.lstrip('.').lower()
            conn_check = get_db_connection(); cur_check = conn_check.cursor()
            cur_check.execute('SELECT 1 FROM refused_extensions WHERE extension = ?', (ext,))
            if cur_check.fetchone():
                conn_check.close()
                return jsonify({'status': 'error', 'message': f'URLs with extension .{ext} are refused'}), 400
            conn_check.close()
    except Exception:
        pass

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from urllib.parse import urlparse
        host = None
        try:
            host = urlparse(url).hostname
        except Exception:
            host = None

        if host:
            cursor.execute('INSERT OR IGNORE INTO urls (url, host) VALUES (?, ?)', (url, host))
        else:
            cursor.execute('INSERT OR IGNORE INTO urls (url) VALUES (?)', (url,))
        conn.commit()
        
        if cursor.rowcount > 0:
            return jsonify({'status': 'ok', 'message': 'URL added'})
        else:
            return jsonify({'status': 'error', 'message': 'URL already exists'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/urls/<int:url_id>', methods=['DELETE'])
def delete_url(url_id):
    """Delete a URL from the queue by ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM urls WHERE id = ?', (url_id,))
        conn.commit()
        if cursor.rowcount > 0:
            return jsonify({'status': 'ok', 'message': 'URL deleted'})
        else:
            return jsonify({'status': 'error', 'message': 'URL not found'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/mime-types', methods=['GET'])
def get_mime_types():
    """Get configured MIME types"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT id, pattern, enabled FROM mime_types ORDER BY pattern')
    rows = cursor.fetchall()
    conn.close()
    
    mime_types = []
    for row in rows:
        mime_types.append({
            'id': row[0],
            'pattern': row[1],
            'enabled': bool(row[2])
        })
    
    return jsonify({'mime_types': mime_types})


# --- Refused filename extensions API ---
@app.route('/api/refused-extensions', methods=['GET'])
def get_refused_extensions():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT extension, reason, created_at FROM refused_extensions ORDER BY extension')
    rows = cur.fetchall()
    conn.close()
    exts = [{'extension': r[0], 'reason': r[1], 'created_at': r[2]} for r in rows]
    return jsonify({'refused_extensions': exts})

@app.route('/api/refused-extensions', methods=['POST'])
def add_refused_extension():
    data = request.json or {}
    ext = (data.get('extension') or '').strip().lstrip('.').lower()
    reason = data.get('reason')
    if not ext:
        return jsonify({'status': 'error', 'message': 'extension required'}), 400
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('INSERT OR IGNORE INTO refused_extensions (extension, reason) VALUES (?, ?)', (ext, reason))
        conn.commit()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/refused-extensions/<string:extension>', methods=['DELETE'])
def delete_refused_extension(extension):
    ext = extension.strip().lstrip('.').lower()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM refused_extensions WHERE extension = ?', (ext,))
        conn.commit()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/mime-types', methods=['POST'])
def add_mime_type():
    """Add a new MIME type pattern"""
    data = request.json
    if not data or 'pattern' not in data:
        return jsonify({'status': 'error', 'message': 'Pattern required'}), 400
    
    pattern = data['pattern']
    enabled = data.get('enabled', True)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('INSERT OR IGNORE INTO mime_types (pattern, enabled) VALUES (?, ?)',
                      (pattern, 1 if enabled else 0))
        conn.commit()
        
        if cursor.rowcount > 0:
            return jsonify({'status': 'ok', 'message': 'MIME type added'})
        else:
            return jsonify({'status': 'error', 'message': 'Pattern already exists'}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/mime-types/<int:mime_id>', methods=['PUT'])
def update_mime_type(mime_id):
    """Update a MIME type"""
    data = request.json
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    updates = []
    params = []
    
    if 'pattern' in data:
        updates.append('pattern = ?')
        params.append(data['pattern'])
    
    if 'enabled' in data:
        updates.append('enabled = ?')
        params.append(1 if data['enabled'] else 0)
    
    if not updates:
        return jsonify({'status': 'error', 'message': 'No fields to update'}), 400
    
    params.append(mime_id)
    query = f"UPDATE mime_types SET {', '.join(updates)} WHERE id = ?"
    
    try:
        cursor.execute(query, params)
        conn.commit()
        
        if cursor.rowcount > 0:
            return jsonify({'status': 'ok', 'message': 'MIME type updated'})
        else:
            return jsonify({'status': 'error', 'message': 'MIME type not found'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/mime-types/<int:mime_id>', methods=['DELETE'])
def delete_mime_type(mime_id):
    """Delete a MIME type"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM mime_types WHERE id = ?', (mime_id,))
        conn.commit()
        
        if cursor.rowcount > 0:
            return jsonify({'status': 'ok', 'message': 'MIME type deleted'})
        else:
            return jsonify({'status': 'error', 'message': 'MIME type not found'}), 404
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/hosts', methods=['GET'])
def get_hosts():
    """Get hosts table contents with optional filters"""
    conn = get_db_connection()
    cursor = conn.cursor()

    host_filter = request.args.get('host_filter', '').strip()
    status = request.args.get('status', '').strip()
    reason = request.args.get('reason', '').strip()

    query = 'SELECT host, last_access, last_http_status, downloads, disabled, disabled_reason, disabled_at FROM hosts WHERE 1=1'
    params = []

    if host_filter:
        pattern = host_filter.replace('*', '%')
        if '%' not in pattern and '_' not in pattern:
            pattern = f'%{pattern}%'
        query += ' AND host LIKE ?'
        params.append(pattern)

    if status:
        # Exact match for status, but handle if user types partial
        try:
            status_int = int(status)
            query += ' AND last_http_status = ?'
            params.append(status_int)
        except ValueError:
            pass # Ignore invalid status

    if reason:
        pattern = reason.replace('*', '%')
        if '%' not in pattern and '_' not in pattern:
            pattern = f'%{pattern}%'
        query += ' AND disabled_reason LIKE ?'
        params.append(pattern)

    query += ' ORDER BY host'
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    hosts = []
    for row in rows:
        hosts.append({
            'host': row[0],
            'last_access': row[1],
            'last_http_status': row[2],
            'downloads': row[3] or 0,
            'disabled': bool(row[4]) if row[4] is not None else False,
            'disabled_reason': row[5],
            'disabled_at': row[6]
        })

    return jsonify({'hosts': hosts})


@app.route('/api/hosts/<path:host>', methods=['PUT'])
def update_host(host):
    """Update host record, e.g., clear disabled flag"""
    try:
        payload = request.get_json() or {}
        if 'disabled' not in payload:
            return jsonify({'status': 'error', 'message': 'missing disabled field'}), 400
        disabled = 1 if payload.get('disabled') else 0
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('INSERT OR IGNORE INTO hosts (host) VALUES (?)', (host,))

        if disabled:
            # Keep any existing disabled_reason but record last_access
            cur.execute('UPDATE hosts SET disabled = 1, last_access = CURRENT_TIMESTAMP WHERE host = ?', (host,))
            print(f"API: set host {host} disabled via API")
        else:
            # Clearing disabled: remove reason/timestamp
            cur.execute('UPDATE hosts SET disabled = 0, disabled_reason = NULL, disabled_at = NULL, last_access = CURRENT_TIMESTAMP WHERE host = ?', (host,))
            print(f"API: cleared disabled flag for host {host}")

        conn.commit()
        conn.close()
        return jsonify({'status': 'ok'})
    except Exception as e:
        print(f"API: error updating host {host}: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# Simple cache for stats
_stats_cache = {'data': None, 'timestamp': 0}
_stats_cache_duration = 60 # seconds

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get crawler statistics with simple caching"""
    global _stats_cache
    
    current_time = time.time()
    if _stats_cache['data'] and (current_time - _stats_cache['timestamp'] < _stats_cache_duration):
        return jsonify(_stats_cache['data'])

    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        stats = {}
        
        # Total URLs
        cursor.execute('SELECT COUNT(*) FROM urls')
        stats['total_urls'] = cursor.fetchone()[0]
        
        # URLs by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM urls 
            GROUP BY status
        ''')
        stats['by_status'] = {row[0] or 'new': row[1] for row in cursor.fetchall()}
        
        # Total size
        cursor.execute('SELECT SUM(size_bytes) FROM urls WHERE size_bytes IS NOT NULL')
        result = cursor.fetchone()[0]
        stats['total_size_bytes'] = result or 0
        
        # MIME types distribution
        cursor.execute('''
            SELECT mime_type, COUNT(*) 
            FROM urls 
            WHERE mime_type IS NOT NULL AND mime_type != ''
            GROUP BY mime_type
            ORDER BY COUNT(*) DESC
            LIMIT 10
        ''')
        stats['top_mime_types'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # New metrics
        cursor.execute("SELECT COUNT(*) FROM urls WHERE status = 'parsed'")
        stats['total_parsed'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM urls WHERE has_abc = 1")
        stats['total_with_abc'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tunebooks")
        stats['total_tunebooks'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tunes")
        stats['total_tunes'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tunebooks WHERE status = 'indexed'")
        stats['total_indexed_tunebooks'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tunes WHERE status = 'parsed' AND intervals IS NULL")
        stats['total_pending_index_tunes'] = cursor.fetchone()[0]

        try:
            cursor.execute("SELECT COUNT(*) FROM faiss_mapping")
            stats['faiss_index_size'] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            stats['faiss_index_size'] = 0

        # Link Distance vs Status breakdown
        cursor.execute('''
            SELECT link_distance, status, COUNT(*) 
            FROM urls 
            GROUP BY link_distance, status
        ''')
        breakdown = {}
        for dist, status, count in cursor.fetchall():
            dist = dist if dist is not None else 0
            status = status if status else 'new'
            if dist not in breakdown:
                breakdown[dist] = {}
            breakdown[dist][status] = count
        stats['distance_status_breakdown'] = breakdown

        _stats_cache = {'data': stats, 'timestamp': current_time}
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()
    
    return jsonify(stats)

if __name__ == '__main__':
    # Ensure database and schema are initialized/up-to-date
    init_database()
    
    app.run(debug=False, host='0.0.0.0', port=5500, threaded=True)
