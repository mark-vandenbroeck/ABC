from flask import Flask, render_template, jsonify, request
import psycopg2
import re
from collections import defaultdict
import os
import json
import threading
import time
import numpy as np
from database_pg import get_db_connection
from vector_index import VectorIndex
from dtaidistance import dtw

app = Flask(__name__)
v_index = VectorIndex()

@app.route('/')
def index():
    return render_template('abc_index.html')

@app.route('/help')
def help_page():
    lang = request.args.get('lang', 'nl')
    if lang == 'en':
        return render_template('help_en.html')
    return render_template('help_nl.html')

def _reduce_rhythms(raw_rhythms):
    """
    Groups rhythms by normalized form.
    Prioritizes mappings defined in config/rhythm_aliases.json.
    Fallback to automatic normalization (lowercase, alpha-numeric only).
    """
    def normalize(s):
        if not s: return ""
        return re.sub(r'[^a-z0-9]', '', s.lower())

    # Load Aliases
    aliases = {}
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'rhythm_aliases.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                aliases = json.load(f)
        except Exception as e:
            print(f"Error loading rhythm aliases: {e}")

    variation_to_canonical = {}
    for canonical, variations in aliases.items():
        for v in variations:
            variation_to_canonical[normalize(v)] = canonical
            variation_to_canonical[normalize(canonical)] = canonical

    groups = defaultdict(list)
    
    for r in raw_rhythms:
        norm = normalize(r)
        if not norm: continue
        
        if norm in variation_to_canonical:
            canonical = variation_to_canonical[norm]
            groups[canonical].append(r)
        else:
            groups[norm].append(r)
            
    display_names = []
    variation_map = {}
    
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)
    
    for key, variants in sorted_groups:
        if key in aliases:
            display_name = key
        else:
            candidates = sorted(variants, key=lambda x: (len(x), x))
            display_name = candidates[0]
            title_case = [v for v in candidates if v and v[0].isupper()]
            if title_case:
                display_name = title_case[0]
            
        display_names.append(display_name)
        variation_map[display_name] = variants
        
    return sorted(display_names), variation_map

def _reduce_keys(raw_keys):
    """
    Groups keys by normalized form.
    Prioritizes mappings defined in config/key_aliases.json.
    """
    aliases = {}
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'key_aliases.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                aliases = json.load(f)
        except Exception as e:
            print(f"Error loading key aliases: {e}")

    variation_to_canonical = {}
    for canonical, variations in aliases.items():
        for v in variations:
            variation_to_canonical[v] = canonical
            
    groups = defaultdict(list)
    
    for k in raw_keys:
        if not k: continue
        if k in variation_to_canonical:
             groups[variation_to_canonical[k]].append(k)
             continue
             
        groups[k.strip()].append(k)
            
    display_names = []
    variation_map = {}
    
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)
    
    for key, variants in sorted_groups:
        display_names.append(key)
        variation_map[key] = variants
        
    return sorted(display_names), variation_map

@app.route('/api/filters')
def get_filters():
    """Get unique keys and rhythms for the UI dropdowns"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT key FROM tunes WHERE key IS NOT NULL AND key != '' ORDER BY key ASC")
        raw_keys = [row['key'] for row in cursor.fetchall()]
        keys, _ = _reduce_keys(raw_keys)
        
        cursor.execute("SELECT DISTINCT rhythm FROM tunes WHERE rhythm IS NOT NULL AND rhythm != '' ORDER BY rhythm ASC")
        raw_rhythms = [row['rhythm'] for row in cursor.fetchall()]
        
        rhythms, _ = _reduce_rhythms(raw_rhythms)

        cursor.execute("SELECT DISTINCT meter FROM tunes WHERE meter IS NOT NULL AND meter != '' ORDER BY meter ASC")
        meters = [row['meter'] for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'keys': keys,
            'rhythms': rhythms,
            'meters': meters
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search', methods=['GET'])
def search_tunes():
    """Metadata-based tune search"""
    query = request.args.get('q', '').strip()
    title = request.args.get('title', '').strip()
    key = request.args.get('key', '').strip()
    rhythm = request.args.get('rhythm', '').strip()
    meter = request.args.get('meter', '').strip()
    composer = request.args.get('composer', '').strip()
    mode = request.args.get('mode', '').strip().lower()
    user_id = request.args.get('user_id', '').strip()
    favorites_only = request.args.get('favorites_only', 'false').lower() == 'true'
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = '''
            SELECT t.id, t.title, t.key, t.rhythm, t.composer, tb.url, t.tune_body, t.status, t.skip_reason, t.meter
            FROM tunes t
            JOIN tunebooks tb ON t.tunebook_id = tb.id
        '''
        params = []
        
        if favorites_only and user_id:
            sql += ' JOIN user_favorites uf ON t.id = uf.tune_id AND uf.user_id = %s '
            params.append(user_id)
            
        sql += ' WHERE 1=1 '
        
        if query:
            # PostgreSQL ILIKE for case-insensitive search
            search_conditions = [
                't.title ILIKE %s', 
                't.composer ILIKE %s', 
                't.notes ILIKE %s',
                't.transcription ILIKE %s',
                't."group" ILIKE %s',
                't.history ILIKE %s',
                't.source ILIKE %s'
            ]
            search_params = [f'%{query}%'] * 7
            
            if query.isdigit():
                search_conditions.append('t.id = %s')
                search_params.append(int(query))
                
            sql += ' AND (' + ' OR '.join(search_conditions) + ')'
            params += search_params
            
        if title:
            sql += ' AND t.title ILIKE %s'
            params.append(f'%{title}%')
            
        if key:
            cursor.execute("SELECT DISTINCT key FROM tunes WHERE key IS NOT NULL AND key != ''")
            raw_keys_db = [row['key'] for row in cursor.fetchall()]
            _, key_map = _reduce_keys(raw_keys_db)
            
            if key in key_map:
                variations = key_map[key]
                placeholders = ', '.join(['%s'] * len(variations))
                sql += f' AND t.key IN ({placeholders})'
                params.extend(variations)
            else:
                sql += ' AND t.key = %s'
                params.append(key)
                
        if mode:
            # Mode filtering logic (ILIKES already case insensitive in PG, but keeping logic)
            if mode == 'major':
                sql += """ AND (
                    (t.key ILIKE '%Maj%' OR t.key ILIKE '%Ion%')
                    OR 
                    (t.key NOT ILIKE '%m%' 
                     AND t.key NOT ILIKE '%dor%'
                     AND t.key NOT ILIKE '%mix%'
                     AND t.key NOT ILIKE '%phr%'
                     AND t.key NOT ILIKE '%lyd%'
                     AND t.key NOT ILIKE '%loc%'
                     AND t.key NOT ILIKE '%aeo%'
                    )
                )"""
            elif mode == 'minor':
                sql += """ AND (
                    t.key ILIKE '%Min%' 
                    OR t.key ILIKE '%Aeo%'
                    OR (t.key ILIKE '%m%' AND t.key NOT ILIKE '%maj%'
                        AND t.key NOT ILIKE '%mix%'
                        AND t.key NOT ILIKE '%lyd%' 
                       )
                )"""
            elif mode == 'dorian':
                sql += " AND (t.key ILIKE '%Dor%')"
            elif mode == 'mixolydian':
                sql += " AND (t.key ILIKE '%Mix%')"
            elif mode == 'lydian':
                sql += " AND (t.key ILIKE '%Lyd%') AND (t.key NOT ILIKE '%Mix%')"
            elif mode == 'phrygian':
                sql += " AND (t.key ILIKE '%Phr%')"
            elif mode == 'locrian':
                sql += " AND (t.key ILIKE '%Loc%')"

        if rhythm:
            cursor.execute("SELECT DISTINCT rhythm FROM tunes WHERE rhythm IS NOT NULL AND rhythm != ''")
            raw_rhythms_db = [row['rhythm'] for row in cursor.fetchall()]
            _, rhythm_map = _reduce_rhythms(raw_rhythms_db)
            
            if rhythm in rhythm_map:
                variations = rhythm_map[rhythm]
                placeholders = ', '.join(['%s'] * len(variations))
                sql += f' AND t.rhythm IN ({placeholders})'
                params.extend(variations)
            else:
                sql += ' AND t.rhythm = %s'
                params.append(rhythm)

        if meter:
            sql += ' AND t.meter = %s'
            params.append(meter)
            
        if composer:
            sql += ' AND t.composer ILIKE %s'
            params.append(f'%{composer}%')

        ids_filter = request.args.get('ids', '').strip()
        if ids_filter:
            try:
                id_list = [int(i) for i in ids_filter.split(',') if i.strip()]
                if id_list:
                    placeholders = ', '.join(['%s'] * len(id_list))
                    sql += f' AND t.id IN ({placeholders})'
                    params.extend(id_list)
            except ValueError:
                pass

        status_filter = request.args.get('status', '').strip()
        if status_filter:
            sql += ' AND t.status = %s'
            params.append(status_filter)
        elif not ids_filter:
            sql += " AND t.status = 'parsed'"
            
        # Get total count for pagination
        count_sql = f"SELECT COUNT(*) as count FROM ({sql}) as sub"
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()['count']
        
        # Get results
        sql += ' ORDER BY t.title ASC LIMIT %s OFFSET %s'
        params += [limit, offset]
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                'id': row['id'],
                'title': row['title'],
                'key': row['key'],
                'rhythm': row['rhythm'],
                'composer': row['composer'],
                'url': row['url'],
                # 'tune_body': row['tune_body'],
                'status': row['status'],
                'skip_reason': row['skip_reason'],
                'meter': row['meter']
            })
            
        conn.close()
        return jsonify({
            'results': results,
            'total': total_count,
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/favorites/<user_id>', methods=['GET'])
def get_user_favorites(user_id):
    """Get list of favorite tune IDs for a user"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT tune_id FROM user_favorites WHERE user_id = %s", (user_id,))
        favorites = [row['tune_id'] for row in cursor.fetchall()]
        conn.close()
        return jsonify(favorites)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/favorites/add', methods=['POST'])
def add_favorite():
    """Add a tune to favorites"""
    try:
        data = request.json
        user_id = data.get('user_id')
        tune_id = data.get('tune_id')
        
        if not user_id or not tune_id:
            return jsonify({'error': 'Missing user_id or tune_id'}), 400
            
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO user_favorites (user_id, tune_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, tune_id)
            )
            conn.commit()
            return jsonify({'status': 'added'})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/favorites/remove', methods=['POST'])
def remove_favorite():
    """Remove a tune from favorites"""
    try:
        data = request.json
        user_id = data.get('user_id')
        tune_id = data.get('tune_id')
        
        if not user_id or not tune_id:
            return jsonify({'error': 'Missing user_id or tune_id'}), 400
            
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM user_favorites WHERE user_id = %s AND tune_id = %s",
                (user_id, tune_id)
            )
            conn.commit()
            return jsonify({'status': 'removed'})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tune/<int:tune_id>')
def get_tune(tune_id):
    """Get full tune details (including reconstructed ABC)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Fetch ALL columns to reconstruct full ABC
        # Note: referencing columns by name in row dict
        cursor.execute('''
            SELECT 
                t.reference_number, t.title, t.composer, t.rhythm, t.key,
                t.meter, t.unit_note_length, t.tempo, t.parts, t.transcription,
                t.notes, t.history, t.origin, t.area, t.book, t.discography,
                t.source, t.instruction, t.tune_body, tb.url, t.status, t.skip_reason, t."group"
            FROM tunes t
            JOIN tunebooks tb ON t.tunebook_id = tb.id
            WHERE t.id = %s
        ''', (tune_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # Reconstruct ABC Headers
            abc_headers = []
            if row['reference_number']: abc_headers.append(f"X:{row['reference_number']}")
            if row['title']: abc_headers.append(f"T:{row['title']}")
            if row['composer']: abc_headers.append(f"C:{row['composer']}")
            if row['rhythm']: abc_headers.append(f"R:{row['rhythm']}")
            if row['meter']: abc_headers.append(f"M:{row['meter']}")
            if row['unit_note_length']: abc_headers.append(f"L:{row['unit_note_length']}")
            if row['tempo']: abc_headers.append(f"Q:{row['tempo']}")
            if row['parts']: abc_headers.append(f"P:{row['parts']}")
            if row['transcription']: abc_headers.append(f"Z:{row['transcription']}")
            if row['notes']: abc_headers.append(f"N:{row['notes']}")
            if row['history']: abc_headers.append(f"H:{row['history']}")
            if row['origin']: abc_headers.append(f"O:{row['origin']}")
            if row['area']: abc_headers.append(f"A:{row['area']}")
            if row['book']: abc_headers.append(f"B:{row['book']}")
            if row['discography']: abc_headers.append(f"D:{row['discography']}")
            if row['source']: abc_headers.append(f"S:{row['source']}")
            if row['instruction']: abc_headers.append(f"I:{row['instruction']}")
            if row['key']: abc_headers.append(f"K:{row['key']}")
            
            full_abc = "\n".join(abc_headers) + "\n" + row['tune_body']

            return jsonify({
                'title': row['title'],
                'key': row['key'],
                'rhythm': row['rhythm'],
                'composer': row['composer'],
                'url': row['url'],
                'abc': full_abc,
                'reference': row['reference_number'],
                'history': row['history'],
                'source': row['source'],
                'notes': row['notes'],
                'transcription': row['transcription'],
                'group': row['group'],
                'status': row['status'],
                'skip_reason': row['skip_reason'],
                'meter': row['meter'],
                'tempo': row['tempo']
            })
        return jsonify({'error': 'Tune not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def rerank_with_dtw(query_intervals, candidates, database_intervals, faiss_distances=None):
    """
    Rerank candidates using normalized DTW (Dynamic Time Warping).
    """
    scored = []
    q_len = len(query_intervals)
    
    for tune_id in candidates:
        if tune_id not in database_intervals:
            continue
        candidate_intervals = database_intervals[tune_id]
        
        try:
            d = dtw.distance(
                np.array(query_intervals, dtype=np.float64),
                np.array(candidate_intervals, dtype=np.float64),
                window=10 
            )
            norm_dtw = d / q_len
            scored.append((tune_id, norm_dtw))
            
        except Exception as e:
            print(f"DTW error for tune {tune_id}: {e}")
            continue
            
    return sorted(scored, key=lambda x: x[1])

@app.route('/api/tune/<int:tune_id>/similar')
def get_similar_tunes(tune_id):
    """Find similar tunes using expanded FAISS preselection and DTW reranking"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Get query tune intervals
        cursor.execute('SELECT intervals FROM tunes WHERE id = %s', (tune_id,))
        row = cursor.fetchone()
        
        # In PG, intervals is a float array, retrieved as a Python list of floats.
        if not row or row['intervals'] is None:
            conn.close()
            return jsonify({'error': 'Query tune has no intervals indexed'}), 400
        
        # No need to split(',') for PostgreSQL array
        query_intervals = row['intervals']
        
        # 2. FAISS Preselection
        faiss_candidates = v_index.get_candidates(query_intervals, k=1000, exclude_id=tune_id)
        candidate_ids = [r['tune_id'] for r in faiss_candidates]
        
        if not candidate_ids:
            conn.close()
            return jsonify({'results': []})
            
        # 3. Fetch intervals for candidates
        placeholders = ', '.join(['%s'] * len(candidate_ids))
        cursor.execute(f'''
            SELECT id, title, key, rhythm, composer, intervals 
            FROM tunes 
            WHERE id IN ({placeholders})
        ''', candidate_ids)
        
        candidate_rows = cursor.fetchall()
        db_intervals = {}
        tune_meta = {}
        
        for r in candidate_rows:
            tid = r['id']
            if r['intervals']:
                # No split(',') needed
                db_intervals[tid] = r['intervals']
            tune_meta[tid] = {
                'id': tid,
                'title': r['title'],
                'key': r['key'],
                'rhythm': r['rhythm'],
                'composer': r['composer']
            }
            
        # 4. Rerank with DTW
        reranked = rerank_with_dtw(query_intervals, candidate_ids, db_intervals, faiss_distances=faiss_candidates)
        
        # 5. Take top 10
        final_results = []
        for tid, dist in reranked[:10]:
            if tid in tune_meta:
                meta = tune_meta[tid]
                meta['similarity_score'] = round(dist, 4)
                final_results.append(meta)
            
        conn.close()
        return jsonify({'results': final_results})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def sync_faiss_loop():
    """
    Background worker to periodically sync new tunes to FAISS index.
    Checks for tunes that have intervals but are not yet in the index.
    """
    print("FAISS Sync Worker started")
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Find tunes that have intervals but are NOT in the FAISS index
            cursor.execute('''
                SELECT id, intervals 
                FROM tunes 
                WHERE intervals IS NOT NULL 
                AND id NOT IN (SELECT tune_id FROM faiss_mapping)
                LIMIT 1000
            ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            if rows:
                print(f"Sync Worker: Found {len(rows)} new tunes to index")
                tune_ids = []
                vectors = []
                
                for row in rows:
                    try:
                        # Intervals is already a list of floats in PG
                        vals = row['intervals']
                        
                        # Generate windows
                        windows = VectorIndex.generate_windows(vals)
                        
                        for w in windows:
                            tune_ids.append(row['id'])
                            vectors.append(w)
                            
                    except ValueError:
                        continue
                
                if tune_ids:
                    # add_vectors handles the DB mapping insert + FAISS save
                    v_index.add_vectors(tune_ids, np.array(vectors))
                    print(f"Sync Worker: Successfully indexed {len(tune_ids)} vectors (from {len(rows)} tunes)")
                        
            time.sleep(30)
            
        except Exception as e:
            print(f"Sync Worker error: {e}")
            time.sleep(30)

if __name__ == '__main__':
    # Start background sync thread can be uncommented if needed, sticking to original pattern
    # sync_thread = threading.Thread(target=sync_faiss_loop, daemon=True)
    # sync_thread.start()
    
    app.run(debug=False, host='0.0.0.0', port=5501)
