from flask import Flask, render_template, jsonify, request
import sqlite3
import os
import numpy as np
from database import get_db_connection
from vector_index import VectorIndex
from dtaidistance import dtw

app = Flask(__name__)
v_index = VectorIndex()

@app.route('/')
def index():
    return render_template('abc_index.html')

@app.route('/api/search', methods=['GET'])
def search_tunes():
    """Metadata-based tune search"""
    query = request.args.get('q', '').strip()
    title = request.args.get('title', '').strip()
    key = request.args.get('key', '').strip()
    rhythm = request.args.get('rhythm', '').strip()
    composer = request.args.get('composer', '').strip()
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = '''
            SELECT t.id, t.title, t.key, t.rhythm, t.composer, tb.url, t.tune_body
            FROM tunes t
            JOIN tunebooks tb ON t.tunebook_id = tb.id
            WHERE 1=1
        '''
        params = []
        
        if query:
            search_conditions = ['t.title LIKE ?', 't.composer LIKE ?', 't.notes LIKE ?']
            search_params = [f'%{query}%', f'%{query}%', f'%{query}%']
            
            if query.isdigit():
                search_conditions.append('t.id = ?')
                search_params.append(query)
                
            sql += ' AND (' + ' OR '.join(search_conditions) + ')'
            params += search_params
            
        if title:
            sql += ' AND t.title LIKE ?'
            params.append(f'%{title}%')
            
        if key:
            sql += ' AND t.key LIKE ?'
            params.append(f'%{key}%')
            
        if rhythm:
            sql += ' AND t.rhythm LIKE ?'
            params.append(f'%{rhythm}%')
            
        if composer:
            sql += ' AND t.composer LIKE ?'
            params.append(f'%{composer}%')
            
        # Get total count for pagination
        count_sql = f"SELECT COUNT(*) FROM ({sql})"
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]
        
        # Get results
        sql += ' ORDER BY t.title ASC LIMIT ? OFFSET ?'
        params += [limit, offset]
        
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                'id': row[0],
                'title': row[1],
                'key': row[2],
                'rhythm': row[3],
                'composer': row[4],
                'url': row[5],
                'abc': row[6]
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

@app.route('/api/tune/<int:tune_id>')
def get_tune(tune_id):
    """Get full tune details (including reconstructed ABC)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Fetch ALL columns to reconstruct full ABC
        cursor.execute('''
            SELECT 
                t.reference_number, t.title, t.composer, t.rhythm, t.key,
                t.meter, t.unit_note_length, t.tempo, t.parts, t.transcription,
                t.notes, t.history, t.origin, t.area, t.book, t.discography,
                t.source, t.instruction, t.tune_body, tb.url
            FROM tunes t
            JOIN tunebooks tb ON t.tunebook_id = tb.id
            WHERE t.id = ?
        ''', (tune_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            # Reconstruct ABC Headers
            abc_headers = []
            if row[0]: abc_headers.append(f"X:{row[0]}")
            if row[1]: abc_headers.append(f"T:{row[1]}")
            if row[2]: abc_headers.append(f"C:{row[2]}")
            if row[3]: abc_headers.append(f"R:{row[3]}")
            if row[5]: abc_headers.append(f"M:{row[5]}")
            if row[6]: abc_headers.append(f"L:{row[6]}")
            if row[7]: abc_headers.append(f"Q:{row[7]}")
            if row[8]: abc_headers.append(f"P:{row[8]}")
            if row[9]: abc_headers.append(f"Z:{row[9]}")
            if row[10]: abc_headers.append(f"N:{row[10]}")
            if row[11]: abc_headers.append(f"H:{row[11]}")
            if row[12]: abc_headers.append(f"O:{row[12]}")
            if row[13]: abc_headers.append(f"A:{row[13]}")
            if row[14]: abc_headers.append(f"B:{row[14]}")
            if row[15]: abc_headers.append(f"D:{row[15]}")
            if row[16]: abc_headers.append(f"S:{row[16]}")
            if row[17]: abc_headers.append(f"I:{row[17]}")
            if row[4]: abc_headers.append(f"K:{row[4]}")
            
            full_abc = "\n".join(abc_headers) + "\n" + row[18]

            return jsonify({
                'title': row[1],
                'key': row[4],
                'rhythm': row[3],
                'composer': row[2],
                'url': row[19],
                'abc': full_abc,
                'reference': row[0],
                'history': row[11],
                'notes': row[10]
            })
        return jsonify({'error': 'Tune not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def rerank_with_dtw(query_intervals, candidates, database_intervals):
    """
    Rerank candidates using Dynamic Time Warping
    """
    scored = []
    for tune_id in candidates:
        if tune_id not in database_intervals:
            continue
        candidate_intervals = database_intervals[tune_id]
        
        # dtaidistance requires numpy arrays
        try:
            d = dtw.distance(
                np.array(query_intervals, dtype=np.float64),
                np.array(candidate_intervals, dtype=np.float64),
                window=5
            )
            scored.append((tune_id, d))
        except Exception as e:
            print(f"DTW error for tune {tune_id}: {e}")
            continue
            
    return sorted(scored, key=lambda x: x[1])

@app.route('/api/tune/<int:tune_id>/similar')
def get_similar_tunes(tune_id):
    """Find similar tunes using FAISS preselection and DTW reranking"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Get query tune intervals
        cursor.execute('SELECT intervals FROM tunes WHERE id = ?', (tune_id,))
        row = cursor.fetchone()
        if not row or not row[0]:
            conn.close()
            return jsonify({'error': 'Query tune has no intervals indexed'}), 400
        
        query_intervals = [float(x) for x in row[0].split(',') if x.strip()]
        
        # 2. FAISS Preselection (top 100)
        # Convert to fixed length vector (the index uses 32 dimensions)
        q_vec = np.zeros(32, dtype=np.float32)
        for i, val in enumerate(query_intervals[:32]):
            q_vec[i] = val
        
        faiss_results = v_index.search(q_vec, k=100)
        candidate_ids = [r['tune_id'] for r in faiss_results if r['tune_id'] != tune_id]
        
        if not candidate_ids:
            conn.close()
            return jsonify({'results': []})
            
        # 3. Fetch intervals for candidates
        placeholders = ', '.join(['?'] * len(candidate_ids))
        cursor.execute(f'''
            SELECT id, title, key, rhythm, composer, intervals 
            FROM tunes 
            WHERE id IN ({placeholders})
        ''', candidate_ids)
        
        candidate_rows = cursor.fetchall()
        db_intervals = {}
        tune_meta = {}
        
        for r in candidate_rows:
            tid = r[0]
            if r[5]:
                db_intervals[tid] = [float(x) for x in r[5].split(',') if x.strip()]
            tune_meta[tid] = {
                'id': tid,
                'title': r[1],
                'key': r[2],
                'rhythm': r[3],
                'composer': r[4]
            }
            
        # 4. Rerank with DTW
        reranked = rerank_with_dtw(query_intervals, candidate_ids, db_intervals)
        
        # 5. Take top 10
        final_results = []
        for tid, dist in reranked[:10]:
            meta = tune_meta[tid]
            meta['similarity_score'] = round(dist, 4)
            final_results.append(meta)
            
        conn.close()
        return jsonify({'results': final_results})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5501)
