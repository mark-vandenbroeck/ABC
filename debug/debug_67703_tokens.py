import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from abc_parser import Tune
from database import get_db_connection
import re

def debug_67703():
    tune_id = 67703
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT tune_body FROM tunes WHERE id = ?', (tune_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        print("Tune not found")
        return
        
    body = row[0]
    print(f"--- FULL BODY ---\n{body}\n-----------------")
    
    # Simulate internal tokenization
    token_pattern = r'''
            "[^"]+"                # Quoted strings (chord symbols, etc.)
            |[A-Z]:\s*[^ \n|]*      # Inline headers (K:, P:, L:, etc.)
            |\[[\w\s,]+\]           # Chords [CEG]
            |[_^=]?[a-gA-G]        # Note with optional accidental
            |z                     # Rest
            |[0-9]+(?:/[0-9]*)?    # Duration (2, /2, 3/2, etc.)
            |/                     # Duration shorthand
            |'                     # Octave up
            |,                     # Octave down
            |[()]                  # Ties
            |x                     # Invisible rest
            |~                     # Trill
            |![\w]+!               # Ornament/articulation
            |\|:                   # Repeat bar
            |:\|                   # Repeat bar
            |\|                    # Bar line
            |::                    # Double bar
            |\[[\d\|.,]+\]         # Alternate ending
        '''
    
    tokens = re.findall(token_pattern, body, re.VERBOSE)
    print(f"\nTokens found: {tokens[:20]}...")
    
    notes = []
    for t in tokens:
        if t.startswith('"') or (len(t) > 1 and t[1] == ':'):
             continue
        if re.match(r'^[_^=]?[a-gA-G]', t):
             notes.append(t)
    
    print(f"\nNotes extracted: {notes[:30]}")

if __name__ == "__main__":
    debug_67703()
