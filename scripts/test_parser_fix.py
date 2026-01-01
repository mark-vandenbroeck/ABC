import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from abc_parser import Tune
from database import get_db_connection

def test_parser_fix():
    tune_id = 67703
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT tune_body FROM tunes WHERE id = ?', (tune_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        print("Tune not found")
        return
        
    abc = "X:1\nT:Test\nK:D\n" + row[0]
    # Force use of internal parser by temporarily disabling music21 path or just checking if elements are correct
    tune = Tune(abc)
    
    print(f"Tune ID {tune_id} - Pitches: {tune.pitches}")
    # The first DAFA should be pitches 62, 69, 65, 69
    # If it starts with 62, 62 it's still broken or using music21
    
    # Check elements
    notes = [e['value'] for e in tune.elements if e['type'] == 'note']
    print(f"Notes from elements: {notes[:10]}")

if __name__ == "__main__":
    test_parser_fix()
