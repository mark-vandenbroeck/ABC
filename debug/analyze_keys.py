import sqlite3
from collections import defaultdict
import os

DB_PATH = 'crawler.db'

def analyze_keys():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT key FROM tunes WHERE key IS NOT NULL AND key != ''")
    raw_keys = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"Total unique raw keys: {len(raw_keys)}")
    
    # Just list the top 50 to see patterns
    print("\nSample of raw keys (first 50 sorted):")
    print(sorted(raw_keys)[:50])

    # Check specifically for case collisions
    # e.g. "Am" vs "AM" vs "am"
    groups = defaultdict(list)
    for k in raw_keys:
        # Simple Key normalization attempt: Root (Upper) + Mode (Lower)
        # But wait, user said "AM" is Major, "Am" is Minor. 
        # Standard ABC notation says: 'm' is minor. Major is default (no suffix) or 'maj'.
        # Let's just see what we have.
        groups[k.lower()].append(k)
        
    collisions = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"\nFound {len(collisions)} keys with case-only variations.")
    
    if collisions:
        print("\nExamples of case collisions:")
        for k, v in list(collisions.items())[:10]:
            print(f"  '{k}': {v}")

if __name__ == "__main__":
    analyze_keys()
