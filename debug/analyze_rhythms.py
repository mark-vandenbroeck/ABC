import sqlite3
import re
from collections import defaultdict
import os

DB_PATH = 'crawler.db'

def normalize(s):
    """Normalize string: lowercase, remove non-alphanumeric chars."""
    if not s: return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def analyze_rhythms():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT rhythm FROM tunes WHERE rhythm IS NOT NULL AND rhythm != ''")
    raw_rhythms = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"Total unique raw rhythms: {len(raw_rhythms)}")

    # Group by normalized form
    groups = defaultdict(list)
    for r in raw_rhythms:
        norm = normalize(r)
        if norm:
            groups[norm].append(r)

    # Find groups with variations
    variations = {k: v for k, v in groups.items() if len(v) > 1}
    
    print(f"\nFound {len(variations)} rhythm types with multiple spelling variations.")
    print("-" * 60)
    
    # Select specific interesting examples or show top groups
    print("\nPROPOSED MAPPING (Display Name -> Variaties in DB):")
    print("=" * 60)
    
    # Heuristic for display name: 
    # 1. Capitalize first letter of words
    # 2. Prefer shorter? Or just take the most common one if we had counts (here we just pick one)
    # Let's pick the one that looks most like Title Case
    
    # Sort by number of variations
    sorted_groups = sorted(variations.items(), key=lambda x: len(x[1]), reverse=True)

    for norm, variants in sorted_groups:
        # Pick the 'prettiest' variant for display
        # robust selection: prefer ones with spaces, Title Case
        display_name = sorted(variants, key=lambda x: (len(x), x))[0] # simplest for now: shortest
        
        # improved selection: try to find Title Case one
        title_case_variants = [v for v in variants if v[0].isupper()]
        if title_case_variants:
            display_name = title_case_variants[0]
            
        # Specific override for common messy ones if needed, but let's see automatic results first
        
        if len(variants) > 1:
            print(f"Display: '{display_name.strip()}'\nIncludes: {sorted(variants)}")
            print("-" * 30)

if __name__ == "__main__":
    analyze_rhythms()
