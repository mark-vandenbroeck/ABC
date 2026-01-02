#!/usr/bin/env python3
"""Reset `urls` and `hosts` tables and insert two seed URLs.

Usage: python scripts/reset_db.py
"""
import os
import sys
import sqlite3
# Ensure project root is on sys.path so we can import local modules when running the script
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from database import get_db_connection


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    print('Deleting rows from urls and hosts...')
    cur.execute('DELETE FROM urls')
    cur.execute('DELETE FROM hosts')
    print('Deleting rows from tunes, tunebooks and faiss_mapping...')
    cur.execute('DELETE FROM tunes')
    cur.execute('DELETE FROM tunebooks')
    cur.execute('DELETE FROM faiss_mapping')
    conn.commit()

    # Remove FAISS index file
    if os.path.exists("data/tunes.index"):
        print('Removing FAISS index file...')
        os.remove("data/tunes.index")

    print('Inserting seed URLs...')
    from urllib.parse import urlparse
    seed_urls = [
        'http://www.campin.me.uk/', 
        'https://www.norbeck.nu/abc/',
        'http://www.tradfrance.com/',
        'https://abc.sourceforge.net/NMD/',
        'https://www.ceolas.org/tunes/',
        'https://www.cranfordpub.com/',
        'http://simonwascher.info/'
    ]
    for u in seed_urls:
        h = urlparse(u).hostname
        cur.execute("INSERT INTO urls (url, host, link_distance, created_at) VALUES (?, ?, 0, datetime('now'))", (u, h))
    conn.commit()

    cur.execute('SELECT COUNT(*) FROM urls')
    urls_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM hosts')
    hosts_count = cur.fetchone()[0]

    print(f'Rows in urls: {urls_count}')
    print(f'Rows in hosts: {hosts_count}')

    cur.execute('SELECT id, url, created_at FROM urls')
    rows = cur.fetchall()
    print('\nInserted URLs:')
    for r in rows:
        print(r)

    conn.close()


if __name__ == '__main__':
    main()