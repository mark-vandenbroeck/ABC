#!/usr/bin/env python3
"""Backfill script to set disabled_reason for hosts missing it.

Usage:
  ./scripts/backfill_disabled_reasons.py abc.sourceforge.net  # set single host
  ./scripts/backfill_disabled_reasons.py --all                # set all missing reasons to 'unknown'
"""
import sqlite3
import sys
from pathlib import Path
# Ensure project root is on sys.path so imports from project work when running from scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database import DB_PATH

def set_reason_for_host(host, reason='unknown'):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT disabled, disabled_reason, disabled_at, last_access FROM hosts WHERE host = ?", (host,))
    row = cur.fetchone()
    if not row:
        print(f"Host {host} not found in hosts table")
        conn.close()
        return
    disabled, existing_reason, disabled_at, last_access = row
    if not disabled:
        print(f"Host {host} is not disabled; skipping")
        conn.close()
        return
    if existing_reason is not None:
        print(f"Host {host} already has reason: {existing_reason}; skipping")
        conn.close()
        return
    # Use last_access as disabled_at if available, else CURRENT_TIMESTAMP
    if disabled_at is None and last_access is not None:
        cur.execute("UPDATE hosts SET disabled_reason = ?, disabled_at = ? WHERE host = ?", (reason, last_access, host))
    else:
        cur.execute("UPDATE hosts SET disabled_reason = ?, disabled_at = CURRENT_TIMESTAMP WHERE host = ?", (reason, host))
    conn.commit()
    print(f"Set reason='{reason}' for host {host}")
    conn.close()

def set_reason_for_all(reason='unknown'):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT host FROM hosts WHERE disabled = 1 AND disabled_reason IS NULL")
    rows = cur.fetchall()
    for (host,) in rows:
        print(f"Backfilling {host} -> {reason}")
        cur.execute("UPDATE hosts SET disabled_reason = ?, disabled_at = COALESCE(disabled_at, last_access, CURRENT_TIMESTAMP) WHERE host = ?", (reason, host))
    conn.commit()
    print(f"Backfilled {len(rows)} hosts")
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: backfill_disabled_reasons.py HOST | --all")
        sys.exit(1)
    if sys.argv[1] == '--all':
        set_reason_for_all()
    else:
        set_reason_for_host(sys.argv[1])
