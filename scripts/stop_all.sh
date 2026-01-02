#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Stop processes using pid files if present
for f in run/*.pid; do
  [ -e "$f" ] || continue
  pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then
    echo "Killing pid $pid (from $f)"
    kill "$pid" || true
  else
    echo "No process with pid $pid (from $f)"
  fi
  rm -f "$f"
done

# Fallback: kill by process name (SIGTERM)
pkill -f url_dispatcher.py 2>/dev/null || true
pkill -f "python app.py" 2>/dev/null || true
pkill -f "python abc_app.py" 2>/dev/null || true
pkill -f url_fetcher.py 2>/dev/null || true
pkill -f url_parser.py 2>/dev/null || true
pkill -f abc_indexer.py 2>/dev/null || true
pkill -f url_purger.py 2>/dev/null || true

# Final sweep with SIGKILL for anything that didn't stop
sleep 1
pkill -9 -f "url_dispatcher.py|app.py|abc_app.py|url_fetcher.py|url_parser.py|abc_indexer.py|url_purger.py" 2>/dev/null || true

echo "Stop attempted. Verify with 'make status' or 'scripts/status.sh'."