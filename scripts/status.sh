#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "--- Listening sockets ---"
if command -v lsof >/dev/null 2>&1; then
  lsof -iTCP:8888 -sTCP:LISTEN -n -P || echo "port 8888: free"
  lsof -iTCP:5500 -sTCP:LISTEN -n -P || echo "port 5500: free"
  lsof -iTCP:5501 -sTCP:LISTEN -n -P || echo "port 5501: free"
else
  echo "lsof not available; showing processes matching components"
fi

echo
echo "--- Processes (app/dispatcher/workers) ---"
ps aux | egrep 'url_dispatcher.py|app.py|abc_app.py|url_fetcher.py|url_parser.py|abc_indexer.py|url_purger.py' | egrep -v 'grep' || echo "No matching processes"

echo
[ -f run/app.pid ] && echo "app.pid (Management): $(cat run/app.pid)" || true
[ -f run/abc_app.pid ] && echo "abc_app.pid (Search): $(cat run/abc_app.pid)" || true
[ -f run/dispatcher.pid ] && echo "dispatcher.pid: $(cat run/dispatcher.pid)" || true
[ -d logs ] && echo "See logs/ for latest output" || true