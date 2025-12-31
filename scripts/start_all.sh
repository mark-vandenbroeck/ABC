#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p run logs

# Start Flask app (if not running)
if [ -f run/app.pid ]; then
  if kill -0 "$(cat run/app.pid)" 2>/dev/null; then
    echo "Flask app already running (pid $(cat run/app.pid))"
  else
    echo "Stale PID file for Flask app, removing"
    rm -f run/app.pid
  fi
fi
if [ ! -f run/app.pid ]; then
  echo "Starting Flask app..."
  nohup python app.py &> logs/app.log &
  echo $! > run/app.pid
  echo "Flask app PID: $(cat run/app.pid)"
fi

# Start dispatcher (if not running)
if [ -f run/dispatcher.pid ]; then
  if kill -0 "$(cat run/dispatcher.pid)" 2>/dev/null; then
    echo "Dispatcher already running (pid $(cat run/dispatcher.pid))"
  else
    echo "Stale PID file for dispatcher, removing"
    rm -f run/dispatcher.pid
  fi
fi
if [ ! -f run/dispatcher.pid ]; then
  echo "Starting dispatcher..."
  python -u url_dispatcher.py >> logs/dispatcher.log 2>> logs/dispatcher_error.log &
  echo $! > run/dispatcher.pid
  echo "Dispatcher PID: $(cat run/dispatcher.pid)"
fi

echo "All processes started. Check 'logs/' for output and 'run/' for pid files."