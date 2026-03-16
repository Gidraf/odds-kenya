#!/bin/bash
# start_dev.sh — starts all services in the correct order
# Usage: chmod +x start_dev.sh && ./start_dev.sh

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  OddsTerminal Dev Startup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Kill existing workers cleanly
pkill -f "celery" 2>/dev/null && echo "Stopped existing Celery workers" || true
sleep 1

# Activate venv if it exists
[ -f venv/bin/activate ] && source venv/bin/activate

echo ""
echo "Starting services... Open 3 new terminal tabs and run:"
echo ""
echo "  ┌─ TAB 1: Flask server"
echo "  │   python run.py"
echo ""
echo "  ┌─ TAB 2: Playwright worker (--pool=solo prevents SIGSEGV)"
echo "  │   celery -A run.celery_app worker --loglevel=info --pool=solo --queues=playwright --hostname=playwright@%%h"
echo ""
echo "  ┌─ TAB 3: Harvest worker (prefork is safe — no Playwright here)"
echo "  │   celery -A run.celery_app worker --loglevel=info --pool=prefork --queues=harvest --hostname=harvest@%%h --concurrency=4"
echo ""
echo "  ┌─ TAB 4: Beat scheduler (triggers harvest every 60s)"
echo "  │   celery -A run.celery_app beat --loglevel=info"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "WHY --pool=solo for playwright:"
echo "  Chromium cannot survive a Unix fork(). Celery's default"
echo "  prefork pool forks the parent process to create workers."
echo "  The child gets a corrupted Chromium state → SIGSEGV."
echo "  --pool=solo runs tasks in the same process (no fork)."
echo ""
echo "If you accidentally run playwright tasks on the harvest worker:"
echo "  SIGSEGV will still occur. Keep the queues separate."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"