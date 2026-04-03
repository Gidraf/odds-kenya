#!/usr/bin/env bash
# =============================================================================
# scripts/dev_start.sh
# ---------------------
# Starts all services needed for development in separate terminal tabs/panes.
#
# Requirements:
#   pip install -r requirements.txt
#   Redis running locally  (brew install redis  OR  docker run -p 6379:6379 redis)
#   PostgreSQL running     (brew install postgresql OR docker compose up db)
#
# Usage:
#   chmod +x scripts/dev_start.sh
#   ./scripts/dev_start.sh
#
# Or run each block manually in separate terminals.
# =============================================================================

set -e

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN="\033[92m"; YELLOW="\033[93m"; CYAN="\033[96m"; RESET="\033[0m"
info()   { echo -e "${CYAN}→${RESET} $1"; }
ok()     { echo -e "${GREEN}✔${RESET} $1"; }
warn()   { echo -e "${YELLOW}⚠${RESET} $1"; }

# ── Config — edit these to match your environment ────────────────────────────
FLASK_APP="${FLASK_APP:-app}"
FLASK_ENV="${FLASK_ENV:-development}"
FLASK_PORT="${FLASK_PORT:-5000}"
REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
CONCURRENCY_HARVEST="${CONCURRENCY_HARVEST:-4}"
CONCURRENCY_LIVE="${CONCURRENCY_LIVE:-8}"
CONCURRENCY_RESULTS="${CONCURRENCY_RESULTS:-2}"
CONCURRENCY_NOTIFY="${CONCURRENCY_NOTIFY:-4}"
CONCURRENCY_EVARBDEFAULT="${CONCURRENCY_EVARBDEFAULT:-4}"
LOG_LEVEL="info"

# ── Dependency checks ─────────────────────────────────────────────────────────
info "Checking dependencies …"

command -v redis-cli >/dev/null 2>&1 || { warn "redis-cli not found — install Redis first"; exit 1; }
redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1 && ok "Redis reachable" || { warn "Redis not responding at $REDIS_URL"; exit 1; }

command -v celery >/dev/null 2>&1 || { warn "celery not found — run: pip install celery"; exit 1; }
command -v flask  >/dev/null 2>&1 || { warn "flask not found  — run: pip install flask";  exit 1; }

ok "All dependencies found"
echo ""

# ── Detect terminal multiplexer ───────────────────────────────────────────────
# Tries tmux → gnome-terminal → macOS Terminal (osascript)
# Falls back to printing manual commands if none found.

CELERY_APP="app.workers.celery_tasks"

CMD_FLASK="flask run --host=0.0.0.0 --port=${FLASK_PORT} --debug --reload"

CMD_WORKER_HARVEST="celery -A ${CELERY_APP} worker \
  --queues=harvest \
  --concurrency=${CONCURRENCY_HARVEST} \
  --loglevel=${LOG_LEVEL} \
  --hostname=harvest@%h \
  --max-tasks-per-child=1000"

CMD_WORKER_LIVE="celery -A ${CELERY_APP} worker \
  --queues=live \
  --concurrency=${CONCURRENCY_LIVE} \
  --loglevel=${LOG_LEVEL} \
  --hostname=live@%h \
  --max-tasks-per-child=500"

CMD_WORKER_RESULTS="celery -A ${CELERY_APP} worker \
  --queues=results \
  --concurrency=${CONCURRENCY_RESULTS} \
  --loglevel=${LOG_LEVEL} \
  --hostname=results@%h"

CMD_WORKER_NOTIFY="celery -A ${CELERY_APP} worker \
  --queues=notify \
  --concurrency=${CONCURRENCY_NOTIFY} \
  --loglevel=${LOG_LEVEL} \
  --hostname=notify@%h"

CMD_WORKER_DEFAULT="celery -A ${CELERY_APP} worker \
  --queues=ev_arb,default \
  --concurrency=${CONCURRENCY_EVARBDEFAULT} \
  --loglevel=${LOG_LEVEL} \
  --hostname=default@%h"

CMD_BEAT="celery -A ${CELERY_APP} beat \
  --loglevel=${LOG_LEVEL} \
  --scheduler celery.beat.PersistentScheduler"

CMD_FLOWER="celery -A ${CELERY_APP} flower \
  --port=5555 \
  --loglevel=${LOG_LEVEL}"

# ─────────────────────────────────────────────────────────────────────────────
# TMUX  (recommended — creates a named session with one window per service)
# ─────────────────────────────────────────────────────────────────────────────
if command -v tmux >/dev/null 2>&1; then
  SESSION="oddspedia"
  info "Starting all services in tmux session '${SESSION}' …"

  tmux kill-session -t "$SESSION" 2>/dev/null || true
  tmux new-session -d -s "$SESSION" -n "flask"

  tmux send-keys -t "$SESSION:flask" "$CMD_FLASK" Enter

  tmux new-window   -t "$SESSION" -n "harvest"
  tmux send-keys -t "$SESSION:harvest" "$CMD_WORKER_HARVEST" Enter

  tmux new-window   -t "$SESSION" -n "live"
  tmux send-keys -t "$SESSION:live"    "$CMD_WORKER_LIVE"    Enter

  tmux new-window   -t "$SESSION" -n "results"
  tmux send-keys -t "$SESSION:results" "$CMD_WORKER_RESULTS" Enter

  tmux new-window   -t "$SESSION" -n "notify"
  tmux send-keys -t "$SESSION:notify"  "$CMD_WORKER_NOTIFY"  Enter

  tmux new-window   -t "$SESSION" -n "default"
  tmux send-keys -t "$SESSION:default" "$CMD_WORKER_DEFAULT" Enter

  tmux new-window   -t "$SESSION" -n "beat"
  tmux send-keys -t "$SESSION:beat"    "$CMD_BEAT"           Enter

  tmux new-window   -t "$SESSION" -n "flower"
  tmux send-keys -t "$SESSION:flower"  "$CMD_FLOWER"         Enter

  echo ""
  ok "All services launched in tmux session '${SESSION}'"
  echo ""
  echo "  Attach with:   tmux attach -t ${SESSION}"
  echo "  Switch panes:  Ctrl+B then window number"
  echo "  Kill session:  tmux kill-session -t ${SESSION}"
  echo ""
  echo "  Flask  → http://localhost:${FLASK_PORT}"
  echo "  Flower → http://localhost:5555"
  echo ""
  tmux attach -t "$SESSION"
  exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK — print manual commands
# ─────────────────────────────────────────────────────────────────────────────
warn "tmux not found — run each command below in a separate terminal:"
echo ""
echo "# ── Terminal 1 — Flask API ──────────────────────────────────"
echo "export FLASK_APP=${FLASK_APP} FLASK_ENV=${FLASK_ENV}"
echo "${CMD_FLASK}"
echo ""
echo "# ── Terminal 2 — Harvest worker ────────────────────────────"
echo "${CMD_WORKER_HARVEST}"
echo ""
echo "# ── Terminal 3 — Live worker ───────────────────────────────"
echo "${CMD_WORKER_LIVE}"
echo ""
echo "# ── Terminal 4 — Results worker ────────────────────────────"
echo "${CMD_WORKER_RESULTS}"
echo ""
echo "# ── Terminal 5 — Notify worker ─────────────────────────────"
echo "${CMD_WORKER_NOTIFY}"
echo ""
echo "# ── Terminal 6 — EV/Arb + Default worker ───────────────────"
echo "${CMD_WORKER_DEFAULT}"
echo ""
echo "# ── Terminal 7 — Beat scheduler ────────────────────────────"
echo "${CMD_BEAT}"
echo ""
echo "# ── Terminal 8 — Flower dashboard (optional) ───────────────"
echo "${CMD_FLOWER}"
echo ""